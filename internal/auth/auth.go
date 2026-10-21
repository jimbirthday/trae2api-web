// Package auth 解析 TRAE SOLO auth 文件（嵌套形：auth + account），
// 提供原子写回与目录扫描。
package auth

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Auth 归一化后的账号凭证。
//
// 并发模型：mu 保护可变字段（AccessToken/RefreshToken/ExpiresAt），
// 写路径（upstream.RefreshToken）持写锁整段执行 ExchangeToken；
// 读路径（NeedsRefresh/JWT/RefreshTokenValue）持读锁读取快照，
// 杜绝与写并发时的数据竞争（-race 实测确认）。
// 其余字段（Domain/ApiHost/MachineID/DeviceID/UID/...）加载后不变，直接读。
type Auth struct {
	mu sync.RWMutex

	AccessToken  string // Cloud-IDE-JWT 头用
	RefreshToken string // 每次 ExchangeToken 轮换
	ExpiresAt    int64  // Unix 秒（accessToken 过期时刻）
	Domain       string // "trae.cn"
	ApiHost      string // "https://api.trae.com.cn"（ExchangeToken host）
	MachineID    string // x-machine-id
	DeviceID     string // x-device-id
	UID          string
	EnterpriseID string
	Nickname     string
	Cookies      []Cookie // 第三方导出格式自带的浏览器 cookie（仅存档，不参与出站请求头）
	FilePath     string   // 落盘路径；refresh 后原子写回

	// ImportDisabled 仅供导入通道使用（JSON 里 enabled=false），不落盘、不参与调度判定。
	ImportDisabled bool `json:"-"`

	// Weight 手动调度权重（1-100，1 = 不干预）。随凭证档持久化在 account.weight。
	Weight int `json:"-"`
	// Note 账号备注（面板自填，纯展示不参与选号），持久化在 account.note。
	Note string `json:"-"`
}

// Cookie 浏览器登录态 cookie（第三方导出格式带出来的原始项）。
// 当前上游链路只依赖 Cloud-IDE-JWT / X-Uid 等请求头，cookie 仅做存档：
// 既能完整回写凭证文件（便于导出/迁移），也为将来需要 X-Cloudide-Session 的端点留好数据。
type Cookie struct {
	Name     string `json:"name"`
	Value    string `json:"value"`
	HostKey  string `json:"host_key,omitempty"`
	Path     string `json:"path,omitempty"`
	Expires  int64  `json:"expires_utc,omitempty"`
	Secure   bool   `json:"is_secure,omitempty"`
	HTTPOnly bool   `json:"is_httponly,omitempty"`
}

// CookieValue 按名字查 cookie 值（找不到返回空串）。
func (a *Auth) CookieValue(name string) string {
	a.mu.RLock()
	defer a.mu.RUnlock()
	for _, c := range a.Cookies {
		if c.Name == name {
			return c.Value
		}
	}
	return ""
}

// CookieCount 返回存档 cookie 数量（面板展示用）。
func (a *Auth) CookieCount() int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return len(a.Cookies)
}

// Lock 供同进程内其他包（upstream.RefreshToken）在改写 Auth 字段期间加写锁。
func (a *Auth) Lock() { a.mu.Lock() }

// Unlock 释放 a.Lock 获取的写锁。
func (a *Auth) Unlock() { a.mu.Unlock() }

// RLock 供读路径持有读锁（与写锁互斥，读读不互斥）。
func (a *Auth) RLock() { a.mu.RLock() }

// RUnlock 释放 a.RLock 获取的读锁。
func (a *Auth) RUnlock() { a.mu.RUnlock() }

// AccessTokenValue 加锁读取 accessToken（出站请求头一律经此取值，勿直读字段）。
func (a *Auth) AccessTokenValue() string {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.AccessToken
}

// Realm 返回账号域。TRAE 当前只有 CN 单域（不做多租户/双域），恒为 "cn"。
// 保留此方法是为了让调度层（pool/session）的接口与未来国际版扩展一致。
func (a *Auth) Realm() string { return "cn" }

// IsEnterprise 报告账号是否为企业版。TRAE 暂无企业号体系，恒为 false；
// 保留以便调度层按统一口径跳过企业专属任务。
func (a *Auth) IsEnterprise() bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return strings.TrimSpace(a.EnterpriseID) != ""
}

// WeightValue 返回归一化后的手动调度权重：1 = 不干预，2-100 = 更高优先级。
func (a *Auth) WeightValue() int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return NormalizeWeight(a.Weight)
}

// SetWeight 设置手动调度权重，越界值收敛而非报错。
func (a *Auth) SetWeight(weight int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.Weight = NormalizeWeight(weight)
}

// SetNote 设置账号备注（纯展示，截断到 200 rune）。
func (a *Auth) SetNote(note string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.Note = NormalizeNote(note)
}

// ProfileSnapshot 返回面板展示需要的账号资料快照（weight/note/nickname/realm）。
func (a *Auth) ProfileSnapshot() (weight int, note, nickname, realm string) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return NormalizeWeight(a.Weight), a.Note, a.Nickname, "cn"
}

// SetProfile 原子更新面板可编辑的账号资料（nil 表示该字段不变）并落盘。
// 只允许改 weight/note：与 token 刷新共用 a.mu，避免 refresh 写回时用旧值覆盖。
func (a *Auth) SetProfile(weight *int, note *string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if weight != nil {
		a.Weight = NormalizeWeight(*weight)
	}
	if note != nil {
		a.Note = NormalizeNote(*note)
	}
	return a.saveAtomicLocked()
}

// SetNickname 在持锁状态下更新昵称并落盘；允许空串（用于清空昵称）。
// 落盘错误会原样返回，绝不吞掉（否则内存与磁盘会静默分叉）。
func (a *Auth) SetNickname(nick string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.Nickname = strings.TrimSpace(nick)
	return a.saveAtomicLocked()
}

// NormalizeWeight 把任意权重收敛到 [1,100]；0/负数/异常值回落 1（不干预）。
func NormalizeWeight(w int) int {
	if w < 1 {
		return 1
	}
	if w > 100 {
		return 100
	}
	return w
}

// NormalizeNote 去首尾空白并截断到 200 rune（按 rune 切，避免切坏中文）。
func NormalizeNote(note string) string {
	note = strings.TrimSpace(note)
	if len(note) <= 200 {
		return note
	}
	r := []rune(note)
	if len(r) > 200 {
		r = r[:200]
	}
	return string(r)
}

// JWT 返回当前 accessToken 的读锁快照，防与 RefreshToken 写并发竞态。
func (a *Auth) JWT() string {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.AccessToken
}

// RefreshTokenValue 返回当前 refreshToken 的读锁快照，防与 RefreshToken 写并发竞态。
func (a *Auth) RefreshTokenValue() string {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.RefreshToken
}

// NeedsRefresh 报告 token 是否将在 within 内过期（或已过期/无 expiry）。
func (a *Auth) NeedsRefresh(within time.Duration) bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.NeedsRefreshLocked(within)
}

// NeedsRefreshLocked 是 NeedsRefresh 的持锁内部版本；调用方必须已持有 a.mu（读或写锁）。
func (a *Auth) NeedsRefreshLocked(within time.Duration) bool {
	if a.ExpiresAt > 0 {
		return time.Now().Add(within).Unix() >= a.ExpiresAt
	}
	// 过期时刻未知：只有拿到 refreshToken 才值得硬刷新；access-token-only 账号
	// 换不出新 token，硬刷新会被上游以 no refreshToken 拒绝并把号打成冷却。
	return strings.TrimSpace(a.RefreshToken) != ""
}

// parseNested 兼容现有 trae-*.json 嵌套形：
//
//	{"account":{...},"auth":{...}}
func parseNested(raw []byte) (*Auth, error) {
	var n struct {
		Auth struct {
			AccessToken  string          `json:"accessToken"`
			RefreshToken string          `json:"refreshToken"`
			ExpiresAt    json.RawMessage `json:"expiresAt"`
			Domain       string          `json:"domain"`
			ApiHost      string          `json:"apiHost"`
			MachineID    string          `json:"machineId"`
			DeviceID     string          `json:"deviceId"`
			Cookies      []Cookie        `json:"cookies"`
		} `json:"auth"`
		Account struct {
			UID          string `json:"uid"`
			EnterpriseID string `json:"enterpriseId"`
			Nickname     string `json:"nickname"`
			Weight       int    `json:"weight"`
			Note         string `json:"note"`
		} `json:"account"`
	}
	if err := json.Unmarshal(raw, &n); err != nil {
		return nil, fmt.Errorf("storage_parse_error: %w", err)
	}
	return &Auth{
		AccessToken:  n.Auth.AccessToken,
		RefreshToken: n.Auth.RefreshToken,
		ExpiresAt:    parseExpiresAtRaw(n.Auth.ExpiresAt),
		Domain:       n.Auth.Domain,
		ApiHost:      n.Auth.ApiHost,
		MachineID:    n.Auth.MachineID,
		DeviceID:     n.Auth.DeviceID,
		Cookies:      n.Auth.Cookies,
		UID:          n.Account.UID,
		EnterpriseID: n.Account.EnterpriseID,
		Nickname:     n.Account.Nickname,
		Weight:       NormalizeWeight(n.Account.Weight),
		Note:         NormalizeNote(n.Account.Note),
	}, nil
}

// parseFlat 兼容扁平形（CPA 面板手建等）：
//
//	{"accessToken":...,"uid":...,"machineId":...,"deviceId":...}
func parseFlat(raw []byte) (*Auth, error) {
	var f struct {
		AccessToken  string          `json:"accessToken"`
		RefreshToken string          `json:"refreshToken"`
		ExpiresAt    json.RawMessage `json:"expiresAt"`
		Domain       string          `json:"domain"`
		ApiHost      string          `json:"apiHost"`
		MachineID    string          `json:"machineId"`
		DeviceID     string          `json:"deviceId"`
		Cookies      []Cookie        `json:"cookies"`
		UID          string          `json:"uid"`
		EnterpriseID string          `json:"enterpriseId"`
		Nickname     string          `json:"nickname"`
		Weight       int             `json:"weight"`
		Note         string          `json:"note"`
	}
	if err := json.Unmarshal(raw, &f); err != nil {
		return nil, fmt.Errorf("storage_parse_error: %w", err)
	}
	return &Auth{
		AccessToken:  f.AccessToken,
		RefreshToken: f.RefreshToken,
		ExpiresAt:    parseExpiresAtRaw(f.ExpiresAt),
		Domain:       f.Domain,
		ApiHost:      f.ApiHost,
		MachineID:    f.MachineID,
		DeviceID:     f.DeviceID,
		Cookies:      f.Cookies,
		UID:          f.UID,
		EnterpriseID: f.EnterpriseID,
		Nickname:     f.Nickname,
		Weight:       NormalizeWeight(f.Weight),
		Note:         NormalizeNote(f.Note),
	}, nil
}

// Parse 兼容两种磁盘形态：
//
//	嵌套形 {"auth":{...},"account":{...}}  （登录脚本产出，现有 trae-*.json）
//	扁平形 {"accessToken":...,"uid":...}   （手建）
func Parse(raw []byte) (*Auth, error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("empty auth storage")
	}
	var probe map[string]json.RawMessage
	if err := json.Unmarshal(raw, &probe); err != nil {
		return nil, fmt.Errorf("storage_parse_error: %w", err)
	}
	var (
		a   *Auth
		err error
	)
	if _, nested := probe["auth"]; nested {
		a, err = parseNested(raw)
	} else {
		a, err = parseFlat(raw)
	}
	if err != nil {
		return nil, err
	}
	if err := a.normalizeIdentity(); err != nil {
		return nil, err
	}
	// refresh-only 账号（只有 refreshToken、accessToken 为空）允许加载：
	// 由 RefreshTokenIfNeeded 在首次使用时换出 accessToken。
	if strings.TrimSpace(a.AccessToken) == "" && strings.TrimSpace(a.RefreshToken) == "" {
		return nil, fmt.Errorf("parse_error: missing accessToken and refreshToken")
	}
	return a, nil
}

// normalizeIdentity 归一化账号标识字段：
//   - 去掉 token / uid 两端空白；
//   - uid 缺失时从 accessToken（JWT）的 data.id 推导；
//   - uid 非空时必须通过 ValidateUID（防路径穿越）。
//
// uid 完全推不出来时保持空串（历史文件兼容），是否可用由调度层判定。
func (a *Auth) normalizeIdentity() error {
	a.AccessToken = strings.TrimSpace(a.AccessToken)
	a.RefreshToken = strings.TrimSpace(a.RefreshToken)
	a.UID = strings.TrimSpace(a.UID)
	if a.UID == "" && a.AccessToken != "" {
		if info := ParseJWT(a.AccessToken); info.UID != "" {
			a.UID = strings.TrimSpace(info.UID)
		}
	}
	if a.UID != "" {
		if err := ValidateUID(a.UID); err != nil {
			return fmt.Errorf("parse_error: %w", err)
		}
	}
	return nil
}

// parseExpiresAtRaw 兼容三种 expiresAt 形态：
//
//	1793756328（秒，数字）、"1793756328612"（毫秒，字符串）、1793756328612（毫秒，数字）。
//
// 统一归一化为 Unix 秒；无法解析/非正数返回 0（视为未知有效期）。
func parseExpiresAtRaw(raw json.RawMessage) int64 {
	s := strings.TrimSpace(string(raw))
	if s == "" || s == "null" {
		return 0
	}
	s = strings.Trim(strings.TrimSpace(s), `"`)
	if s == "" {
		return 0
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil || v <= 0 {
		// 兼容浮点写法（1793756328.0）
		f, ferr := strconv.ParseFloat(s, 64)
		if ferr != nil || f <= 0 {
			return 0
		}
		v = int64(f)
	}
	if v > 1e11 { // 毫秒
		v /= 1000
	}
	if v <= 0 {
		return 0
	}
	return v
}

// SaveAtomic 以嵌套形原子写回 FilePath（tmp + rename，0600），保持登录脚本可读格式。
// 加锁外壳：防止与 RefreshToken 并发读写 token 字段导致写回半更新。
func (a *Auth) SaveAtomic() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.saveAtomicLocked()
}

// cookiesForSave 返回不入库的空安全 cookie 切片（nil → 空数组，保持文件结构稳定）。
func (a *Auth) cookiesForSave() []Cookie {
	if len(a.Cookies) == 0 {
		return []Cookie{}
	}
	return a.Cookies
}

// saveAtomicLocked 是 SaveAtomic 的持锁内部版本；调用方必须已持有 a.mu。
func (a *Auth) saveAtomicLocked() error {
	if a.FilePath == "" {
		return fmt.Errorf("no FilePath set")
	}
	doc := map[string]any{
		"auth": map[string]any{
			"accessToken":  a.AccessToken,
			"refreshToken": a.RefreshToken,
			"expiresAt":    a.ExpiresAt,
			"domain":       a.Domain,
			"apiHost":      a.ApiHost,
			"machineId":    a.MachineID,
			"deviceId":     a.DeviceID,
			"cookies":      a.cookiesForSave(),
		},
		"account": map[string]any{
			"uid":          a.UID,
			"enterpriseId": a.EnterpriseID,
			"nickname":     a.Nickname,
			"weight":       NormalizeWeight(a.Weight),
			"note":         NormalizeNote(a.Note),
		},
	}
	raw, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return err
	}
	// 先写临时文件 + fsync，再 rename，最后 fsync 父目录：保证 refresh token
	// 轮换后即使进程/机器崩溃也不会丢盘。
	tmp := a.FilePath + ".tmp"
	if err := writeFileSync(tmp, raw); err != nil {
		removeQuiet(tmp)
		return err
	}
	if err := os.Rename(tmp, a.FilePath); err != nil {
		removeQuiet(tmp)
		return err
	}
	return syncDir(filepath.Dir(a.FilePath))
}

// writeFileSync 写入文件并 fsync 到磁盘，权限固定 0600（覆盖已有文件也强制收敛）。
func writeFileSync(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	// 临时文件若已存在（上一次崩溃遗留）也强制收敛到 0600。
	if err := f.Chmod(0o600); err != nil {
		f.Close()
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// syncDir 把 rename 产生的目录元数据刷盘。部分文件系统不支持目录 fsync（EINVAL 等），
// 这类错误按尽力而为忽略；打开失败仍上报。
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return nil
	}
	return nil
}

// removeQuiet 清理临时文件，失败忽略（rename 前失败时不留下脏文件）。
func removeQuiet(path string) { _ = os.Remove(path) }

// MaxUIDLen 是账号 UID 允许的最大长度。
const MaxUIDLen = 64

// ValidateUID 校验账号 UID 是否可安全用于文件名：
// 非空、长度 1..MaxUIDLen、只含 [A-Za-z0-9_-]。
//
// UID 来自导入文件与查询参数，而 UID 会拼进 trae-{uid}.json 文件名；
// 不校验时 uid="../../etc/passwd" 就能越出 authDir（读写删全部可命中）。
func ValidateUID(uid string) error {
	if uid == "" {
		return fmt.Errorf("invalid uid: empty")
	}
	if len(uid) > MaxUIDLen {
		return fmt.Errorf("invalid uid: too long (%d > %d)", len(uid), MaxUIDLen)
	}
	for i := 0; i < len(uid); i++ {
		c := uid[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '_', c == '-':
		default:
			return fmt.Errorf("invalid uid: illegal character %q at %d", c, i)
		}
	}
	return nil
}

// SafeFilePathFor 校验 UID 后构造 trae-{uid}.json 落盘路径；非法 UID 返回 error。
// 调用方必须处理 error（不要退化成 FilePathFor 的空串）。
func SafeFilePathFor(authDir, uid string) (string, error) {
	if err := ValidateUID(uid); err != nil {
		return "", err
	}
	dir := strings.TrimSpace(authDir)
	if dir == "" {
		return "", fmt.Errorf("empty auth dir")
	}
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return "", err
	}
	absFP, err := filepath.Abs(filepath.Join(dir, "trae-"+uid+".json"))
	if err != nil {
		return "", err
	}
	// 双保险：清理后的路径必须直接落在 authDir 下（防平台相关的路径解释差异）。
	if filepath.Dir(absFP) != filepath.Clean(absDir) {
		return "", fmt.Errorf("invalid uid %q: path escapes auth dir", uid)
	}
	return filepath.Join(dir, "trae-"+uid+".json"), nil
}

// FilePathFor 在已知 AuthDir 时构造 trae-{uid}.json 落盘路径。
// UID 非法时返回空串 ""（保留历史签名；新代码请用 SafeFilePathFor 并处理 error）。
func FilePathFor(authDir, uid string) string {
	fp, err := SafeFilePathFor(authDir, uid)
	if err != nil {
		return ""
	}
	return fp
}

// MaskToken 保留前 n 字符 + 省略号；不足则全显示。用于面板 JSON 预览脱敏。
func MaskToken(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…(" + strconv.Itoa(len(s)) + " chars)"
}

// LoadDir 扫描 dir 下 trae-*.json。
//
// 解析失败的文件不再静默跳过：每个失败文件打一条日志（文件名 + 原因），
// 最后汇总一条"加载 N 个 / 跳过 M 个"。成功解析的照常返回，
// 单个坏文件不会让账号凭空消失且无人知晓。
func LoadDir(dir string) ([]*Auth, error) {
	files, err := filepath.Glob(filepath.Join(dir, "trae-*.json"))
	if err != nil {
		return nil, err
	}
	var out []*Auth
	var skipped int
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			skipped++
			log.Printf("auth: skip %s: read failed: %v", f, err)
			continue
		}
		a, err := Parse(raw)
		if err != nil {
			skipped++
			log.Printf("auth: skip %s: parse failed: %v", f, err)
			continue
		}
		a.FilePath = f
		out = append(out, a)
	}
	if skipped > 0 {
		log.Printf("auth: loaded %d account(s) from %s, skipped %d bad file(s)", len(out), dir, skipped)
	}
	return out, nil
}
