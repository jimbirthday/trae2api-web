// Package auth 解析 TRAE SOLO auth 文件（嵌套形：auth + account），
// 提供原子写回与目录扫描。
package auth

import (
	"encoding/json"
	"fmt"
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
	if a.ExpiresAt <= 0 {
		return true
	}
	return time.Now().Add(within).Unix() >= a.ExpiresAt
}

// parseNested 兼容现有 trae-*.json 嵌套形：
//
//	{"account":{...},"auth":{...}}
func parseNested(raw []byte) (*Auth, error) {
	var n struct {
		Auth struct {
			AccessToken  string   `json:"accessToken"`
			RefreshToken string   `json:"refreshToken"`
			ExpiresAt    int64    `json:"expiresAt"`
			Domain       string   `json:"domain"`
			ApiHost      string   `json:"apiHost"`
			MachineID    string   `json:"machineId"`
			DeviceID     string   `json:"deviceId"`
			Cookies      []Cookie `json:"cookies"`
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
		ExpiresAt:    n.Auth.ExpiresAt,
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
		AccessToken  string   `json:"accessToken"`
		RefreshToken string   `json:"refreshToken"`
		ExpiresAt    int64    `json:"expiresAt"`
		Domain       string   `json:"domain"`
		ApiHost      string   `json:"apiHost"`
		MachineID    string   `json:"machineId"`
		DeviceID     string   `json:"deviceId"`
		Cookies      []Cookie `json:"cookies"`
		UID          string   `json:"uid"`
		EnterpriseID string   `json:"enterpriseId"`
		Nickname     string   `json:"nickname"`
		Weight       int      `json:"weight"`
		Note         string   `json:"note"`
	}
	if err := json.Unmarshal(raw, &f); err != nil {
		return nil, fmt.Errorf("storage_parse_error: %w", err)
	}
	return &Auth{
		AccessToken:  f.AccessToken,
		RefreshToken: f.RefreshToken,
		ExpiresAt:    f.ExpiresAt,
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
	if strings.TrimSpace(a.AccessToken) == "" {
		return nil, fmt.Errorf("parse_error: missing accessToken")
	}
	return a, nil
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
	tmp := a.FilePath + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, a.FilePath)
}

// FilePathFor 在已知 AuthDir 时构造 trae-{uid}.json 落盘路径。
func FilePathFor(authDir, uid string) string {
	return filepath.Join(authDir, "trae-"+uid+".json")
}

// MaskToken 保留前 n 字符 + 省略号；不足则全显示。用于面板 JSON 预览脱敏。
func MaskToken(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…(" + strconv.Itoa(len(s)) + " chars)"
}

// LoadDir 扫描 dir 下 trae-*.json。解析失败的文件静默跳过（启动日志由调用方统计）。
func LoadDir(dir string) ([]*Auth, error) {
	files, err := filepath.Glob(filepath.Join(dir, "trae-*.json"))
	if err != nil {
		return nil, err
	}
	var out []*Auth
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			continue
		}
		a, err := Parse(raw)
		if err != nil {
			continue
		}
		a.FilePath = f
		out = append(out, a)
	}
	return out, nil
}
