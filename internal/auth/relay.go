package auth

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// ParseRelay 兼容两类第三方导出格式，解析后归一化为 Auth。
//
// 格式 B（trae-cn-relay-accounts v1）单条：
//
//	{"user_id":"1","label":"昵称","token":"<jwt>","refresh_token":"...",
//	 "expired_at":"1793756328612","host":"https://api.trae.com.cn","edition":"cn"}
//
// 整包：{"format":"trae-cn-relay-accounts","accounts":[ ...单条... ]}
//
// 格式 A（浏览器登录态导出，带 cookies）单条：
//
//	{"name":"账号461","token":"<jwt>","refreshToken":"...","deviceId":"","machineId":"",
//	 "enabled":true,"cookies":[{"name":"_clck","value":"...","host_key":".trae.cn"}]}
//
// 整包：{"accounts":[ ...单条... ]}（无 format 字段亦可）
//
// 格式 A 没有 user_id / expired_at：uid 从 JWT 的 data.id 解析，有效期取 JWT 的 exp。
// 返回归一化后的 Auth 列表（不含 FilePath，由调用方补）。
func ParseRelay(raw []byte) ([]*Auth, error) {
	var probe struct {
		Accounts []json.RawMessage `json:"accounts"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil {
		return nil, fmt.Errorf("relay_parse_error: %w", err)
	}
	var items []json.RawMessage
	if len(probe.Accounts) > 0 {
		items = probe.Accounts
	} else {
		items = []json.RawMessage{json.RawMessage(raw)}
	}
	out := make([]*Auth, 0, len(items))
	var firstErr error
	for _, it := range items {
		a, err := parseRelayOne(it)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		out = append(out, a)
	}
	if len(out) == 0 {
		if firstErr != nil {
			return nil, firstErr
		}
		return nil, fmt.Errorf("relay_parse_error: no accounts")
	}
	return out, nil
}

// LooksLikeRelay 判断一段 JSON 是否是第三方导出格式（而非本仓库扁平/嵌套形）。
//
// 判据只认「批量或中转格式 B 的独有形状」：
//   - 顶层有 "accounts" 数组（两种批量格式都是这个外壳）
//   - 顶层有 "token"（格式 A 单条 / 格式 B 单条）
//   - 顶层有 "refresh_token"（格式 B 单条，snake_case）
//
// 刻意**不**把 "refreshToken"（驼峰）当判据：本仓库自己导出的扁平形
// {"uid":..,"accessToken":..,"refreshToken":..} 也带这个键，若据此判成批量，
// 单账号导入会被塞进批量通道、走中继解析器而丢掉 uid。
// 格式 A 的驼峰 refreshToken 一定伴随 "token"，上面第二条已经覆盖。
func LooksLikeRelay(raw []byte) bool {
	var probe struct {
		Accounts []json.RawMessage `json:"accounts"`
		Token    *string           `json:"token"`
		Refresh  *string           `json:"refresh_token"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil {
		return false
	}
	return len(probe.Accounts) > 0 || probe.Token != nil || probe.Refresh != nil
}

func parseRelayOne(raw []byte) (*Auth, error) {
	// 同一份 "accounts" 数组里可能混放本仓库嵌套形（导出后再导入的闭环）：
	// {"auth":{...},"account":{...}} 交给 Parse 处理，其余按中转格式解析。
	var keys map[string]json.RawMessage
	if err := json.Unmarshal(raw, &keys); err == nil {
		if _, nested := keys["auth"]; nested {
			a, err := Parse(raw)
			if err != nil {
				return nil, err
			}
			if a.Domain == "" {
				a.Domain = "trae.cn"
			}
			if a.ApiHost == "" {
				a.ApiHost = "https://api.trae.com.cn"
			}
			return a, nil
		}
	}
	var m struct {
		UserID   string `json:"user_id"`
		UID      string `json:"uid"` // 本仓库扁平形的字段名，混在批量数组里时也要认
		ID       string `json:"id"`
		Label    string `json:"label"`
		Nickname string `json:"nickname"`
		Name     string `json:"name"` // 格式 A
		Token    string `json:"token"`
		// access_token（snake）与 accessToken（camel）都认：前者是格式 B，
		// 后者是本仓库扁平形，二者可能出现在同一个 accounts 数组里。
		AccessToken   string          `json:"access_token"`
		AccessTokenCC string          `json:"accessToken"`
		RefreshToken  string          `json:"refresh_token"`
		RefreshCamel  string          `json:"refreshToken"` // 格式 A（驼峰）
		ExpiredAt     json.RawMessage `json:"expired_at"`
		Host          string          `json:"host"`
		Edition       string          `json:"edition"`
		MachineID     string          `json:"machineId"`
		DeviceID      string          `json:"deviceId"`
		Enabled       *bool           `json:"enabled"` // 格式 A；nil = 未提供
		Cookies       []Cookie        `json:"cookies"` // 格式 A
		ExpiresAtJSON json.RawMessage `json:"expiresAt"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("relay_parse_error: %w", err)
	}
	token := strings.TrimSpace(m.Token)
	if token == "" {
		token = strings.TrimSpace(m.AccessToken)
	}
	if token == "" {
		token = strings.TrimSpace(m.AccessTokenCC)
	}
	refresh := strings.TrimSpace(m.RefreshToken)
	if refresh == "" {
		refresh = strings.TrimSpace(m.RefreshCamel)
	}
	// refresh-only 账号（只有 refreshToken）允许导入：首次调用时再换取 accessToken。
	if token == "" && refresh == "" {
		return nil, fmt.Errorf("relay_parse_error: missing token")
	}
	meta := ParseJWT(token)
	uid := strings.TrimSpace(m.UserID)
	if uid == "" {
		uid = strings.TrimSpace(m.UID)
	}
	if uid == "" {
		uid = strings.TrimSpace(m.ID)
	}
	if uid == "" {
		uid = meta.UID // 格式 A 没有 user_id，从 JWT 的 data.id 取
	}
	nick := strings.TrimSpace(m.Label)
	if nick == "" {
		nick = strings.TrimSpace(m.Nickname)
	}
	if nick == "" {
		nick = strings.TrimSpace(m.Name) // 格式 A
	}
	if nick == "" && uid != "" {
		n := uid
		if len(n) > 4 {
			n = n[len(n)-4:]
		}
		nick = "用户" + n
	}
	exp := parseEpochSec(m.ExpiredAt)
	if exp == 0 {
		exp = parseEpochSec(m.ExpiresAtJSON) // 本仓扁平形字段名
	}
	if exp == 0 {
		exp = meta.Exp // 兜底用 JWT 的 exp
	}
	a := &Auth{
		AccessToken:  token,
		RefreshToken: refresh,
		ExpiresAt:    exp,
		UID:          uid,
		Nickname:     nick,
		Domain:       "trae.cn",
		ApiHost:      "https://api.trae.com.cn",
		MachineID:    strings.TrimSpace(m.MachineID),
		DeviceID:     strings.TrimSpace(m.DeviceID),
		Cookies:      m.Cookies,
	}
	if h := normalizeHost(m.Host); h != "" {
		a.ApiHost = h
	}
	// 统一归一化：token 去空白、uid 缺失时从 JWT 推导、uid 必须通过 ValidateUID。
	if err := a.normalizeIdentity(); err != nil {
		return nil, fmt.Errorf("relay_parse_error: %w", err)
	}
	if m.Enabled != nil && !*m.Enabled {
		a.ImportDisabled = true // 由导入层转成 pool 的 disabled 状态
	}
	// 中转格式的 edition 只可能是 cn（global 不走本仓）
	return a, nil
}

// normalizeHost 只接受 https://api.trae.com.cn 这类端点，忽略带路径的脏值。
func normalizeHost(h string) string {
	h = strings.TrimSpace(h)
	if h == "" {
		return ""
	}
	h = strings.TrimRight(h, "/")
	if !strings.HasPrefix(h, "http://") && !strings.HasPrefix(h, "https://") {
		return ""
	}
	return h
}

// parseEpochSec 兼容 "1793756328612"（毫秒字符串）、1793756328612（毫秒数字）、
// 1793756328（秒）、""，统一归一化为 Unix 秒；无法解析返回 0（视为未知有效期）。
func parseEpochSec(raw json.RawMessage) int64 {
	s := strings.TrimSpace(string(raw))
	if s == "" || s == "null" || s == `""` {
		return 0
	}
	s = strings.Trim(s, `"`)
	v, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil || v <= 0 {
		return 0
	}
	if v > 1e11 { // 毫秒
		v /= 1000
	}
	return v
}

// ---------------------------------------------------------------------------
// 多文档 / 数组 / 文件导入
// ---------------------------------------------------------------------------

// ParseAnyAccounts 解析「任意一种导出形态」的文件内容，返回归一化账号列表。
//
// 支持：
//   - 单个 JSON 对象（单条账号，格式 A 或 B）
//   - 整包 {"accounts":[...]}（格式 A / B / 本仓导出包）
//   - 顶层数组 [ {...}, {...} ]
//   - 多个 JSON 文档首尾相接（用户手工拼接的导出文件，流式解码器天然支持）
//
// 单条解析失败不阻断其余，只要有一条成功即返回。
func ParseAnyAccounts(raw []byte) ([]*Auth, error) {
	docs := splitJSONDocs(raw)
	if len(docs) == 0 {
		return nil, fmt.Errorf("relay_parse_error: no JSON document found")
	}
	var out []*Auth
	var firstErr error
	for _, doc := range docs {
		trimmed := bytes.TrimSpace(doc)
		if len(trimmed) == 0 {
			continue
		}
		// 顶层数组：逐元素当单条解析（可能仍含 accounts 包裹，交给 ParseRelay）
		if trimmed[0] == '[' {
			var items []json.RawMessage
			if err := json.Unmarshal(trimmed, &items); err != nil {
				if firstErr == nil {
					firstErr = fmt.Errorf("relay_parse_error: %w", err)
				}
				continue
			}
			for _, it := range items {
				list, err := ParseRelay(it)
				if err != nil {
					if firstErr == nil {
						firstErr = err
					}
					continue
				}
				out = append(out, list...)
			}
			continue
		}
		list, err := ParseRelay(trimmed)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		out = append(out, list...)
	}
	if len(out) == 0 {
		if firstErr != nil {
			return nil, firstErr
		}
		return nil, fmt.Errorf("relay_parse_error: no accounts")
	}
	return out, nil
}

// splitJSONDocs 把「一串首尾相接的 JSON 文档」切成若干个文档。
// decoding 失败（尾部截断等）时返回已成功解析的前缀文档。
func splitJSONDocs(raw []byte) [][]byte {
	dec := json.NewDecoder(bytes.NewReader(raw))
	var docs [][]byte
	for {
		var v json.RawMessage
		if err := dec.Decode(&v); err != nil {
			if err == io.EOF {
				break
			}
			break // 尾部脏数据不影响已解出的文档
		}
		if len(v) == 0 {
			continue
		}
		docs = append(docs, v)
	}
	if len(docs) == 0 {
		// 不是合法 JSON 流时，把原文当单文档试一次（由调用方报错）
		if t := bytes.TrimSpace(raw); len(t) > 0 {
			return [][]byte{t}
		}
	}
	return docs
}

// HasAccounts 报告文件内容是否是一条（或一串）看起来像账号导出的 JSON。
// 用于面板/接口判断走批量通道还是单条通道。
func HasAccounts(raw []byte) bool {
	for _, doc := range splitJSONDocs(raw) {
		t := bytes.TrimSpace(doc)
		if len(t) == 0 {
			continue
		}
		if t[0] == '[' {
			return true
		}
		if LooksLikeRelay(t) {
			return true
		}
	}
	return false
}
