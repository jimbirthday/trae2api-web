// jwt.go 只读解析 Cloud-IDE-JWT 的 payload，用于导入第三方导出格式时补齐
// 缺失的 user_id / 有效期（导出格式 A 只有 token，没有 user_id、expired_at）。
//
// 只解码不验签：本函数的用途是「拿本地已知字段」，不是安全边界；
// 真正鉴权仍由上游校验签名。
package auth

import (
	"encoding/base64"
	"encoding/json"
	"strings"
)

// JWTInfo 从 JWT payload 提取的可用字段。
type JWTInfo struct {
	UID    string // data.id（TRAE 的 user id）
	Exp    int64  // exp（Unix 秒）
	Iat    int64  // iat（Unix 秒）
	Tenant string // data.tenant_id
	Type   string // data.type（user 等）
}

// ParseJWT 解码 JWT 的 payload 段（不验签）。非法 token 返回零值。
func ParseJWT(token string) JWTInfo {
	token = strings.TrimSpace(token)
	parts := strings.Split(token, ".")
	if len(parts) < 2 {
		return JWTInfo{}
	}
	payload, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(parts[1], "="))
	if err != nil {
		// 兼容带 padding 的 base64url
		if payload, err = base64.URLEncoding.DecodeString(parts[1]); err != nil {
			return JWTInfo{}
		}
	}
	var doc struct {
		Exp  int64 `json:"exp"`
		Iat  int64 `json:"iat"`
		Data struct {
			ID       string `json:"id"`
			TenantID string `json:"tenant_id"`
			Type     string `json:"type"`
		} `json:"data"`
		// 兜底：部分 token 把字段放在顶层
		ID       string `json:"id"`
		TenantID string `json:"tenant_id"`
	}
	if err := json.Unmarshal(payload, &doc); err != nil {
		return JWTInfo{}
	}
	info := JWTInfo{Exp: doc.Exp, Iat: doc.Iat, Tenant: doc.Data.TenantID, Type: doc.Data.Type}
	info.UID = strings.TrimSpace(doc.Data.ID)
	if info.UID == "" {
		info.UID = strings.TrimSpace(doc.ID)
	}
	if info.Tenant == "" {
		info.Tenant = strings.TrimSpace(doc.TenantID)
	}
	if info.Exp > 1e11 { // 毫秒
		info.Exp /= 1000
	}
	if info.Iat > 1e11 {
		info.Iat /= 1000
	}
	return info
}
