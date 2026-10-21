package session

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
)

// KeyFields 是会话键提取所需的已解析字段，供 handler 复用同一次 JSON 解码。
type KeyFields struct {
	// User 是 OpenAI 兼容请求体的 user 字段（客户端自带的稳定标识）。
	User string
	// System 是首条 system/developer 消息的文本。
	System string
	// FirstUser 是首条 role=="user" 消息的文本。
	FirstUser string
}

// ExtractKey 从 OpenAI 兼容请求体派生会话键。
//
// 优先级：body 的 user 字段 → 首条 user 消息内容 sha256 → 全量 messages sha256。
// 空 body / 无法解析 → ""（无会话语义，调用方按请求级随机处理）。
func ExtractKey(body []byte) string {
	if len(body) == 0 {
		return ""
	}
	var req struct {
		User     string          `json:"user"`
		Messages []wireMessage   `json:"messages"`
		Input    json.RawMessage `json:"input"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return ""
	}
	f := KeyFields{User: strings.TrimSpace(req.User)}
	for _, m := range req.Messages {
		text := messageText(m.Content)
		switch m.Role {
		case "system", "developer":
			if f.System == "" {
				f.System = text
			}
		case "user":
			if f.FirstUser == "" {
				f.FirstUser = text
			}
		}
		if f.System != "" && f.FirstUser != "" {
			break
		}
	}
	if k := ExtractKeyFromFields(f); k != "" {
		return k
	}
	// 第三优先级：全量 messages 归一化后 hash（无 user 文本但有对话内容时仍可粘住）。
	if len(req.Messages) > 0 {
		return hashParts("msgs", messagesSignature(req.Messages))
	}
	return ""
}

// ExtractKeyFromFields 是 ExtractKey 的已解析版本，语义一致：
// user → 首条 user 消息 hash → 首条 system 消息 hash（无对话内容时的兜底）。
func ExtractKeyFromFields(f KeyFields) string {
	if u := strings.TrimSpace(f.User); u != "" {
		return "user:" + u
	}
	if t := strings.TrimSpace(f.FirstUser); t != "" {
		return hashParts("u1", t)
	}
	if t := strings.TrimSpace(f.System); t != "" {
		return hashParts("sys", t)
	}
	return ""
}

// wireMessage 是请求体消息的最小视图。
type wireMessage struct {
	Role    string          `json:"role"`
	Content json.RawMessage `json:"content"`
}

// messageText 抽取消息文本：content 可为 string，或 [{"type":"text","text":"..."}] 数组。
func messageText(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	var parts []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if err := json.Unmarshal(raw, &parts); err == nil {
		var b strings.Builder
		for _, p := range parts {
			if p.Type == "text" || p.Type == "" {
				b.WriteString(p.Text)
			}
		}
		return b.String()
	}
	return ""
}

// messagesSignature 把全部消息归一化成签名文本（角色 + 文本，跳过无文本内容）。
func messagesSignature(msgs []wireMessage) string {
	var b strings.Builder
	for _, m := range msgs {
		text := messageText(m.Content)
		if text == "" {
			continue
		}
		b.WriteString(strings.ToLower(strings.TrimSpace(m.Role)))
		b.WriteByte(0)
		b.WriteString(text)
		b.WriteByte(1)
	}
	return b.String()
}

// hashParts 对若干段文本做带前缀的 sha256（前缀避免不同来源的键撞在同一空间）。
func hashParts(prefix string, parts ...string) string {
	h := sha256.New()
	h.Write([]byte(prefix))
	for _, p := range parts {
		h.Write([]byte{0})
		h.Write([]byte(p))
	}
	return hex.EncodeToString(h.Sum(nil))[:32]
}

// ResolveConversationID 从请求体提取 conversationId（snake/camel 双形态，metadata 优先）。
// 找不到返回 ""；绝不回落到 user_id。
func ResolveConversationID(body []byte) string {
	if len(body) == 0 {
		return ""
	}
	var obj map[string]any
	if err := json.Unmarshal(body, &obj); err != nil {
		return ""
	}
	if meta, ok := obj["metadata"].(map[string]any); ok {
		if v := strValue(meta["conversation_id"]); v != "" {
			return v
		}
		if v := strValue(meta["conversationId"]); v != "" {
			return v
		}
	}
	if v := strValue(obj["conversation_id"]); v != "" {
		return v
	}
	return strValue(obj["conversationId"])
}

func strValue(v any) string {
	if s, ok := v.(string); ok {
		return strings.TrimSpace(s)
	}
	return ""
}
