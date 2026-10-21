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
	// Conversation 是显式会话 ID（metadata.conversation_id / conversationId）。
	Conversation string
	// System 是首条 system/developer 消息的文本。
	System string
	// FirstUser 是首条 role=="user" 消息的文本。
	FirstUser string
}

// ExtractKey 从 OpenAI 兼容请求体派生会话键。
//
// 优先级：
//  1. 显式 conversationId（+ user）——最强信号，客户端自己保证了会话边界；
//  2. user 字段 + 首条 user 消息 hash——同一用户的不同对话不会互相覆盖；
//  3. 首条 user 消息 hash → 首条 system 消息 hash → 全量 messages hash；
//  4. 实在推导不出来（空 body / 无对话内容且无 user）→ ""。
//
// **绝不返回固定常量**：键只要退化成 "default" 这类常量，全站所有用户会被绑到
// 同一个上游账号上（粘性失效 + 单号打爆），这比「不粘」坏得多。返回 "" 表示
// 本次请求不参与粘性，由调用方按请求级随机处理。
func ExtractKey(body []byte) string {
	if len(body) == 0 {
		return ""
	}
	var req struct {
		User     string          `json:"user"`
		Messages []wireMessage   `json:"messages"`
		Metadata json.RawMessage `json:"metadata"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return ""
	}
	// metadata 直接复用本次解码的原始片段，避免为取 conversationId 再整份
	// json.Unmarshal 一次（热路径上每次请求都会走这里）。
	f := KeyFields{
		User:         strings.TrimSpace(req.User),
		Conversation: conversationIDFromMetadata(req.Metadata),
	}
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
	// 兜底：无 user 文本但有对话内容时仍可粘住（assistant-only 等非常规请求）。
	if len(req.Messages) > 0 {
		if sig := messagesSignature(req.Messages); sig != "" {
			return hashParts("msgs", sig)
		}
	}
	return ""
}

// ExtractKeyFromFields 是 ExtractKey 的已解析版本，语义一致。
func ExtractKeyFromFields(f KeyFields) string {
	conv := strings.TrimSpace(f.Conversation)
	user := strings.TrimSpace(f.User)
	firstUser := strings.TrimSpace(f.FirstUser)
	system := strings.TrimSpace(f.System)

	if conv != "" {
		if user != "" {
			return hashParts("conv+user", user, conv)
		}
		return hashParts("conv", conv)
	}
	if user != "" {
		// 带上首条内容：同一 user 值被多家客户端/多台机器复用时不会互相串会话。
		if firstUser != "" {
			return hashParts("user+msg", user, firstUser)
		}
		if system != "" {
			return hashParts("user+sys", user, system)
		}
		return hashParts("user", user)
	}
	if firstUser != "" {
		return hashParts("u1", firstUser)
	}
	if system != "" {
		return hashParts("sys", system)
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

// conversationIDFromMetadata 从 metadata 片段里取 conversationId（两种命名）。
func conversationIDFromMetadata(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var meta map[string]any
	if err := json.Unmarshal(raw, &meta); err != nil {
		return ""
	}
	if v := strValue(meta["conversation_id"]); v != "" {
		return v
	}
	return strValue(meta["conversationId"])
}

// ResolveConversationID 从请求体提取 conversationId（snake/camel 双形态，metadata 优先）。
// 找不到返回 ""；绝不回落到 user_id。
//
// 语义：这是上游会话 ID 的原值（不 hash），用于把请求映射到上游的 conversation。
// ExtractKey 用的会话键则是 hash 后的不透明串——两者不是一回事：键要防碰撞，
// 上游 ID 要保真透传。
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
