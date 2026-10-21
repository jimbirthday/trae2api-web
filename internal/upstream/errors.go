// errors.go 错误分类扩展：模型级错误规则（可配置，默认空）、Retry-After 解析、
// 超时/取消判定、流内错误分类钩子。
package upstream

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ModelErrorRule 模型级错误规则（config 注入）。
// 三个条件均为「有值才参与匹配」，全部满足才算命中：
//   - Code != 0：body 含该业务码（兼容 `"code":1234`、`"code": 1234` 与裸数字）
//   - Status != 0：HTTP 状态码相等
//   - Contains 非空：所有子串（大小写不敏感）都命中
//
// 无任何条件的规则视为无效，永不命中。
type ModelErrorRule struct {
	Code     int
	Status   int
	Contains []string
	Kind     string // "rate_limit" | "model_unavailable" | "unknown_model"
}

const (
	ModelKindRateLimit        = "rate_limit"
	ModelKindModelUnavailable = "model_unavailable"
	ModelKindUnknownModel     = "unknown_model"
)

// modelRuleMu 保护 modelRules（Set 可能与并发请求竞争）。
var modelRuleMu sync.RWMutex

// SetModelErrorRules 注入/替换模型级错误规则表；nil/空 = 永不触发。
func (c *Client) SetModelErrorRules(rules []ModelErrorRule) {
	cp := make([]ModelErrorRule, 0, len(rules))
	for _, r := range rules {
		if r.Code == 0 && r.Status == 0 && len(r.Contains) == 0 {
			continue // 无效规则丢弃
		}
		c2 := r
		if len(r.Contains) > 0 {
			c2.Contains = append([]string(nil), r.Contains...)
		}
		if c2.Kind == "" {
			c2.Kind = ModelKindModelUnavailable
		}
		cp = append(cp, c2)
	}
	modelRuleMu.Lock()
	c.modelRules = cp
	modelRuleMu.Unlock()
}

// ModelErrorRules 返回当前规则表副本（供管理端展示）。
func (c *Client) ModelErrorRules() []ModelErrorRule {
	modelRuleMu.RLock()
	defer modelRuleMu.RUnlock()
	return append([]ModelErrorRule(nil), c.modelRules...)
}

// MatchModelError 按注入的规则匹配模型级错误；未配置规则时恒为 false。
func (c *Client) MatchModelError(status int, body string) (kind string, ok bool) {
	modelRuleMu.RLock()
	rules := c.modelRules
	modelRuleMu.RUnlock()
	if len(rules) == 0 {
		return "", false
	}
	lower := strings.ToLower(body)
	for _, r := range rules {
		if r.Code != 0 && !containsBizCode(body, r.Code) {
			continue
		}
		if r.Status != 0 && r.Status != status {
			continue
		}
		hit := true
		for _, sub := range r.Contains {
			if sub == "" {
				continue
			}
			if !strings.Contains(lower, strings.ToLower(sub)) {
				hit = false
				break
			}
		}
		if !hit {
			continue
		}
		k := r.Kind
		if k == "" {
			k = ModelKindModelUnavailable
		}
		return k, true
	}
	return "", false
}

// bizCodeFromBody 结构化取业务码：优先顶层 code，其次 error.code，再看
// err_code/error_code/status。取不到（非 JSON / 无该字段）返回 ok=false。
//
// 为什么不继续用子串匹配：`"1005"` 这种子串会命中 message 里恰好出现的数字、
// 也可能命中 11005 这类别的码。先解析 JSON 再比值才稳；只有整段 body 根本不是
// JSON 时才退回 containsBizCode 的宽松判断。
func bizCodeFromBody(body string) (int64, bool) {
	if body == "" {
		return 0, false
	}
	var raw map[string]any
	if json.Unmarshal([]byte(body), &raw) != nil {
		return 0, false
	}
	keys := []string{"code", "err_code", "error_code", "status", "status_code"}
	for _, k := range keys {
		if v, ok := numField(raw, k); ok {
			return v, true
		}
	}
	if e, ok := raw["error"].(map[string]any); ok {
		for _, k := range keys {
			if v, ok := numField(e, k); ok {
				return v, true
			}
		}
	}
	return 0, false
}

// containsBizCode 判定 body 是否含业务码（JSON code 字段或独立数字 token）。
func containsBizCode(body string, code int) bool {
	s := strconv.Itoa(code)
	if strings.Contains(body, `"code":`+s) || strings.Contains(body, `"code": `+s) ||
		strings.Contains(body, `"Code":`+s) || strings.Contains(body, `"Code": `+s) {
		return true
	}
	return containsNumberToken(body, s)
}

// containsNumberToken 找独立数字 token（前后都不是数字，避免 1005 命中 11005）。
func containsNumberToken(s, tok string) bool {
	idx := 0
	for {
		i := strings.Index(s[idx:], tok)
		if i < 0 {
			return false
		}
		i += idx
		before, after := byte(' '), byte(' ')
		if i > 0 {
			before = s[i-1]
		}
		if i+len(tok) < len(s) {
			after = s[i+len(tok)]
		}
		if !isDigitByte(before) && !isDigitByte(after) {
			return true
		}
		idx = i + len(tok)
	}
}

func isDigitByte(b byte) bool { return b >= '0' && b <= '9' }

// ParseRetryAfter 从响应头解析建议重试等待时长，0 表示上游未给出。
// 支持：Retry-After（秒数 / HTTP-date）、retry-after-ms（毫秒）、
// x-ratelimit-reset（unix 秒或毫秒，取相对 now 的差值）。
func ParseRetryAfter(h http.Header, now time.Time) time.Duration {
	if h == nil {
		return 0
	}
	if v := strings.TrimSpace(h.Get("Retry-After")); v != "" {
		if d, ok := parseRetryAfterValue(v, now); ok && d > 0 {
			return d
		}
	}
	if v := strings.TrimSpace(h.Get("retry-after-ms")); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f > 0 {
			return time.Duration(f * float64(time.Millisecond))
		}
	}
	if v := strings.TrimSpace(h.Get("x-ratelimit-reset")); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f > 0 {
			var reset time.Time
			if f > 1e12 { // 毫秒时间戳
				reset = time.UnixMilli(int64(f))
			} else {
				reset = time.Unix(int64(f), 0)
			}
			if d := reset.Sub(now); d > 0 {
				return d
			}
		}
	}
	return 0
}

// parseRetryAfterValue 秒数或 HTTP-date。
func parseRetryAfterValue(v string, now time.Time) (time.Duration, bool) {
	if f, err := strconv.ParseFloat(v, 64); err == nil {
		if f <= 0 {
			return 0, false
		}
		return time.Duration(f * float64(time.Second)), true
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := t.Sub(now); d > 0 {
			return d, true
		}
		return 0, false
	}
	return 0, false
}

// ErrStreamIdleTimeout 上游 SSE 在「帧间空闲」上限内一个字节都没吐。
//
// 为什么需要独立哨兵：这类中断既不是客户端取消（context.Canceled），也不是
// 账号本身有问题（上游可能只是把自己的推理进程挂住了）。用独立哨兵才能让
// 上层区分「别罚号」与「换号」，也避免 IsClientGone 误判。
var ErrStreamIdleTimeout = errors.New("upstream stream idle timeout")

// ErrEmptyCompletion 上游整条流里没有任何正文/思考/工具调用帧。
//
// 以前这种流会被聚合/转发成「成功但内容为空、finish_reason=stop」，
// 客户端拿到空回复且无从重试。现在显式报错，交给上层换号重试。
var ErrEmptyCompletion = errors.New("upstream returned empty completion")

// ErrTruncatedStream 上游流有内容但缺少 done 收尾（连接被中途掐断）。
// 上层据此判定「可换号重试」，而不是把半句回复当成功。
var ErrTruncatedStream = errors.New("upstream stream truncated (no done event)")

// IsUpstreamTimeout 判定是否为上游超时（含 url.Error 等包装）。
func IsUpstreamTimeout(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, os.ErrDeadlineExceeded) ||
		errors.Is(err, ErrStreamIdleTimeout) {
		return true
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return true
	}
	return false
}

// IsClientGone 判定是否为客户端主动取消（断开连接）。
func IsClientGone(err error) bool {
	return err != nil && errors.Is(err, context.Canceled)
}

// streamErrClassifier 可注入的流内错误分类器（nil = 默认规则）。
var (
	streamErrMu         sync.RWMutex
	streamErrClassifier func(code int64, msg string) ErrKind
)

// SetStreamErrorClassifier 注入 SOLO event:error 的分类逻辑；传 nil 恢复默认
// （1005 → ErrPlanLimit，其余 → ErrClient）。
// SetStreamErrorClassifier 设置进程级流内错误分类钩子（默认 1005→PlanLimit、其余→Client）。
// 若同时设置了模型级错误规则，请在 c.streamErrKind 中优先判定规则，避免误把
// 「该账号跑不了这个模型」当成账号级故障而熔断整个账号。
func SetStreamErrorClassifier(fn func(code int64, msg string) ErrKind) {
	streamErrMu.Lock()
	streamErrClassifier = fn
	streamErrMu.Unlock()
}

// classifyStreamError 按注入钩子或默认规则分类流内错误。
func classifyStreamError(code int64, msg string) ErrKind {
	streamErrMu.RLock()
	fn := streamErrClassifier
	streamErrMu.RUnlock()
	if fn != nil {
		return fn(code, msg)
	}
	if code == 1005 {
		return ErrPlanLimit
	}
	return ErrClient
}

// UsageDetail token_usage 事件的完整用量（含缓存与思考链）。
//
// 字段对应 TRAE 上游实测 event:token_usage 报文：
//
//	{"prompt_tokens":2023,"completion_tokens":133,"total_tokens":2156,
//	 "cache_creation_input_tokens":0,"cache_read_input_tokens":1920,
//	 "reasoning_tokens":127,"cluster":"normal_context"}
type UsageDetail struct {
	PromptTokens      int64
	CompletionTokens  int64
	TotalTokens       int64
	CacheReadTokens   int64 // 命中 prompt 缓存的输入 token
	CacheCreateTokens int64 // 写入缓存的 token（实测恒 0）
	ReasoningTokens   int64 // 思考链 token（已含在 completion 内）
}

// UsageDetailFromEvent 从 token_usage 事件提取完整用量。ok=false 表示无用量信息。
func UsageDetailFromEvent(ev *SOLOEvent) (UsageDetail, bool) {
	if ev == nil || ev.Usage == nil {
		return UsageDetail{}, false
	}
	var d UsageDetail
	var ok bool
	if v, o := numField(ev.Usage, "prompt_tokens"); o {
		d.PromptTokens, ok = v, true
	}
	if v, o := numField(ev.Usage, "completion_tokens"); o {
		d.CompletionTokens, ok = v, true
	}
	if v, o := numField(ev.Usage, "total_tokens"); o {
		d.TotalTokens, ok = v, true
	}
	if d.TotalTokens == 0 && (d.PromptTokens > 0 || d.CompletionTokens > 0) {
		d.TotalTokens = d.PromptTokens + d.CompletionTokens
	}
	if v, o := numField(ev.Usage, "cache_read_input_tokens"); o {
		d.CacheReadTokens = v
	}
	if v, o := numField(ev.Usage, "cache_creation_input_tokens"); o {
		d.CacheCreateTokens = v
	}
	if v, o := numField(ev.Usage, "reasoning_tokens"); o {
		d.ReasoningTokens = v
	}
	return d, ok
}

// UsageFromEvent 从 token_usage 事件提取 token 数（JSON 数字为 float64）。
// total 缺失时用 prompt+completion 兜底。ok=false 表示该事件无用量信息。
func UsageFromEvent(ev *SOLOEvent) (prompt, completion, total int64, ok bool) {
	if ev == nil || ev.Usage == nil {
		return 0, 0, 0, false
	}
	if v, okc := numField(ev.Usage, "prompt_tokens"); okc {
		prompt, ok = v, true
	}
	if v, okc := numField(ev.Usage, "completion_tokens"); okc {
		completion, ok = v, true
	}
	if v, okc := numField(ev.Usage, "total_tokens"); okc {
		total, ok = v, true
	}
	if total == 0 && (prompt > 0 || completion > 0) {
		total = prompt + completion
	}
	return prompt, completion, total, ok
}

func numField(m map[string]any, key string) (int64, bool) {
	switch v := m[key].(type) {
	case float64:
		return int64(v), true
	case int64:
		return v, true
	case int:
		return int64(v), true
	case json.Number:
		n, err := v.Int64()
		if err != nil {
			return 0, false
		}
		return n, true
	}
	return 0, false
}

// streamErrKind 按「模型级规则 → 进程级钩子 → 默认」的顺序判定流内错误类别。
//
// 流内错误（event:error）只有解析后的 code 与 message，**没有原始 JSON 体**，
// 所以不能套用 MatchModelError 的 body 子串匹配，必须按 code/msg 直接比对规则。
func (c *Client) streamErrKind(code int64, msg string) ErrKind {
	if c != nil && c.matchModelErrorCode(code, msg) {
		return ErrModelBlocked
	}
	return classifyStreamError(code, msg)
}

// matchModelErrorCode 用流内错误的 (code, message) 匹配模型级规则。
// 语义：Code!=0 时必须等于该 code；Contains 需全部命中 message（大小写不敏感）。
// 没有 Code 也没有 Contains 的规则不参与流内判定（避免误伤）。
func (c *Client) matchModelErrorCode(code int64, msg string) bool {
	if c == nil {
		return false
	}
	rules := c.ModelErrorRules()
	if len(rules) == 0 {
		return false
	}
	lower := strings.ToLower(msg)
	for _, r := range rules {
		if r.Status != 0 {
			continue // 流内错误无 HTTP 状态，带 status 的规则不参与
		}
		if r.Code == 0 && len(r.Contains) == 0 {
			continue
		}
		if r.Code != 0 && int64(r.Code) != code {
			continue
		}
		hit := true
		for _, sub := range r.Contains {
			if sub == "" {
				continue
			}
			if !strings.Contains(lower, strings.ToLower(sub)) {
				hit = false
				break
			}
		}
		if hit {
			return true
		}
	}
	return false
}

// UsageDetailFromMap 把聚合结果里的 usage map（OpenAI 形态）转成 UsageDetail。
// 非流式路径走 Aggregate，usage 是 map[string]any，需要统一到同一结构。
func UsageDetailFromMap(m map[string]any) UsageDetail {
	var d UsageDetail
	num := func(k string) int64 {
		if v, ok := numField(m, k); ok {
			return v
		}
		return 0
	}
	d.PromptTokens = num("prompt_tokens")
	d.CompletionTokens = num("completion_tokens")
	d.TotalTokens = num("total_tokens")
	if d.TotalTokens == 0 {
		d.TotalTokens = d.PromptTokens + d.CompletionTokens
	}
	d.CacheReadTokens = num("cache_read_input_tokens")
	d.CacheCreateTokens = num("cache_creation_input_tokens")
	d.ReasoningTokens = num("reasoning_tokens")
	return d
}
