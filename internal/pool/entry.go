// 条目与对外状态类型 + 健康判定。
package pool

import (
	"sync/atomic"
	"time"

	"trae2api-web/internal/auth"
)

// CoolKind 冷却类型。
type CoolKind int

const (
	CoolPlan  CoolKind = iota // 1005 plan 权益不足 → 长冷却（默认 12h）
	CoolSoft                  // 429/404 → 短冷却，指数退避
	CoolErr                   // 5xx/传输/连续错误 → 中冷却
	CoolModel                 // 模型级不可用（预留，规则可配）
)

func (k CoolKind) String() string {
	switch k {
	case CoolPlan:
		return "plan_limit"
	case CoolSoft:
		return "soft_rate"
	case CoolErr:
		return "error_threshold"
	case CoolModel:
		return "model_blocked"
	}
	return "unknown"
}

// TokenUsage 账号聊天请求的累计 token 用量摘要（不含任何原始凭证）。
type TokenUsage struct {
	RequestCount     int64 `json:"request_count,omitempty"`
	PromptTokens     int64 `json:"prompt_tokens,omitempty"`
	CompletionTokens int64 `json:"completion_tokens,omitempty"`
	TotalTokens      int64 `json:"total_tokens,omitempty"`
	// CacheReadTokens 上游命中 prompt 缓存的输入 token 数（TRAE 的
	// token_usage.cache_read_input_tokens）。用于算缓存率：
	//   缓存率 = CacheReadTokens / PromptTokens
	CacheReadTokens int64 `json:"cache_read_tokens,omitempty"`
	// ReasoningTokens 思考链 token（token_usage.reasoning_tokens）。
	// 计入 completion_tokens，单列出来便于观察推理模型的成本构成。
	ReasoningTokens int64     `json:"reasoning_tokens,omitempty"`
	LastLatencyMs   int64     `json:"last_latency_ms,omitempty"`
	LastUsedAt      time.Time `json:"last_used_at,omitempty"`
	LastModel       string    `json:"last_model,omitempty"`
}

// TokenUsageDelta 是一次聊天尝试的 usage 增量。HasUsage 区分「上游没给」与「确实为 0」。
type TokenUsageDelta struct {
	Model            string
	PromptTokens     int64
	CompletionTokens int64
	TotalTokens      int64
	CacheReadTokens  int64
	ReasoningTokens  int64
	LatencyMs        int64
	HasUsage         bool
}

// RateLimitedModel 当前仍在限额的模型条目（面板展示用）。
type RateLimitedModel struct {
	Model  string    `json:"model"`
	Kind   string    `json:"kind"`
	Until  time.Time `json:"until,omitempty"`
	Reason string    `json:"reason,omitempty"`
}

// ModelBlockStatus 单个模型的全局可用性（跨账号聚合）。
type ModelBlockStatus struct {
	Model   string    `json:"model"`
	Blocked bool      `json:"blocked"`
	Until   time.Time `json:"until,omitempty"`
}

// Counts 池内账号分类计数。
type Counts struct {
	Total    int `json:"total"`
	Healthy  int `json:"healthy"`
	Cooling  int `json:"cooling"`
	Disabled int `json:"disabled"`
	Paused   int `json:"paused"`
}

// Status 单个账号对外暴露的状态（脱敏，不含 token）。
type Status struct {
	UID      string `json:"uid"`
	Nickname string `json:"nickname,omitempty"`
	// Weight / Note 手动调度权重（1 = 不干预）与面板备注；Note 纯展示。
	Weight    int    `json:"weight,omitempty"`
	Note      string `json:"note,omitempty"`
	HasTokens bool   `json:"has_tokens"`

	Credits      int64 `json:"credits"`
	CreditsTotal int64 `json:"credits_total,omitempty"`

	Cooling       bool      `json:"cooling"`
	CoolKind      string    `json:"cool_kind,omitempty"`
	CoolRemaining int64     `json:"cool_remaining_sec,omitempty"`
	Until         time.Time `json:"until,omitempty"`
	Reason        string    `json:"reason,omitempty"`
	SoftStreak    int       `json:"soft_streak,omitempty"`

	RateLimitedModels []RateLimitedModel `json:"rate_limited_models,omitempty"`

	Realm        string `json:"realm,omitempty"`
	Enterprise   bool   `json:"enterprise,omitempty"`
	EnterpriseID string `json:"enterprise_id,omitempty"`

	Disabled       bool   `json:"disabled"`
	DisabledReason string `json:"disabled_reason,omitempty"`
	Paused         bool   `json:"paused,omitempty"`

	Enabled bool `json:"enabled"`

	SuccessCount    int64     `json:"success_count,omitempty"`
	ErrTotal        int64     `json:"err_total,omitempty"`
	LastSuccessTime time.Time `json:"last_success_time,omitempty"`
	LastErrTime     time.Time `json:"last_err_time,omitempty"`

	InFlight int `json:"in_flight,omitempty"`

	BreakerFails int       `json:"breaker_fails,omitempty"`
	BreakerUntil time.Time `json:"breaker_until,omitempty"`

	ConsecutiveFails int       `json:"consecutive_fails,omitempty"`
	DegradeUntil     time.Time `json:"degrade_until,omitempty"`
	// ErrCount 是 NoteError 的短计数（达 BreakerThresh 转熔断），供面板观测。
	ErrCount int `json:"err_count,omitempty"`

	TokenUsage  TokenUsage `json:"token_usage,omitempty"`
	CheckinDone bool       `json:"checkin_done,omitempty"`
}

// modelCool 单个模型的冷却台账条目。
type modelCool struct {
	Until  time.Time `json:"until,omitempty"`
	Kind   string    `json:"kind,omitempty"`
	Reason string    `json:"reason,omitempty"`
}

// entry 池内单账号运行态。除 inFlight（atomic）外，字段都受 p.mu 保护。
type entry struct {
	a *auth.Auth

	// 硬禁用（session 失效终态）与软开关（用户可逆启停）。
	disabled       bool
	disabledReason string
	enabled        bool
	paused         bool

	credits      int64
	creditsTotal int64

	// 账号级冷却（until/reason 与 coolKind 同源）。
	until    time.Time
	reason   string
	coolKind string

	softStreak int

	breakerFails int
	breakerUntil time.Time

	consecutiveFails int
	degradeUntil     time.Time

	// errCount 是 NoteError 的短计数（达 BreakerThresh 转熔断）。
	errCount int

	successCount int64
	errTotal     int64
	lastSuccess  time.Time
	lastErr      time.Time

	modelCooldowns map[string]modelCool

	tokenUsage  TokenUsage
	checkinDone bool

	// 选号热路径状态：lastUsed 用于闲置补偿，usedSeq 记录最近使用序号。
	lastUsed time.Time
	usedSeq  uint64

	inFlight atomic.Int32
}

func newEntry(a *auth.Auth) *entry {
	return &entry{a: a, enabled: true, modelCooldowns: map[string]modelCool{}}
}

// healthy 账号级可用：未禁用、开关打开、未暂停、不在账号级冷却/熔断期内。
//
// 连败降权（degradeUntil）不在候选条件里：它是「降权」而非「出池」，
// 由选号权重乘 0.2 体现（见 degradeFactor），账号仍可被选中只是概率降低。
func (e *entry) healthy(now time.Time) bool {
	if e.disabled || !e.enabled || e.paused {
		return false
	}
	if now.Before(e.until) {
		return false
	}
	if now.Before(e.breakerUntil) {
		return false
	}
	return true
}

// modelCooled 报告该账号在指定模型上是否仍在冷却。
func (e *entry) modelCooled(now time.Time, model string) bool {
	if model == "" || len(e.modelCooldowns) == 0 {
		return false
	}
	c, ok := e.modelCooldowns[model]
	if !ok {
		return false
	}
	return now.Before(c.Until)
}

// healthyForModel 账号级健康 + 该模型未冷却。
func (e *entry) healthyForModel(now time.Time, model string) bool {
	return e.healthy(now) && !e.modelCooled(now, model)
}

// pendingModelCools 返回未过期的模型级冷却（按模型名排序由调用方保证可读性）。
func (e *entry) pendingModelCools(now time.Time) []RateLimitedModel {
	if len(e.modelCooldowns) == 0 {
		return nil
	}
	out := make([]RateLimitedModel, 0, len(e.modelCooldowns))
	for m, c := range e.modelCooldowns {
		if now.Before(c.Until) {
			out = append(out, RateLimitedModel{Model: m, Kind: c.Kind, Until: c.Until, Reason: c.Reason})
		}
	}
	return out
}

// manualWeight 返回归一化后的手动调度权重。
func (e *entry) manualWeight() int {
	if e.a == nil {
		return 1
	}
	return e.a.WeightValue()
}

// profileSnapshot 返回 weight/note/nickname（无凭证时安全回落）。
func (e *entry) profileSnapshot() (weight int, note, nickname string) {
	if e.a == nil {
		return 1, "", ""
	}
	w, n, nick, _ := e.a.ProfileSnapshot()
	return w, n, nick
}

// hasTokens 凭证完整度：accessToken + refreshToken 都有。
func (e *entry) hasTokens() bool {
	if e.a == nil {
		return false
	}
	return e.a.AccessTokenValue() != "" && e.a.RefreshTokenValue() != ""
}
