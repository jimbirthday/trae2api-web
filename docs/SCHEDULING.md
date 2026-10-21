# trae2api-web 调度内核 · 接口契约（v1）

本文件是并行开发的**唯一契约**。三个包各自实现下面的 API，主线程负责 server/cmd 集成。
TRAE 只有 CN 单域、无企业号、无多租户 Key。模型级错误**未知**，
因此模型级冷却以「可配置规则表 + 空默认」实现：规则为空时永不触发。

## 1. internal/logfmt（新包）

```go
package logfmt
func Truncate(s string, n int) string        // 按 rune 边界截断，先 TrimSpace
func UID8(uid string) string                 // 前 8 位；空 → "-"
func Label(uid, nick string) string          // "昵称(a1b2c3d4)"；昵称为空退 UID8
func DisplayWidth(s string) int              // CJK/emoji 记 2 列
func Pad(s string, width int) string         // 右补空格到显示宽度
func ShortUA(ua string) string               // 取 name/version token，非渲染引擎
```

## 2. internal/pool（重写）

依赖只允许 `auth` + `logfmt`。**不得依赖 upstream**（用注入的回调）。

### 2.1 类型

```go
type CoolKind int
const (
    CoolPlan  CoolKind = iota // 1005 plan 权益不足 → 长冷却（默认 12h）
    CoolSoft                  // 429/404 → 短冷却，指数退避
    CoolErr                   // 5xx/传输/连续错误 → 中冷却
    CoolModel                 // 模型级不可用（预留，规则可配）
)

type TokenUsage struct {
    RequestCount     int64      `json:"request_count,omitempty"`
    PromptTokens     int64      `json:"prompt_tokens,omitempty"`
    CompletionTokens int64      `json:"completion_tokens,omitempty"`
    TotalTokens      int64      `json:"total_tokens,omitempty"`
    LastLatencyMs    int64      `json:"last_latency_ms,omitempty"`
    LastUsedAt       time.Time  `json:"last_used_at,omitempty"`
    LastModel        string     `json:"last_model,omitempty"`
}

type TokenUsageDelta struct {
    Model string
    PromptTokens, CompletionTokens, TotalTokens int64
    LatencyMs int64
    HasUsage bool
}

type RateLimitedModel struct {
    Model  string    `json:"model"`
    Kind   string    `json:"kind"`    // "rate_limit" | "model_unavailable" | "unknown_model"
    Until  time.Time `json:"until,omitempty"`
    Reason string    `json:"reason,omitempty"`
}

type ModelBlockStatus struct {
    Model   string `json:"model"`
    Blocked bool   `json:"blocked"`
    Until   time.Time `json:"until,omitempty"`
}

type Counts struct {
    Total, Healthy, Cooling, Disabled, Paused int
}

type Status struct {
    UID, Nickname string
    Weight int
    Note string
    HasTokens bool
    Credits, CreditsTotal int64
    Cooling bool
    CoolKind string
    CoolRemaining int64
    Until time.Time
    Reason string
    SoftStreak int
    RateLimitedModels []RateLimitedModel
    Realm string
    Enterprise bool
    Disabled bool
    DisabledReason string
    Paused bool
    SuccessCount, ErrTotal int64
    LastSuccessTime, LastErrTime time.Time
    InFlight int
    BreakerFails int
    BreakerUntil time.Time
    ConsecutiveFails int
    DegradeUntil time.Time
    TokenUsage TokenUsage
    CheckinDone bool
}

type Limits struct {
    MaxInFlight    int           // 单账号在途上限，0=不限（默认 2）
    SoftRateBase   time.Duration // 软冷却基数（默认 60s）
    SoftRateMax    time.Duration // 软冷却退避封顶（默认 30m）
    BreakerThresh  int           // 连续失败触发熔断（默认 3）
    BreakerBase    time.Duration // 熔断基数（默认 10m）
    BreakerMax     time.Duration // 熔断封顶（默认 1h）
    DegradeThresh  int           // 连败降权阈值（默认 5）
    DegradeWindow  time.Duration // 降权时长（默认 10m）
}
```

### 2.2 构造与账号增删

```go
func New(stateFp string) *Pool
func (p *Pool) SetLimits(l Limits)
func (p *Pool) Add(a *auth.Auth)
func (p *Pool) SyncToDir(auths []*auth.Auth)
func (p *Pool) Remove(uid string) bool
```

### 2.3 选号

```go
func (p *Pool) Pick() *auth.Auth
func (p *Pool) PickExcluding(tried map[string]bool) *auth.Auth
func (p *Pool) PickExcludingForModel(tried map[string]bool, reqModel string) *auth.Auth
func (p *Pool) PickByUIDForModel(uid, model string) *auth.Auth  // 粘性命中路径，nil=不可用
func (p *Pool) HasRotatable(tried map[string]bool, reqModel string) bool
func (p *Pool) AvailableUIDs() []string
func (p *Pool) AvailableForModel(model string) []string
func (p *Pool) UIDAvailableForModel(uid, model string) bool
```

候选条件（全满足）：`!disabled && enabled && !paused && 不在账号级冷却 && 不在该模型级冷却 && inFlight < maxInFlight`。
权重：`credits 因子 × 闲置补偿 × 手动权重 × 降权系数`；按权重排序取 Top5，Top5 内加权随机。
排序在锁外、提交在短写锁内复核（避免长锁）。

### 2.4 在途租约

```go
func (p *Pool) Acquire(uid string) bool  // CAS 抢名额；满/不存在返回 false
func (p *Pool) Release(uid string)
func (p *Pool) InFlight(uid string) int
```
Acquire/Release 必须成对且幂等安全（Release 不会把计数减成负数）。

### 2.5 状态迁移

```go
func (p *Pool) SetEnabled(uid string, enabled bool, reason string) bool
func (p *Pool) Pause(uid string) bool
func (p *Pool) Resume(uid string) bool
func (p *Pool) Disable(uid, reason string)      // session 终态
func (p *Pool) Revive(uid string) bool          // 解除 disabled
func (p *Pool) Cooldown(uid string, kind CoolKind, d time.Duration, reason string)
func (p *Pool) CooldownSoftRate(uid string, base time.Duration, resetAt time.Time, reason string)
func (p *Pool) CooldownModel(uid, model string, d time.Duration, reason string)
func (p *Pool) NoteSuccess(uid string)
func (p *Pool) NoteError(uid string)            // 累计 errCount，达阈值转冷却
func (p *Pool) NoteFailures(uid string)         // 连败计数，达阈值降权
func (p *Pool) NoteUsage(uid string, delta TokenUsageDelta)
```

`CooldownSoftRate`：streak 指数退避 `base * 2^(streak-1)`，封顶 SoftRateMax；resetAt 非零且更晚时取 resetAt（但不超封顶）。
`CooldownModel`：只锁该模型，不影响账号级；同一 (uid,model) 已有更长且未过期条目时不缩短。

### 2.6 额度

```go
func (p *Pool) SetCredits(uid string, credits, total int64)
func (p *Pool) SetCreditsDetailed(uid string, credits, total, expiring int64, earliestAt time.Time, earliestRemaining int64)
func (p *Pool) ReenableIfCredits(uid string, remain int64) // 冷却中且 remain>0 → 解冻
func (p *Pool) NoteCheckinDone(uid string)
```

### 2.7 查询

```go
func (p *Pool) Status(uid string) (Status, bool)
func (p *Pool) List() []Status            // 按 uid 排序
func (p *Pool) AuthByUID(uid string) *auth.Auth
func (p *Pool) Counts() Counts
func (p *Pool) ModelBlocked(model string) ModelBlockStatus
```

### 2.8 持久化

`state.json`：
```json
{"accounts": {"<uid>": {
  "credits": 0, "credits_total": 0, "disabled": false, "enabled": true,
  "paused": false, "reason": "", "until": "...", "cool_kind": "soft_rate",
  "soft_streak": 0, "breaker_fails": 0, "breaker_until": "...",
  "consecutive_fails": 0, "success_count": 0, "err_total": 0,
  "checkin_done": false, "token_usage": {...},
  "model_cooldowns": {"<model>": {"until":"...","kind":"...","reason":"..."}}
}}}
```
原子写（tmp + rename，0600），写盘在锁外（saveMu 串行化），带 dirty 标记与 5s 后台 flusher（或每次变更同步落盘，简化即可）。
`enabled` 缺省按 true 读（向后兼容现有 state.json）。

## 3. internal/session（新包）

纯内存会话粘性，无 Redis（保留 Store 接口以便未来接入，默认 nil）。

```go
package session
type Config struct {
    TTL       time.Duration
    GCInterval time.Duration
    Available func() []string
    AvailableForModel func(model string) []string
    CanRoute func(model, uid string) bool
}
type Router struct{ /* ... */ }
func New(cfg Config) *Router
func (r *Router) StartGC()
func (r *Router) StopGC()
func (r *Router) Resolve(key string) (string, bool)
func (r *Router) ResolveForModel(key, model string) (uid string, ok bool)
func (r *Router) Bind(key, uid string)
func (r *Router) Unbind(key string) bool
func (r *Router) Count() int

// 会话键提取：从 OpenAI 请求 body 派生
func ExtractKey(body []byte) string
func ExtractKeyFromFields(f KeyFields) string
type KeyFields struct{ User string; System string; FirstUser string }
func ResolveConversationID(body []byte) string
```

提取优先级：`user` 字段 → 首条 user 消息内容 hash → 全量 messages hash。空 body 返回 ""。
`ResolveForModel` 命中后须校验 `CanRoute(model, uid)`（nil 时回落到 AvailableForModel 线性包含），不可用则 Unbind 并返回 false。

## 4. internal/reqlog（新包）

内存环形缓冲（最近 100 条）+ 可选 JSONL 归档（按天轮转、容量上限、过期清理）。

```go
package reqlog
func NewRequestID() string
type Config struct{ Dir string; Enabled bool; FileMaxBytes int64; RetainDays int; QueueSize int }
type Event struct {
    RequestID string; Time time.Time; Path string; Model string
    UID, Nickname string; Status int; Outcome string
    LatencyMs int64; PromptTokens, CompletionTokens, TotalTokens int64
    Rotates int; Stream bool; Error string; ClientIP, UserAgent string
}
type Recorder struct{ /* ... */ }
func New(cfg Config) *Recorder
func (r *Recorder) Record(e Event)
func (r *Recorder) Snapshot() Snapshot
func (r *Recorder) Recent(limit int) []Event
func (r *Recorder) Metrics() Metrics
func (r *Recorder) ReadArchive(limit int, filter Filter) ([]Event, error)
func (r *Recorder) Close()
```
`Record` 绝不阻塞（队列满丢弃并计数）。不得记录提示词/响应正文/凭证。

## 5. internal/upstream（增量修改，保留现有 SOLO 协议）

### 5.1 错误分类扩展

```go
// ErrKind 追加：
ErrModelBlocked   // 模型级不可用（由可配规则判定）

// 可配置的模型级错误规则（默认空 = 永不触发）
type ModelErrorRule struct {
    Code    int      // 上游业务码，如 6004（0 = 不用码匹配）
    Status  int      // HTTP 状态码（0 = 不匹配状态）
    Contains []string // body 子串（AND）
    Kind    string   // "rate_limit" | "model_unavailable" | "unknown_model"
}
func (c *Client) SetModelErrorRules(rules []ModelErrorRule)
func (c *Client) MatchModelError(status int, body string) (kind string, ok bool)
```

### 5.2 Retry-After

```go
func ParseRetryAfter(h http.Header, now time.Time) time.Duration  // 秒数/HTTP-date，0=未给
```

### 5.3 超时/取消判定

```go
func IsUpstreamTimeout(err error) bool   // net.Error.Timeout / DeadlineExceeded
func IsClientGone(err error) bool        // context.Canceled
```

### 5.4 Usage 提取

```go
// 从 SOLO token_usage 事件映射到 pool.TokenUsageDelta 所需字段
func UsageFromEvent(ev *SOLOEvent) (prompt, completion, total int64, ok bool)
```

### 5.5 现有保持不变

`ChatStream` / `FetchModels` / `RefreshToken` / `CheckinStatus` / `CheckinClaim` / `EntUsage` 签名不变。
`Error` 结构新增 `RetryAfter time.Duration` 与 `Model string`。

## 6. 分工与写入边界

| 角色 | 写入范围 |
|---|---|
| Agent A | `internal/pool/**`, `internal/logfmt/**` |
| Agent B | `internal/session/**`, `internal/reqlog/**` |
| Agent C | `internal/upstream/**` |
| 主线程 | `internal/server/**`, `internal/scheduler/**`, `cmd/**`, `config*`, `README.md`, `admin.html` |

任何一方**不得**修改本文件之外的他人目录；跨包调用只允许按本契约的签名。
