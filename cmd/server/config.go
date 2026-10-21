// config.go 加载 JSON 配置 + TW2A_* 环境变量覆盖。
// APIKey 只从环境变量 TW2A_API_KEY 读取（key 走 env，不落盘 git）。
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"trae2api-web/internal/upstream"
)

// Config 顶层配置。
type Config struct {
	Listen       string `json:"listen"`        // ":8000"
	CallbackPort string `json:"callback_port"` // "18080"（TRAE 登录回调监听端口，0 = 不起）
	// CallbackBind 回调 server 的监听地址。容器内必须是 0.0.0.0，否则宿主/反代
	// 的请求进不来（只在容器命名空间内可达 = 回调永远超时）。
	CallbackBind string `json:"callback_bind"` // "0.0.0.0"
	// PublicBaseURL TRAE 登录回调用的公网基址，例如 https://trae.example.com。
	// 非空时回调地址 = <PublicBaseURL>/authorize（走反代/域名，推荐）；
	// 为空时退回 http://<callback_host>:<callback_port>/authorize（本机调试）。
	PublicBaseURL string `json:"public_base_url"` // ""
	// CORSOrigins /v1/* 允许的跨域来源；空 = "*"（Bearer 鉴权，无需 credentials）。
	CORSOrigins  []string `json:"cors_origins"`
	APIKey       string   `json:"-"`             // 只读 env TW2A_API_KEY（不读 json）
	AuthDir      string   `json:"auth_dir"`      // "./auths"
	StateFile    string   `json:"state_file"`    // "./data/state.json"
	DefaultModel string   `json:"default_model"` // "glm-5.2"

	Cooldown struct {
		PlanCredit  string `json:"plan_credit"`   // "12h"
		SoftRate    string `json:"soft_rate"`     // "60s"
		ErrThresh   int    `json:"err_threshold"` // 3（旧键，兼容保留 = 熔断阈值）
		ErrCooldown string `json:"err_cooldown"`  // "10m"
	} `json:"cooldown"`

	Pool struct {
		MaxInFlight        int    `json:"max_in_flight"`        // 单账号在途上限，0 = 不限（默认 2）
		SoftRateMax        string `json:"soft_rate_max"`        // 软冷却退避封顶（默认 30m）
		BreakerThreshold   int    `json:"breaker_threshold"`    // 连续失败触发熔断（默认 3）
		BreakerCooldown    string `json:"breaker_cooldown"`     // 熔断基数（默认 10m）
		BreakerCooldownMax string `json:"breaker_cooldown_max"` // 熔断封顶（默认 1h）
		DegradeThreshold   int    `json:"degrade_threshold"`    // 连败降权阈值（默认 5）
		DegradeWindow      string `json:"degrade_window"`       // 降权时长（默认 10m）
		CreditFloor        int64  `json:"credit_floor"`         // 积分保底，0 = 关闭
	} `json:"pool"`

	Session struct {
		Enabled    bool   `json:"enabled"`     // 会话粘性（默认 true）
		TTL        string `json:"ttl"`         // "30m"
		GCInterval string `json:"gc_interval"` // "5m"
	} `json:"session"`

	ReqLog struct {
		Enabled    bool   `json:"enabled"`     // 请求流水归档（默认 true）
		Dir        string `json:"dir"`         // "./data/logs"
		FileMaxMB  int    `json:"file_max_mb"` // 单文件上限（默认 16）
		RetainDays int    `json:"retain_days"` // 保留天数（默认 3）
	} `json:"reqlog"`

	Stream struct {
		// Thinking 思考内容的呈现方式（上游 SOLO 会先长时间思考再给正文）：
		//   "reasoning"（默认）→ 放进 delta.reasoning_content（DeepSeek 约定，
		//                       Cherry Studio / Chatbox / OpenWebUI 等会渲染成「思考过程」）
		//   "content"         → 包进  thinking…<｜end▁of▁thinking｜> 作为正文增量（空档期就有字在动，
		//                       与 trae-cn-relay 等中转的默认行为一致；只渲染 content
		//                       的客户端——Cline / Zed / 各类 CLI——能立刻看到输出）
		//   "none"            → 丢弃思考，只输出正文（最省 token，但空档期最长）
		Thinking string `json:"thinking"`
		// Heartbeat 空闲保活间隔（Go duration 字符串）。上游静默超过该时长就写
		// 一行 SSE 注释，避免客户端/中间代理判定卡死。"0" 或 "off" = 关闭。
		Heartbeat string `json:"heartbeat"`
	} `json:"stream"`

	Usage struct {
		Enabled        bool   `json:"enabled"`         // 按模型用量统计（默认 true）
		File           string `json:"file"`            // "./data/usage.json"
		CreditSampling string `json:"credit_sampling"` // 积分采样节流窗口，默认 "30s"；"0"=关闭
	} `json:"usage"`

	Schedule struct {
		CheckinHour  int   `json:"checkin_hour"`  // 9
		RefreshHours []int `json:"refresh_hours"` // [3]
		BalanceHours []int `json:"balance_hours"` // [8,12,18,23] 余额回写
		// Concurrency 定时任务对上游的并发上限。串行在几百个账号时会跑上十分钟，
		// 且无法被 SIGTERM 及时打断；并发 4 是「快」与「别把上游打成 429」的折中。
		Concurrency int `json:"concurrency"` // 4
	} `json:"schedule"`

	Upstream struct {
		TimeoutSeconds int `json:"timeout_seconds"` // 120
		// ModelErrorRules 模型级错误规则表。默认空 = 永不触发模型级冷却。
		// TRAE 上游是否存在模型级错误目前未知，抓到真实报文后只需在此加规则，无需改代码。
		ModelErrorRules []ModelErrorRuleConfig `json:"model_error_rules"`
	} `json:"upstream"`

	// 运行时诊断（不参与 JSON 解析）：被忽略的环境变量与被夹紧/修正的配置项。
	// main 启动摘要会打印数量，细节已逐条 log 过。
	IgnoredEnv []string `json:"-"`
	Warnings   []string `json:"-"`

	// 解析后的 duration。
	PlanCreditDur    time.Duration `json:"-"`
	SoftRateDur      time.Duration `json:"-"`
	ErrCooldownDur   time.Duration `json:"-"`
	SoftRateMaxDur   time.Duration `json:"-"`
	BreakerDur       time.Duration `json:"-"`
	BreakerMaxDur    time.Duration `json:"-"`
	DegradeWindowDur time.Duration `json:"-"`
	// CreditSamplingDur 积分采样节流窗口；0 = 关闭积分采样（token/缓存统计不受影响）。
	CreditSamplingDur time.Duration `json:"-"`
	// StreamHeartbeat 空闲保活间隔；0 = 关闭心跳。
	StreamHeartbeat   time.Duration `json:"-"`
	SessionTTL        time.Duration `json:"-"`
	SessionGCDuration time.Duration `json:"-"`
}

// 思考内容呈现方式。
const (
	StreamThinkingReasoning = "reasoning" // delta.reasoning_content（默认）
	StreamThinkingContent   = "content"   // 包进  thinking…<｜end▁of▁thinking｜> 作为正文
	StreamThinkingNone      = "none"      // 丢弃
)

// ModelErrorRuleConfig 配置层的模型级错误规则（转成 upstream.ModelErrorRule）。
type ModelErrorRuleConfig struct {
	Code     int      `json:"code"`     // 上游业务码，0 = 不按码匹配
	Status   int      `json:"status"`   // HTTP 状态码，0 = 不按状态匹配
	Contains []string `json:"contains"` // body 全部子串命中（大小写不敏感）
	Kind     string   `json:"kind"`     // rate_limit | model_unavailable | unknown_model
}

// Default 返回默认配置。
func Default() *Config {
	c := &Config{
		Listen:       ":8000",
		CallbackPort: "18080",
		CallbackBind: "0.0.0.0",
		APIKey:       "",
		AuthDir:      "./auths",
		StateFile:    "./data/state.json",
		DefaultModel: "glm-5.2",
	}
	c.Cooldown.PlanCredit = "12h"
	c.Cooldown.SoftRate = "60s"
	c.Cooldown.ErrThresh = 3
	c.Cooldown.ErrCooldown = "10m"

	c.Pool.MaxInFlight = 2
	c.Pool.SoftRateMax = "30m"
	c.Pool.BreakerThreshold = 3
	c.Pool.BreakerCooldown = "10m"
	c.Pool.BreakerCooldownMax = "1h"
	c.Pool.DegradeThreshold = 5
	c.Pool.DegradeWindow = "10m"
	c.Pool.CreditFloor = 0

	c.Session.Enabled = true
	c.Session.TTL = "30m"
	c.Session.GCInterval = "5m"

	c.ReqLog.Enabled = true
	c.ReqLog.Dir = "./data/logs"
	c.ReqLog.FileMaxMB = 16
	c.ReqLog.RetainDays = 3

	c.Stream.Thinking = StreamThinkingReasoning
	c.Stream.Heartbeat = "1s"

	c.Usage.Enabled = true
	c.Usage.File = "./data/usage.json"
	c.Usage.CreditSampling = "30s"

	c.Schedule.CheckinHour = 9
	c.Schedule.RefreshHours = []int{3}
	c.Schedule.BalanceHours = []int{8, 12, 18, 23}
	c.Schedule.Concurrency = 4
	c.Upstream.TimeoutSeconds = 120
	return c
}

// Load 从 path 读配置，再用 TW2A_* env 覆盖。path 为空或不存在时用默认 + env。
func Load(path string) (*Config, error) {
	c := Default()
	if path != "" {
		raw, err := os.ReadFile(path)
		if err != nil {
			if os.IsNotExist(err) {
				c = Default()
			} else {
				return nil, fmt.Errorf("read config: %w", err)
			}
		} else if err := json.Unmarshal(raw, c); err != nil {
			return nil, fmt.Errorf("parse config: %w", err)
		}
	}
	applyEnv(c)
	if err := c.normalize(); err != nil {
		return nil, err
	}
	return c, nil
}

// applyEnv 用 TW2A_* 环境变量覆盖配置。
//
// 解析失败一律不静默：记一条带「变量名 / 原始值 / 当前生效值」的日志，并计入
// c.IgnoredEnv，启动摘要会报数量。以前静默跳过是运维排障最大的坑——
// 比如 TW2A_MAX_IN_FLIGHT=abc 会被无声忽略，行为与预期完全不符。
func applyEnv(c *Config) {
	envStr := func(key string, dst *string) {
		if v := strings.TrimSpace(os.Getenv(key)); v != "" {
			*dst = v
		}
	}
	envInt := func(key string, dst *int) {
		raw, ok := os.LookupEnv(key)
		if !ok || strings.TrimSpace(raw) == "" {
			return
		}
		n, err := strconv.Atoi(strings.TrimSpace(raw))
		if err != nil {
			c.skipEnv(key, raw, strconv.Itoa(*dst))
			return
		}
		*dst = n
	}
	envInt64 := func(key string, dst *int64) {
		raw, ok := os.LookupEnv(key)
		if !ok || strings.TrimSpace(raw) == "" {
			return
		}
		n, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
		if err != nil {
			c.skipEnv(key, raw, strconv.FormatInt(*dst, 10))
			return
		}
		*dst = n
	}
	envBool := func(key string, dst *bool) {
		raw, ok := os.LookupEnv(key)
		if !ok || strings.TrimSpace(raw) == "" {
			return
		}
		switch strings.ToLower(strings.TrimSpace(raw)) {
		case "1", "true", "yes", "on":
			*dst = true
		case "0", "false", "no", "off":
			*dst = false
		default:
			c.skipEnv(key, raw, strconv.FormatBool(*dst))
		}
	}
	// envHours 解析 "3,15" 形式的整点列表；非法元素整条不生效并告警。
	envHours := func(key string, dst *[]int) {
		raw, ok := os.LookupEnv(key)
		if !ok || strings.TrimSpace(raw) == "" {
			return
		}
		var out []int
		for _, part := range strings.Split(raw, ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			n, err := strconv.Atoi(part)
			if err != nil {
				c.skipEnv(key, raw, fmt.Sprint(*dst))
				return
			}
			out = append(out, n)
		}
		if len(out) > 0 {
			*dst = out
		}
	}

	envStr("TW2A_API_KEY", &c.APIKey)
	envStr("TW2A_LISTEN", &c.Listen)
	envStr("TW2A_CALLBACK_PORT", &c.CallbackPort)
	envStr("TW2A_CALLBACK_BIND", &c.CallbackBind)
	envStr("TW2A_PUBLIC_BASE_URL", &c.PublicBaseURL)
	envStr("TW2A_AUTH_DIR", &c.AuthDir)
	envStr("TW2A_STATE_FILE", &c.StateFile)
	envStr("TW2A_DEFAULT_MODEL", &c.DefaultModel)
	envStr("TW2A_STREAM_THINKING", &c.Stream.Thinking)
	envStr("TW2A_STREAM_HEARTBEAT", &c.Stream.Heartbeat)

	// 冷却
	envStr("TW2A_PLAN_CREDIT", &c.Cooldown.PlanCredit)
	envStr("TW2A_SOFT_RATE", &c.Cooldown.SoftRate)
	envStr("TW2A_ERR_COOLDOWN", &c.Cooldown.ErrCooldown)
	// 旧键兼容：TW2A_ERR_THRESHOLD 同时设置熔断阈值（README 已注明）。
	// 只在解析成功时传播——解析失败时不能拿「当前生效值」去覆盖 json 里的
	// pool.breaker_threshold。显式的 TW2A_BREAKER_THRESHOLD 在下面仍会赢。
	if raw, ok := os.LookupEnv("TW2A_ERR_THRESHOLD"); ok && strings.TrimSpace(raw) != "" {
		if n, err := strconv.Atoi(strings.TrimSpace(raw)); err == nil {
			c.Cooldown.ErrThresh = n
			c.Pool.BreakerThreshold = n
		} else {
			c.skipEnv("TW2A_ERR_THRESHOLD", raw, strconv.Itoa(c.Cooldown.ErrThresh))
		}
	}

	// 账号池
	envInt("TW2A_MAX_IN_FLIGHT", &c.Pool.MaxInFlight)
	envStr("TW2A_SOFT_RATE_MAX", &c.Pool.SoftRateMax)
	envInt("TW2A_BREAKER_THRESHOLD", &c.Pool.BreakerThreshold)
	envStr("TW2A_BREAKER_COOLDOWN", &c.Pool.BreakerCooldown)
	envStr("TW2A_BREAKER_COOLDOWN_MAX", &c.Pool.BreakerCooldownMax)
	envInt("TW2A_DEGRADE_THRESHOLD", &c.Pool.DegradeThreshold)
	envStr("TW2A_DEGRADE_WINDOW", &c.Pool.DegradeWindow)
	envInt64("TW2A_CREDIT_FLOOR", &c.Pool.CreditFloor)

	// 用量
	envBool("TW2A_USAGE_ENABLED", &c.Usage.Enabled)
	envStr("TW2A_USAGE_FILE", &c.Usage.File)
	envStr("TW2A_CREDIT_SAMPLING", &c.Usage.CreditSampling)

	// 会话粘性
	envBool("TW2A_SESSION_ENABLED", &c.Session.Enabled)
	envStr("TW2A_SESSION_TTL", &c.Session.TTL)
	envStr("TW2A_SESSION_GC_INTERVAL", &c.Session.GCInterval)

	// 请求流水
	envBool("TW2A_REQLOG_ENABLED", &c.ReqLog.Enabled)
	envStr("TW2A_REQLOG_DIR", &c.ReqLog.Dir)
	envInt("TW2A_REQLOG_FILE_MAX_MB", &c.ReqLog.FileMaxMB)
	envInt("TW2A_REQLOG_RETAIN_DAYS", &c.ReqLog.RetainDays)

	// 定时任务
	envInt("TW2A_CHECKIN_HOUR", &c.Schedule.CheckinHour)
	envHours("TW2A_SCHEDULE_REFRESH_HOURS", &c.Schedule.RefreshHours)
	envHours("TW2A_SCHEDULE_BALANCE_HOURS", &c.Schedule.BalanceHours)
	envInt("TW2A_SCHEDULE_CONCURRENCY", &c.Schedule.Concurrency)

	// 上游
	envInt("TW2A_TIMEOUT_SECONDS", &c.Upstream.TimeoutSeconds)

	// CORS：逗号分隔来源列表，例如 "https://a.com,https://b.com"。
	if raw, ok := os.LookupEnv("TW2A_CORS_ORIGINS"); ok && strings.TrimSpace(raw) != "" {
		var out []string
		for _, part := range strings.Split(raw, ",") {
			if part = strings.TrimSpace(part); part != "" {
				out = append(out, part)
			}
		}
		c.CORSOrigins = out
	}
}

// skipEnv 记录一个被忽略的环境变量（值非法），并立刻打日志。
func (c *Config) skipEnv(key, raw, effective string) {
	raw = strings.TrimSpace(raw)
	if len(raw) > 40 {
		raw = raw[:40] + "…"
	}
	log.Printf("[config] 忽略非法环境变量 %s=%q（保持当前生效值 %s）", key, raw, effective)
	c.IgnoredEnv = append(c.IgnoredEnv, key)
}

// warnf 记录一条归一化阶段的修正（夹紧/回落），并打日志。
func (c *Config) warnf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	log.Printf("[config] %s", msg)
	c.Warnings = append(c.Warnings, msg)
}

func (c *Config) normalize() error {
	var err error
	// parseDur 解析 duration 并夹紧到 (0, max]。<=0 表示「关掉」的字段由调用方
	// 单独传 allowZero。
	parseDur := func(name, raw string, max time.Duration) (time.Duration, error) {
		d, derr := time.ParseDuration(strings.TrimSpace(raw))
		if derr != nil {
			return 0, fmt.Errorf("%s: %w", name, derr)
		}
		if d < 0 {
			return 0, fmt.Errorf("%s: 不能为负（收到 %s）", name, raw)
		}
		if d == 0 {
			return 0, fmt.Errorf("%s: 不能为 0（收到 %s）", name, raw)
		}
		if d > max {
			c.warnf("%s=%s 超过上限 %s，已夹紧", name, raw, max)
			d = max
		}
		return d, nil
	}
	const durMax = 24 * time.Hour

	if c.PlanCreditDur, err = parseDur("cooldown.plan_credit", c.Cooldown.PlanCredit, durMax); err != nil {
		return err
	}
	if c.SoftRateDur, err = parseDur("cooldown.soft_rate", c.Cooldown.SoftRate, durMax); err != nil {
		return err
	}
	if c.ErrCooldownDur, err = parseDur("cooldown.err_cooldown", c.Cooldown.ErrCooldown, durMax); err != nil {
		return err
	}
	if c.SoftRateMaxDur, err = parseDur("pool.soft_rate_max", c.Pool.SoftRateMax, durMax); err != nil {
		return err
	}
	if c.BreakerDur, err = parseDur("pool.breaker_cooldown", c.Pool.BreakerCooldown, durMax); err != nil {
		return err
	}
	if c.BreakerMaxDur, err = parseDur("pool.breaker_cooldown_max", c.Pool.BreakerCooldownMax, durMax); err != nil {
		return err
	}
	if c.DegradeWindowDur, err = parseDur("pool.degrade_window", c.Pool.DegradeWindow, durMax); err != nil {
		return err
	}
	// 积分采样窗口：空 → 30s；"0"/"0s" → 关闭（0），不报错。
	c.Usage.CreditSampling = strings.TrimSpace(c.Usage.CreditSampling)
	if c.Usage.CreditSampling == "" {
		c.Usage.CreditSampling = "30s"
	}
	if c.CreditSamplingDur, err = time.ParseDuration(c.Usage.CreditSampling); err != nil {
		return fmt.Errorf("usage.credit_sampling: %w", err)
	}
	if c.CreditSamplingDur < 0 {
		return fmt.Errorf("usage.credit_sampling: 不能为负")
	}
	if c.CreditSamplingDur > durMax {
		c.warnf("usage.credit_sampling=%s 超过上限 %s，已夹紧", c.Usage.CreditSampling, durMax)
		c.CreditSamplingDur = durMax
	}
	c.Stream.Thinking = strings.ToLower(strings.TrimSpace(c.Stream.Thinking))
	switch c.Stream.Thinking {
	case "":
		c.Stream.Thinking = StreamThinkingReasoning
	case StreamThinkingReasoning, StreamThinkingContent, StreamThinkingNone:
	default:
		return fmt.Errorf("stream.thinking: 只能是 %q / %q / %q，收到 %q",
			StreamThinkingReasoning, StreamThinkingContent, StreamThinkingNone, c.Stream.Thinking)
	}
	hb := strings.ToLower(strings.TrimSpace(c.Stream.Heartbeat))
	switch hb {
	case "", "off":
		c.StreamHeartbeat = 0
	default:
		d, derr := time.ParseDuration(hb)
		if derr != nil {
			return fmt.Errorf("stream.heartbeat: %w", derr)
		}
		if d < 0 {
			return fmt.Errorf("stream.heartbeat: 不能为负")
		}
		if d > durMax {
			c.warnf("stream.heartbeat=%s 超过上限 %s，已夹紧", hb, durMax)
			d = durMax
		}
		c.StreamHeartbeat = d
	}
	if c.Usage.File == "" {
		c.Usage.File = "./data/usage.json"
	}
	if c.SessionTTL, err = parseDur("session.ttl", c.Session.TTL, durMax); err != nil {
		return err
	}
	if c.SessionGCDuration, err = parseDur("session.gc_interval", c.Session.GCInterval, durMax); err != nil {
		return err
	}
	// 并发/重试类整数：合法区间 [1, 64]。0 视为「未配置」→ 回落默认；
	// 负数/超上限 → 夹紧并告警（超上限的并发会把上游打爆）。
	clampInt := func(name string, v *int, def, min, max int) {
		switch {
		case *v == 0:
			*v = def
		case *v < min:
			c.warnf("%s=%d 低于下限 %d，已夹紧", name, *v, min)
			*v = min
		case *v > max:
			c.warnf("%s=%d 超过上限 %d，已夹紧", name, *v, max)
			*v = max
		}
	}
	clampInt("cooldown.err_threshold", &c.Cooldown.ErrThresh, 3, 1, 64)
	clampInt("pool.breaker_threshold", &c.Pool.BreakerThreshold, 3, 1, 64)
	clampInt("pool.degrade_threshold", &c.Pool.DegradeThreshold, 5, 1, 64)
	// MaxInFlight 的 0 语义是「不限」，单独处理（上限仍夹紧）。
	if c.Pool.MaxInFlight < 0 {
		c.warnf("pool.max_in_flight=%d 为负，已按 0（不限）处理", c.Pool.MaxInFlight)
		c.Pool.MaxInFlight = 0
	} else if c.Pool.MaxInFlight > 64 {
		c.warnf("pool.max_in_flight=%d 超过上限 64，已夹紧", c.Pool.MaxInFlight)
		c.Pool.MaxInFlight = 64
	}
	if c.Pool.CreditFloor < 0 {
		c.warnf("pool.credit_floor=%d 为负，已按 0（关闭）处理", c.Pool.CreditFloor)
		c.Pool.CreditFloor = 0
	}
	if c.BreakerMaxDur < c.BreakerDur {
		c.warnf("pool.breaker_cooldown_max(%s) < breaker_cooldown(%s)，已提升到后者",
			c.BreakerMaxDur, c.BreakerDur)
		c.BreakerMaxDur = c.BreakerDur
	}
	if c.SoftRateMaxDur < c.SoftRateDur {
		c.warnf("pool.soft_rate_max(%s) < cooldown.soft_rate(%s)，已提升到后者",
			c.SoftRateMaxDur, c.SoftRateDur)
		c.SoftRateMaxDur = c.SoftRateDur
	}
	if c.BreakerDur <= 0 {
		c.BreakerDur = c.ErrCooldownDur
	}
	if c.Upstream.TimeoutSeconds <= 0 {
		c.warnf("upstream.timeout_seconds=%d 非法，已回落 120", c.Upstream.TimeoutSeconds)
		c.Upstream.TimeoutSeconds = 120
	}
	if c.Upstream.TimeoutSeconds > 3600 {
		c.warnf("upstream.timeout_seconds=%d 超过上限 3600，已夹紧", c.Upstream.TimeoutSeconds)
		c.Upstream.TimeoutSeconds = 3600
	}
	if c.DefaultModel == "" {
		c.DefaultModel = "glm-5.2"
	}
	if c.Listen == "" {
		c.Listen = ":8000"
	}
	if !strings.HasPrefix(c.Listen, ":") && !strings.Contains(c.Listen, ":") {
		c.Listen = ":" + c.Listen
	}
	// CallbackPort：空/未设 → 默认 18080；显式 "0" → 不起回调 server（纯手动粘贴模式）
	if c.CallbackPort = strings.TrimSpace(c.CallbackPort); c.CallbackPort == "" {
		c.CallbackPort = "18080"
	}
	if c.CallbackPort != "0" {
		if n, perr := strconv.Atoi(c.CallbackPort); perr != nil || n <= 0 || n > 65535 {
			return fmt.Errorf("callback_port: 需要 1..65535 的端口号或 \"0\"（关闭），收到 %q", c.CallbackPort)
		}
	}
	// CallbackBind：空 → 0.0.0.0（容器内必须绑通配地址，否则宿主/反代进不来）。
	// 注意 127.0.0.1 只在「同机本地调试」时可用，docker 里等于回调不可达。
	if c.CallbackBind = strings.TrimSpace(c.CallbackBind); c.CallbackBind == "" {
		c.CallbackBind = "0.0.0.0"
	}
	// PublicBaseURL：去掉结尾斜杠，必须是 http(s):// 开头，否则回调链接不可点。
	if u := strings.TrimSpace(c.PublicBaseURL); u != "" {
		u = strings.TrimRight(u, "/")
		if !strings.HasPrefix(u, "http://") && !strings.HasPrefix(u, "https://") {
			return fmt.Errorf("public_base_url: 需要以 http:// 或 https:// 开头，收到 %q", c.PublicBaseURL)
		}
		c.PublicBaseURL = u
	}
	// CORS：去空白、去空项；显式写 "*" 等价于不限制。
	if cleaned := make([]string, 0, len(c.CORSOrigins)); len(c.CORSOrigins) > 0 {
		for _, o := range c.CORSOrigins {
			if o = strings.TrimSpace(o); o != "" {
				cleaned = append(cleaned, o)
			}
		}
		c.CORSOrigins = cleaned
	}
	if c.ReqLog.Dir == "" {
		c.ReqLog.Dir = "./data/logs"
	}
	if c.ReqLog.FileMaxMB <= 0 {
		c.warnf("reqlog.file_max_mb=%d 非法，已回落 16", c.ReqLog.FileMaxMB)
		c.ReqLog.FileMaxMB = 16
	}
	if c.ReqLog.FileMaxMB > 4096 {
		c.warnf("reqlog.file_max_mb=%d 超过上限 4096，已夹紧", c.ReqLog.FileMaxMB)
		c.ReqLog.FileMaxMB = 4096
	}
	if c.ReqLog.RetainDays <= 0 {
		c.warnf("reqlog.retain_days=%d 非法，已回落 3", c.ReqLog.RetainDays)
		c.ReqLog.RetainDays = 3
	}
	if c.ReqLog.RetainDays > 365 {
		c.warnf("reqlog.retain_days=%d 超过上限 365，已夹紧", c.ReqLog.RetainDays)
		c.ReqLog.RetainDays = 365
	}
	// 定时任务整点：签到 0..23；刷新/回写小时表非空、元素 0..23、去重升序。
	if c.Schedule.CheckinHour < 0 || c.Schedule.CheckinHour > 23 {
		c.warnf("schedule.checkin_hour=%d 越界（0..23），已回落 9", c.Schedule.CheckinHour)
		c.Schedule.CheckinHour = 9
	}
	c.Schedule.RefreshHours = normalizeHours(c, "schedule.refresh_hours", c.Schedule.RefreshHours, []int{3})
	c.Schedule.BalanceHours = normalizeHours(c, "schedule.balance_hours", c.Schedule.BalanceHours, []int{8, 12, 18, 23})
	// 定时任务并发：0 视为未配置（回落 4），上限 32——再多只会把上游打成 429，
	// 签到/查额度这类调用没有「越快越好」的收益。
	if c.Schedule.Concurrency <= 0 {
		c.Schedule.Concurrency = 4
	}
	if c.Schedule.Concurrency > 32 {
		c.warnf("schedule.concurrency=%d 超过上限 32，已夹紧", c.Schedule.Concurrency)
		c.Schedule.Concurrency = 32
	}
	// 模型级错误规则校验：kind 必须非空——没有 kind 的规则无法归类到冷却类型，
	// 静默放过会变成「命中即无效」，比不配更危险，所以直接报错。
	for i := range c.Upstream.ModelErrorRules {
		r := &c.Upstream.ModelErrorRules[i]
		r.Kind = strings.TrimSpace(r.Kind)
		if r.Kind == "" {
			return fmt.Errorf("upstream.model_error_rules[%d]: kind 不能为空（rate_limit | model_unavailable | unknown_model）", i)
		}
		if r.Code == 0 && r.Status == 0 && len(r.Contains) == 0 {
			return fmt.Errorf("upstream.model_error_rules[%d]: 至少需要 code/status/contains 之一", i)
		}
	}
	return nil
}

// normalizeHours 清洗整点列表：剔除越界项、去重、升序；清洗后为空则回落 def。
func normalizeHours(c *Config, name string, in, def []int) []int {
	if len(in) == 0 {
		return append([]int(nil), def...)
	}
	seen := make(map[int]bool, len(in))
	out := make([]int, 0, len(in))
	for _, h := range in {
		if h < 0 || h > 23 {
			c.warnf("%s 含越界整点 %d（合法 0..23），已剔除", name, h)
			continue
		}
		if seen[h] {
			continue
		}
		seen[h] = true
		out = append(out, h)
	}
	sort.Ints(out)
	if len(out) == 0 {
		c.warnf("%s 清洗后为空，已回落默认 %v", name, def)
		return append([]int(nil), def...)
	}
	return out
}

// modelErrorRules 转换为 upstream 规则表；空时返回 nil（= 永不触发模型级分类）。
func (c *Config) modelErrorRules() []upstream.ModelErrorRule {
	if len(c.Upstream.ModelErrorRules) == 0 {
		return nil
	}
	out := make([]upstream.ModelErrorRule, 0, len(c.Upstream.ModelErrorRules))
	for _, r := range c.Upstream.ModelErrorRules {
		out = append(out, upstream.ModelErrorRule{
			Code: r.Code, Status: r.Status, Contains: r.Contains, Kind: r.Kind,
		})
	}
	return out
}
