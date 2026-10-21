// config.go 加载 JSON 配置 + TW2A_* 环境变量覆盖。
// APIKey 只从环境变量 TW2A_API_KEY 读取（key 走 env，不落盘 git）。
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"trae2api-web/internal/upstream"
)

// Config 顶层配置。
type Config struct {
	Listen       string `json:"listen"`        // ":8000"
	CallbackPort string `json:"callback_port"` // "18080"（TRAE 登录回调监听端口，0 = 不起）
	APIKey       string `json:"-"`             // 只读 env TW2A_API_KEY（不读 json）
	AuthDir      string `json:"auth_dir"`      // "./auths"
	StateFile    string `json:"state_file"`    // "./data/state.json"
	DefaultModel string `json:"default_model"` // "glm-5.2"

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
		RetainDays int    `json:"retain_days"` // 保留天数（默认 7）
	} `json:"reqlog"`

	Usage struct {
		Enabled        bool   `json:"enabled"`         // 按模型用量统计（默认 true）
		File           string `json:"file"`            // "./data/usage.json"
		CreditSampling string `json:"credit_sampling"` // 积分采样节流窗口，默认 "30s"；"0"=关闭
	} `json:"usage"`

	Schedule struct {
		CheckinHour  int   `json:"checkin_hour"`  // 9
		RefreshHours []int `json:"refresh_hours"` // [3]
		BalanceHours []int `json:"balance_hours"` // [8,12,18,23] 余额回写
	} `json:"schedule"`

	Upstream struct {
		TimeoutSeconds int `json:"timeout_seconds"` // 120
		// ModelErrorRules 模型级错误规则表。默认空 = 永不触发模型级冷却。
		// TRAE 上游是否存在模型级错误目前未知，抓到真实报文后只需在此加规则，无需改代码。
		ModelErrorRules []ModelErrorRuleConfig `json:"model_error_rules"`
	} `json:"upstream"`

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
	SessionTTL        time.Duration `json:"-"`
	SessionGCDuration time.Duration `json:"-"`
}

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
	c.ReqLog.RetainDays = 7

	c.Usage.Enabled = true
	c.Usage.File = "./data/usage.json"
	c.Usage.CreditSampling = "30s"

	c.Schedule.CheckinHour = 9
	c.Schedule.RefreshHours = []int{3}
	c.Schedule.BalanceHours = []int{8, 12, 18, 23}
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

func applyEnv(c *Config) {
	if v := os.Getenv("TW2A_API_KEY"); v != "" {
		c.APIKey = v
	}
	if v := os.Getenv("TW2A_LISTEN"); v != "" {
		c.Listen = v
	}
	if v := os.Getenv("TW2A_CALLBACK_PORT"); v != "" {
		c.CallbackPort = v
	}
	if v := os.Getenv("TW2A_AUTH_DIR"); v != "" {
		c.AuthDir = v
	}
	if v := os.Getenv("TW2A_STATE_FILE"); v != "" {
		c.StateFile = v
	}
	if v := os.Getenv("TW2A_DEFAULT_MODEL"); v != "" {
		c.DefaultModel = v
	}
	if v := os.Getenv("TW2A_PLAN_CREDIT"); v != "" {
		c.Cooldown.PlanCredit = v
	}
	if v := os.Getenv("TW2A_SOFT_RATE"); v != "" {
		c.Cooldown.SoftRate = v
	}
	if v := os.Getenv("TW2A_ERR_THRESHOLD"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			c.Cooldown.ErrThresh = n
			c.Pool.BreakerThreshold = n
		}
	}
	if v := os.Getenv("TW2A_ERR_COOLDOWN"); v != "" {
		c.Cooldown.ErrCooldown = v
	}
	if v := os.Getenv("TW2A_SOFT_RATE_MAX"); v != "" {
		c.Pool.SoftRateMax = v
	}
	if v := os.Getenv("TW2A_MAX_IN_FLIGHT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			c.Pool.MaxInFlight = n
		}
	}
	if v := os.Getenv("TW2A_BREAKER_THRESHOLD"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			c.Pool.BreakerThreshold = n
		}
	}
	if v := os.Getenv("TW2A_BREAKER_COOLDOWN"); v != "" {
		c.Pool.BreakerCooldown = v
	}
	if v := os.Getenv("TW2A_CREDIT_FLOOR"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			c.Pool.CreditFloor = n
		}
	}
	if v := os.Getenv("TW2A_USAGE_ENABLED"); v != "" {
		c.Usage.Enabled = v == "1" || strings.EqualFold(v, "true")
	}
	if v := os.Getenv("TW2A_USAGE_FILE"); v != "" {
		c.Usage.File = v
	}
	if v := os.Getenv("TW2A_CREDIT_SAMPLING"); v != "" {
		c.Usage.CreditSampling = v
	}
	if v := os.Getenv("TW2A_SESSION_ENABLED"); v != "" {
		c.Session.Enabled = v == "1" || strings.EqualFold(v, "true")
	}
	if v := os.Getenv("TW2A_SESSION_TTL"); v != "" {
		c.Session.TTL = v
	}
	if v := os.Getenv("TW2A_REQLOG_ENABLED"); v != "" {
		c.ReqLog.Enabled = v == "1" || strings.EqualFold(v, "true")
	}
	if v := os.Getenv("TW2A_REQLOG_DIR"); v != "" {
		c.ReqLog.Dir = v
	}
	if v := os.Getenv("TW2A_REQLOG_RETAIN_DAYS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			c.ReqLog.RetainDays = n
		}
	}
	if v := os.Getenv("TW2A_CHECKIN_HOUR"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			c.Schedule.CheckinHour = n
		}
	}
	if v := os.Getenv("TW2A_TIMEOUT_SECONDS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			c.Upstream.TimeoutSeconds = n
		}
	}
}

func (c *Config) normalize() error {
	var err error
	if c.PlanCreditDur, err = time.ParseDuration(c.Cooldown.PlanCredit); err != nil {
		return fmt.Errorf("cooldown.plan_credit: %w", err)
	}
	if c.SoftRateDur, err = time.ParseDuration(c.Cooldown.SoftRate); err != nil {
		return fmt.Errorf("cooldown.soft_rate: %w", err)
	}
	if c.ErrCooldownDur, err = time.ParseDuration(c.Cooldown.ErrCooldown); err != nil {
		return fmt.Errorf("cooldown.err_cooldown: %w", err)
	}
	if c.SoftRateMaxDur, err = time.ParseDuration(c.Pool.SoftRateMax); err != nil {
		return fmt.Errorf("pool.soft_rate_max: %w", err)
	}
	if c.BreakerDur, err = time.ParseDuration(c.Pool.BreakerCooldown); err != nil {
		return fmt.Errorf("pool.breaker_cooldown: %w", err)
	}
	if c.BreakerMaxDur, err = time.ParseDuration(c.Pool.BreakerCooldownMax); err != nil {
		return fmt.Errorf("pool.breaker_cooldown_max: %w", err)
	}
	if c.DegradeWindowDur, err = time.ParseDuration(c.Pool.DegradeWindow); err != nil {
		return fmt.Errorf("pool.degrade_window: %w", err)
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
	if c.Usage.File == "" {
		c.Usage.File = "./data/usage.json"
	}
	if c.SessionTTL, err = time.ParseDuration(c.Session.TTL); err != nil {
		return fmt.Errorf("session.ttl: %w", err)
	}
	if c.SessionGCDuration, err = time.ParseDuration(c.Session.GCInterval); err != nil {
		return fmt.Errorf("session.gc_interval: %w", err)
	}
	if c.Cooldown.ErrThresh <= 0 {
		c.Cooldown.ErrThresh = 3
	}
	if c.Pool.BreakerThreshold <= 0 {
		c.Pool.BreakerThreshold = 3
	}
	if c.BreakerMaxDur < c.BreakerDur {
		c.BreakerMaxDur = c.BreakerDur
	}
	if c.SoftRateMaxDur < c.SoftRateDur {
		c.SoftRateMaxDur = c.SoftRateDur
	}
	if c.ErrCooldownDur <= 0 {
		c.ErrCooldownDur = 10 * time.Minute
	}
	if c.BreakerDur <= 0 {
		c.BreakerDur = c.ErrCooldownDur
	}
	if c.Upstream.TimeoutSeconds <= 0 {
		c.Upstream.TimeoutSeconds = 120
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
	if c.CallbackPort == "" {
		c.CallbackPort = "18080"
	}
	if c.ReqLog.Dir == "" {
		c.ReqLog.Dir = "./data/logs"
	}
	if c.ReqLog.FileMaxMB <= 0 {
		c.ReqLog.FileMaxMB = 16
	}
	if c.ReqLog.RetainDays <= 0 {
		c.ReqLog.RetainDays = 7
	}
	// 模型级错误规则校验：kind 必须非空，否则该条无效（宁可不触发也不要误判）。
	for i := range c.Upstream.ModelErrorRules {
		r := &c.Upstream.ModelErrorRules[i]
		r.Kind = strings.TrimSpace(r.Kind)
		if r.Code == 0 && r.Status == 0 && len(r.Contains) == 0 {
			return fmt.Errorf("upstream.model_error_rules[%d]: 至少需要 code/status/contains 之一", i)
		}
	}
	return nil
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
