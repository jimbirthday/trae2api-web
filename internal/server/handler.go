// Package server 暴露 OpenAI 兼容 HTTP 接口，内部驱动 pool 挑号 + upstream 转发。
package server

import (
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"trae2api-web/internal/pool"
	"trae2api-web/internal/reqlog"
	"trae2api-web/internal/session"
	"trae2api-web/internal/upstream"
	"trae2api-web/internal/usage"
)

// Config handler 依赖。
type Config struct {
	Pool         *pool.Pool
	Upstream     *upstream.Client
	APIKey       string        // 空 = 不鉴权
	AuthDir      string        // auths/ 目录，用于 import/delete 落盘 trae-*.json
	MaxRotate    int           // 单请求最多换号次数，默认 3
	PlanCooldown time.Duration // 1005 冷却，默认 12h
	SoftCooldown time.Duration // 429 冷却，默认 60s
	ErrThreshold int           // 连续错误阈值，默认 3
	ErrCooldown  time.Duration // 错误冷却，默认 10m
	RefreshSkew  time.Duration // token 预刷新窗口，默认 24h
	DefaultModel string        // 默认 glm-5.2
	// StreamThinking 思考内容呈现方式（上游会先思考几十秒才给正文，
	// 这里决定这段时间给客户端看什么）。零值 = reasoning_content。
	StreamThinking upstream.ThinkingMode
	// StreamHeartbeat 空闲保活间隔；0 = 关闭心跳。
	StreamHeartbeat time.Duration

	// Session 会话粘性路由（nil = 关闭粘性，每次请求独立选号）。
	Session *session.Router
	// RequestLog 请求流水记录器（nil = 不记录）。
	RequestLog *reqlog.Recorder
	// Usage 用量统计（按模型累计 token/缓存/积分采样；nil = 不统计）。
	Usage *usage.Tracker
	// CreditSampler 积分采样器（nil = 不采样积分，则「积分/百万」显示为空）。
	CreditSampler *usage.Sampler
	// ConfigPath config.json 路径（面板保存配置用；空 = 禁用写回）。
	ConfigPath string
}

// maxBodyBytes 请求体大小上限（8MB），超过返回 413。
const maxBodyBytes = 8 << 20

// Handler 主路由。
type Handler struct {
	cfg Config
	mux *http.ServeMux

	// credits 额度快照缓存（见 admin.go）。放在 Handler 上而不是全局变量，
	// 这样多个 handler 实例（测试）之间互不干扰。
	credits creditsCache

	// Web 登录 pending 态：pendingID → 登录进行中的临时上下文。
	// 回调 /authorize 捕获后标记成功；面板轮询 result 取结果。
	loginMu sync.Mutex
	logins  map[string]*pendingLogin
}

// NewHandler 构建 handler。
func NewHandler(cfg Config) *Handler {
	if cfg.MaxRotate <= 0 {
		cfg.MaxRotate = 3
	}
	if cfg.PlanCooldown <= 0 {
		cfg.PlanCooldown = 12 * time.Hour
	}
	if cfg.SoftCooldown <= 0 {
		cfg.SoftCooldown = 60 * time.Second
	}
	if cfg.ErrThreshold <= 0 {
		cfg.ErrThreshold = 3
	}
	if cfg.ErrCooldown <= 0 {
		cfg.ErrCooldown = 10 * time.Minute
	}
	if cfg.RefreshSkew <= 0 {
		cfg.RefreshSkew = 24 * time.Hour
	}
	if cfg.DefaultModel == "" {
		cfg.DefaultModel = upstream.DefaultConfigName
	}
	h := &Handler{cfg: cfg, mux: http.NewServeMux(), logins: map[string]*pendingLogin{}}
	h.mux.HandleFunc("POST /v1/chat/completions", h.withAuth(h.chatCompletions))
	h.mux.HandleFunc("GET /v1/models", h.withAuth(h.models))
	h.mux.HandleFunc("GET /status", h.withAuth(h.status))
	h.mux.HandleFunc("GET /healthz", h.healthz)
	// 管理面板：本地面板
	// 读接口无鉴权（局域网内只读）；写接口（accounts 写/login/refresh/authorize）
	// 经 withAdminAuth 校验 Bearer = TW2A_API_KEY（见 §4 安全设计）。
	h.mux.HandleFunc("GET /admin", h.adminPageGated)
	h.mux.HandleFunc("GET /admin/api/credits", h.adminReadAuth(h.adminCredits))
	// 账号 CRUD
	h.mux.HandleFunc("GET /admin/api/accounts", h.adminReadAuth(h.adminAccounts))
	h.mux.HandleFunc("POST /admin/api/accounts/import", h.withAdminAuth(h.adminImportAccount))
	h.mux.HandleFunc("DELETE /admin/api/accounts/{uid}", h.withAdminAuth(h.adminDeleteAccount))
	h.mux.HandleFunc("PATCH /admin/api/accounts/{uid}", h.withAdminAuth(h.adminPatchAccount))
	h.mux.HandleFunc("POST /admin/api/accounts/{uid}/refresh", h.withAdminAuth(h.adminRefreshAccount))
	h.mux.HandleFunc("GET /admin/api/accounts/{uid}/json", h.adminReadAuth(h.adminAccountJSON))
	h.mux.HandleFunc("POST /admin/api/accounts/{uid}/pause", h.withAdminAuth(h.adminPauseAccount))
	h.mux.HandleFunc("POST /admin/api/accounts/{uid}/resume", h.withAdminAuth(h.adminResumeAccount))
	h.mux.HandleFunc("POST /admin/api/accounts/{uid}/revive", h.withAdminAuth(h.adminReviveAccount))
	h.mux.HandleFunc("POST /admin/api/accounts/{uid}/profile", h.withAdminAuth(h.adminAccountProfile))
	h.mux.HandleFunc("POST /admin/api/accounts/{uid}/balance", h.withAdminAuth(h.adminAccountBalance))
	h.mux.HandleFunc("POST /admin/api/accounts/set_all", h.withAdminAuth(h.adminAccountsSetAll))
	h.mux.HandleFunc("GET /admin/api/accounts/export", h.withAdminAuth(h.adminExportAccounts))
	h.mux.HandleFunc("POST /admin/api/accounts/{uid}/test", h.withAdminAuth(h.adminAccountTest))
	h.mux.HandleFunc("POST /admin/api/checkin_all", h.withAdminAuth(h.adminCheckinAll))
	h.mux.HandleFunc("GET /admin/api/requests", h.adminReadAuth(h.adminRequests))
	h.mux.HandleFunc("GET /admin/api/metrics", h.adminReadAuth(h.adminMetrics))
	h.mux.HandleFunc("GET /admin/api/config", h.adminReadAuth(h.adminGetConfig))
	h.mux.HandleFunc("POST /admin/api/config", h.withAdminAuth(h.adminSaveConfig))
	// 面板会话：POST /admin/api/session 用 key 换 cookie（登录），DELETE 退出。
	h.mux.HandleFunc("POST /admin/api/session", h.adminSession)
	h.mux.HandleFunc("DELETE /admin/api/session", h.adminLogout)
	// 用量统计（按模型 token/缓存/积分）
	h.mux.HandleFunc("GET /admin/api/usage", h.adminReadAuth(h.adminUsage))
	// Web 登录闭环
	h.mux.HandleFunc("POST /admin/api/login", h.withAdminAuth(h.adminLoginStart))
	h.mux.HandleFunc("GET /admin/api/login/result", h.adminReadAuth(h.adminLoginResult))
	h.mux.HandleFunc("POST /admin/api/login/cancel", h.withAdminAuth(h.adminLoginCancel))
	// TRAE 回调落点（/authorize）：无需 Bearer（TRAE 浏览器 302 不带 key），
	// 仅捕获 query 写 pending 队列，不直接落盘 token。
	h.mux.HandleFunc("GET /authorize", h.authorizeCallback)
	return h
}

// adminReadAuth 校验面板**读**接口的访问权（Bearer 或登录 cookie）。
// 与 withAdminAuth 的区别：除 Bearer 外还接受浏览器 cookie 会话，
// 这样浏览器登录一次后所有列表/流水/统计接口都能直接用。
func (h *Handler) adminReadAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if h.adminAuthorized(r) {
			next(w, r)
			return
		}
		writeOpenAIError(w, http.StatusUnauthorized, "invalid_api_key", "需要 API Key 才能访问控制台数据")
	}
}

// withAdminAuth 校验写操作的 Bearer API Key（常量时间比较，复用 withAuth 逻辑）。
// APIKey 为空时（未配置 TW2A_API_KEY）退化为不鉴权——本地无 key 场景仍可用，
// 但生产强烈建议配 key（见 PLAN §4）。
func (h *Handler) withAdminAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if h.cfg.APIKey == "" {
			next(w, r)
			return
		}
		authz := r.Header.Get("Authorization")
		const prefix = "Bearer "
		if len(authz) < len(prefix) || !strings.EqualFold(authz[:len(prefix)], prefix) {
			writeOpenAIError(w, http.StatusUnauthorized, "invalid_api_key", "missing or invalid API key")
			return
		}
		key := authz[len(prefix):]
		if subtle.ConstantTimeCompare([]byte(key), []byte(h.cfg.APIKey)) != 1 {
			writeOpenAIError(w, http.StatusUnauthorized, "invalid_api_key", "missing or invalid API key")
			return
		}
		next(w, r)
	}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
}

func (h *Handler) withAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if h.cfg.APIKey != "" {
			authz := r.Header.Get("Authorization")
			const prefix = "Bearer "
			if len(authz) < len(prefix) || !strings.EqualFold(authz[:len(prefix)], prefix) {
				writeOpenAIError(w, http.StatusUnauthorized, "invalid_api_key", "missing or invalid API key")
				return
			}
			key := authz[len(prefix):]
			// 常量时间比较，防时序攻击（本地代理但按规范）。
			if subtle.ConstantTimeCompare([]byte(key), []byte(h.cfg.APIKey)) != 1 {
				writeOpenAIError(w, http.StatusUnauthorized, "invalid_api_key", "missing or invalid API key")
				return
			}
		}
		next(w, r)
	}
}

func (h *Handler) healthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (h *Handler) status(w http.ResponseWriter, r *http.Request) {
	counts := h.cfg.Pool.Counts()
	sessionCount := 0
	if h.cfg.Session != nil {
		sessionCount = h.cfg.Session.Count()
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"accounts":         h.cfg.Pool.List(),
		"counts":           counts,
		"sticky_sessions":  sessionCount,
		"max_rotate":       h.cfg.MaxRotate,
		"default_model":    h.cfg.DefaultModel,
		"refresh_skew_sec": int64(h.cfg.RefreshSkew / time.Second),
	})
}

// ---------------------------------------------------------------------------
// 模型映射
// ---------------------------------------------------------------------------

// mapModel 将客户端传入的 model 映射为 config_name（SPEC §4.5）：
//
//	"glm-5.2"（config_name）        → 直接转发
//	"glm-5.2__dev"（内部名）        → 去掉后缀映射回 config_name
//	"auto" / ""                     → 默认模型
//	其他未知                        → 400
func (h *Handler) mapModel(model string) (string, error) {
	model = strings.TrimSpace(model)
	if model == "" || model == "auto" {
		return h.cfg.DefaultModel, nil
	}
	// 去掉内部名后缀（__dev / __max 等）
	base := model
	if i := strings.Index(model, "__"); i >= 0 {
		base = model[:i]
	}
	if h.knownModel(base) {
		return base, nil
	}
	// 宽松匹配：下划线 → 横线，大小写不敏感（deepseek_v4_pro → DeepSeek-V4-Pro）
	norm := normalizeModelName(base)
	if h.knownModel(norm) {
		return norm, nil
	}
	return "", fmt.Errorf("unknown model %q", model)
}

// normalizeModelName 将下划线命名的内部名归一化为 config_name 风格（横线分隔）。
func normalizeModelName(s string) string {
	parts := strings.Split(s, "_")
	for i, p := range parts {
		if p == "" {
			continue
		}
		parts[i] = strings.ToUpper(p[:1]) + strings.ToLower(p[1:])
	}
	return strings.Join(parts, "-")
}

// knownModel 判断 model 是否在动态/静态模型表中。
func (h *Handler) knownModel(model string) bool {
	for _, m := range h.modelList() {
		if m["id"] == model {
			return true
		}
	}
	return false
}

// 静态 SOLO 模型表（SPEC P3：32 个 config_name，来自逆向报告；动态拉取失败时回退）。
var staticModels = []map[string]any{
	{"id": "Doubao-Seed-2.1-Pro", "object": "model", "created": 1753600000, "owned_by": "trae-solo", "context_length": 131072},
	{"id": "seed-code-pro-0430", "object": "model", "created": 1753600000, "owned_by": "trae-solo", "context_length": 131072},
	{"id": "Doubao-Seed-2.1-Turbo", "object": "model", "created": 1753600000, "owned_by": "trae-solo", "context_length": 131072},
	{"id": "Doubao-Seed-2.0-Code", "object": "model", "created": 1753600000, "owned_by": "trae-solo", "context_length": 131072},
	{"id": "DeepSeek-V4-Flash-Official", "object": "model", "created": 1753600000, "owned_by": "trae-solo", "context_length": 131072},
	{"id": "browser_use_subagent", "object": "model", "created": 1753600000, "owned_by": "trae-solo", "context_length": 131072},
	{"id": "glm-5.2", "object": "model", "created": 1753600000, "owned_by": "trae-solo", "context_length": 131072},
	{"id": "glm-5-turbo", "object": "model", "created": 1753600000, "owned_by": "trae-solo", "context_length": 131072},
	{"id": "glm-5", "object": "model", "created": 1753600000, "owned_by": "trae-solo", "context_length": 131072},
	{"id": "DeepSeek-V4-Pro", "object": "model", "created": 1753600000, "owned_by": "trae-solo", "context_length": 131072},
	{"id": "DeepSeek-V4-Flash", "object": "model", "created": 1753600000, "owned_by": "trae-solo", "context_length": 131072},
	{"id": "kimi-k3", "object": "model", "created": 1753600000, "owned_by": "trae-solo", "context_length": 131072},
	{"id": "kimi-k2.7-code", "object": "model", "created": 1753600000, "owned_by": "trae-solo", "context_length": 131072},
	{"id": "kimi-k2.6", "object": "model", "created": 1753600000, "owned_by": "trae-solo", "context_length": 131072},
	{"id": "minimax-m3", "object": "model", "created": 1753600000, "owned_by": "trae-solo", "context_length": 131072},
	{"id": "qwen-3.7-plus", "object": "model", "created": 1753600000, "owned_by": "trae-solo", "context_length": 131072},
	{"id": "sagitta", "object": "model", "created": 1753600000, "owned_by": "trae-solo", "context_length": 131072},
	{"id": "aquila", "object": "model", "created": 1753600000, "owned_by": "trae-solo", "context_length": 131072},
	{"id": "custom_model_gemini", "object": "model", "created": 1753600000, "owned_by": "trae-solo", "context_length": 131072},
	{"id": "custom_model_placeholder", "object": "model", "created": 1753600000, "owned_by": "trae-solo", "context_length": 131072},
	{"id": "custom_model_1M_text", "object": "model", "created": 1753600000, "owned_by": "trae-solo", "context_length": 131072},
	{"id": "custom_model_1M", "object": "model", "created": 1753600000, "owned_by": "trae-solo", "context_length": 131072},
	{"id": "custom_model_kimi", "object": "model", "created": 1753600000, "owned_by": "trae-solo", "context_length": 131072},
	{"id": "custom_model_claude", "object": "model", "created": 1753600000, "owned_by": "trae-solo", "context_length": 131072},
	{"id": "custom_model_gpt-5", "object": "model", "created": 1753600000, "owned_by": "trae-solo", "context_length": 131072},
	{"id": "custom_model_no-fc", "object": "model", "created": 1753600000, "owned_by": "trae-solo", "context_length": 131072},
	{"id": "custom_model_deepseek_chat", "object": "model", "created": 1753600000, "owned_by": "trae-solo", "context_length": 131072},
	{"id": "custom_model_deepseek_reasoner", "object": "model", "created": 1753600000, "owned_by": "trae-solo", "context_length": 131072},
	{"id": "custom_model_deepseek_v4", "object": "model", "created": 1753600000, "owned_by": "trae-solo", "context_length": 131072},
	{"id": "explore_sub_agent_v13", "object": "model", "created": 1753600000, "owned_by": "trae-solo", "context_length": 131072},
	{"id": "explore_sub_agent_v2", "object": "model", "created": 1753600000, "owned_by": "trae-solo", "context_length": 131072},
	{"id": "summary", "object": "model", "created": 1753600000, "owned_by": "trae-solo", "context_length": 131072},
}

// dynamicModelsCache 动态模型缓存（成功 1h / 失败负缓存 5min）。
var dynamicModelsCache struct {
	sync.RWMutex
	ids      []upstream.ModelInfo
	fetched  time.Time
	lastFail time.Time
}

const (
	dynamicModelsTTL        = time.Hour
	modelsFetchFailCooldown = 5 * time.Minute
)

func (h *Handler) models(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"object": "list",
		"data":   h.modelList(),
	})
}

// modelList 动态获取模型列表并包装成 OpenAI 格式；失败回退静态表。
func (h *Handler) modelList() []map[string]any {
	if infos := h.fetchDynamicModels(); len(infos) > 0 {
		out := make([]map[string]any, 0, len(infos))
		for _, mi := range infos {
			entry := map[string]any{
				"id":             mi.ID,
				"object":         "model",
				"created":        1753600000,
				"owned_by":       "trae-solo",
				"context_length": mi.ContextWindow,
			}
			if entry["context_length"] == 0 {
				entry["context_length"] = 131072
			}
			out = append(out, entry)
		}
		return out
	}
	return staticModels
}

// fetchDynamicModels 从池中任一健康账号拉模型列表（get_detail_param），缓存 1h。
func (h *Handler) fetchDynamicModels() []upstream.ModelInfo {
	dynamicModelsCache.RLock()
	if len(dynamicModelsCache.ids) > 0 && time.Since(dynamicModelsCache.fetched) < dynamicModelsTTL {
		out := dynamicModelsCache.ids
		dynamicModelsCache.RUnlock()
		return out
	}
	if !dynamicModelsCache.lastFail.IsZero() && time.Since(dynamicModelsCache.lastFail) < modelsFetchFailCooldown {
		dynamicModelsCache.RUnlock()
		return nil
	}
	dynamicModelsCache.RUnlock()

	// 用 Peek（只读选号）：拉模型表是辅助调用，不能提交 lastUsed/usedSeq，
	// 否则每个 chat 请求都会因 mapModel→knownModel→modelList 触发一次隐式选号，
	// 污染闲置补偿并让轮换次序错乱。
	acct := h.cfg.Pool.Peek("")
	if acct == nil {
		return nil
	}
	infos, err := h.cfg.Upstream.FetchModels(acct)
	if err != nil || len(infos) == 0 {
		dynamicModelsCache.Lock()
		dynamicModelsCache.lastFail = time.Now()
		dynamicModelsCache.Unlock()
		return nil
	}
	dynamicModelsCache.Lock()
	dynamicModelsCache.ids = infos
	dynamicModelsCache.fetched = time.Now()
	dynamicModelsCache.lastFail = time.Time{}
	dynamicModelsCache.Unlock()
	return infos
}

// ---------------------------------------------------------------------------
// chat
// ---------------------------------------------------------------------------

// setModelInBody 将 body 中 model 字段替换为 configName，并返回改写后的 body。
func setModelInBody(body []byte, configName string) []byte {
	var obj map[string]any
	if err := json.Unmarshal(body, &obj); err != nil {
		return body
	}
	obj["model"] = configName
	out, err := json.Marshal(obj)
	if err != nil {
		return body
	}
	return out
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func writeJSON(w http.ResponseWriter, status int, v any) {
	raw, _ := json.Marshal(v)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(raw)
}

func writeOpenAIError(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, map[string]any{
		"error": map[string]any{
			"message": msg,
			"type":    "api_error",
			"code":    code,
		},
	})
}
