// Package server 暴露 OpenAI 兼容 HTTP 接口，内部驱动 pool 挑号 + upstream 转发。
package server

import (
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
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
	// PublicBaseURL 对外可访问的服务根地址（如 https://trae.example.com）。
	// 用于生成 TRAE 登录回调链接 http(s)://<host>/authorize；为空时回落到
	// http://127.0.0.1:<callback_port>/authorize（仅本机裸跑可用，Docker 下不通）。
	PublicBaseURL string
	// CORSOrigins 允许跨域访问 /v1/* 的来源列表；空 = "*"。
	// 本服务用 Bearer 鉴权、不用 cookie，因此不需要 Allow-Credentials。
	CORSOrigins []string
	// ModelAliases 客户端模型名 → 上游 config_name 的别名表（nil = 无别名）。
	//
	// 只做**一级**映射，不递归；且客户端传的名字若是合法静态模型 id 则不生效
	// （避免把真实模型名误改写成别的）。详见 alias.go 的 resolveModelAlias。
	ModelAliases map[string]string
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

	// 动态模型表缓存（成功 1h / 失败负缓存 30s）。
	// 放在 Handler 上而不是包级全局：多个 Handler 实例（测试/多 mux）互不干扰，
	// 也避免负缓存把「刚加完号」的实例卡在空模型表里。
	modelsMu      sync.Mutex
	modelsIDs     []upstream.ModelInfo
	modelsFetched time.Time
	modelsFail    time.Time
	modelsLoading chan struct{} // non-nil = 有 goroutine 正在拉取（singleflight）

	// 模型别名的运行时读写锁（面板保存后热更新，见 alias.go）。
	aliasesMu sync.RWMutex

	// 面板登录爆破限流（按来源 IP 计数）。
	loginLimiter loginLimiter
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
	// 别名为空时依次回落到 config.json 的 model_aliases、TW2A_MODEL_ALIASES。
	// 三条来源汇成同一个 map，优先级见 alias.go 文件尾注释。
	if len(cfg.ModelAliases) == 0 {
		cfg.ModelAliases = loadModelAliasesConfig(cfg.ConfigPath)
	}
	// 面板保存的兜底副本（Docker 下 config.json 常常没挂载，只有这里能活过重建）。
	if len(cfg.ModelAliases) == 0 {
		cfg.ModelAliases = loadModelAliasesStore(aliasStorePathFor(cfg.ConfigPath))
	}
	if len(cfg.ModelAliases) == 0 {
		cfg.ModelAliases = loadModelAliasesEnv()
	}
	h := &Handler{cfg: cfg, mux: http.NewServeMux(), logins: map[string]*pendingLogin{}}
	h.mux.HandleFunc("POST /v1/chat/completions", h.withAuth(h.chatCompletions))
	h.mux.HandleFunc("GET /v1/models", h.withAuth(h.models))
	h.mux.HandleFunc("GET /v1/models/{id}", h.withAuth(h.modelByID))
	// 兼容面补齐（见 compat.go）：老版 text completion、新版 Responses API、
	// 以及明确「不支持」的 embeddings。三者都复用 chatCompletions 的业务内核。
	h.mux.HandleFunc("POST /v1/completions", h.withAuth(h.legacyCompletions))
	h.mux.HandleFunc("POST /v1/responses", h.withAuth(h.responses))
	// embeddings 上游没有：返回 501 而不是 404，让调用方明确知道「不是路径错」。
	h.mux.HandleFunc("POST /v1/embeddings", h.withAuth(h.embeddingsUnsupported))
	// Prometheus 抓取端点：刻意不鉴权（scrape_config 不便带自定义头），
	// 生产需用反向代理做 IP 白名单。详见 metrics.go 文件头注释。
	h.mux.HandleFunc("GET /metrics", h.metrics)
	// 模型别名：GET 查看、POST 保存（写 config.json 并立即生效）。
	h.mux.HandleFunc("GET /admin/api/aliases", h.adminReadAuth(h.adminAliases))
	h.mux.HandleFunc("POST /admin/api/aliases", h.withAdminAuth(h.adminAliases))
	h.mux.HandleFunc("GET /status", h.withAuth(h.status))
	h.mux.HandleFunc("GET /healthz", h.healthz)
	// readiness 与 liveness 必须分开：
	//   /healthz 只回答「进程还活着」，用于容器重启判定——账号全挂了也不该重启进程；
	//   /readyz  回答「现在能不能接流量」，用于 LB/反代摘流——没有可用账号时
	//            继续把请求分进来只会让用户拿到 503，不如干脆摘掉。
	h.mux.HandleFunc("GET /readyz", h.readyz)
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
	// 所有响应统一带 X-Request-ID（请求头已带则沿用，便于端到端串联上游/本端日志）。
	reqID := strings.TrimSpace(r.Header.Get("X-Request-ID"))
	if reqID == "" {
		reqID = newRequestID()
	}
	w.Header().Set("X-Request-ID", reqID)

	if h.applyCORS(w, r) {
		return // OPTIONS 预检已处理
	}
	h.mux.ServeHTTP(w, r)
}

// applyCORS 处理跨域：/v1/* 支持 OPTIONS 预检，返回 true 表示已响应、调用方应直接返回。
//
// 本服务用 Authorization: Bearer 鉴权、不依赖 cookie，因此**不**设置
// Access-Control-Allow-Credentials: true，可以安全地用 "*" 作为默认允许来源。
// 配置了 CORSOrigins 时按白名单回显（支持浏览器带凭据以外的场景）。
func (h *Handler) applyCORS(w http.ResponseWriter, r *http.Request) bool {
	// 只对 API 路径开跨域。/authorize（浏览器跳转落点，query 里带 refreshToken）
	// 与 /admin 页面一律不加 CORS 头——回调页绝不允许被任何来源的脚本读取。
	if !strings.HasPrefix(r.URL.Path, "/v1/") && !strings.HasPrefix(r.URL.Path, "/admin/api/") {
		return false
	}
	origin := strings.TrimSpace(r.Header.Get("Origin"))
	if origin != "" {
		allow := "*"
		if len(h.cfg.CORSOrigins) > 0 {
			allow = ""
			for _, o := range h.cfg.CORSOrigins {
				if o == "*" || strings.EqualFold(o, origin) {
					allow = o
					break
				}
			}
		}
		if allow != "" {
			if allow == "*" {
				w.Header().Set("Access-Control-Allow-Origin", "*")
			} else {
				w.Header().Set("Access-Control-Allow-Origin", allow)
				w.Header().Add("Vary", "Origin")
			}
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, X-Request-ID")
			w.Header().Set("Access-Control-Expose-Headers", "X-Request-ID")
			w.Header().Set("Access-Control-Max-Age", "600")
		}
	}
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return true
	}
	return false
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

// readyz 就绪探针：至少有一个可路由账号才算 ready。
//
// 为什么不用 /healthz 兼任：容器编排用 liveness 决定「重启」，而「所有账号
// 都在冷却」是运行期正常状态（等冷却结束即可自愈），重启进程毫无帮助且会
// 丢掉内存里的冷却计时。所以两者必须是不同语义、不同消费者。
//
// 返回 503 时带上可诊断的计数，避免运维只能看到「不 ready」却不知为何。
func (h *Handler) readyz(w http.ResponseWriter, r *http.Request) {
	if h.cfg.Pool == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"ready": false, "reason": "pool not configured"})
		return
	}
	cnt := h.cfg.Pool.Counts()
	if cnt.Healthy <= 0 {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"ready": false, "reason": "no routable account", "counts": cnt,
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ready": true, "healthy": cnt.Healthy})
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
	// 别名一级映射：clientModel → alias → 下面照常走静态/动态表 → configName。
	// 放在 mapModel 内部而不是调用点，是为了让「所有入口」都自动获得别名能力
	// （chat / completions / responses 共三条路径，漏一处就会出现行为不一致）。
	model = h.resolveModelAlias(model)
	// 去掉内部名后缀（__dev / __max 等）
	base := model
	if i := strings.Index(model, "__"); i >= 0 {
		base = model[:i]
	}
	// 先查静态表：静态表覆盖了全部已确认的 config_name，命中就**不必**打上游。
	// 这条快速路径很关键——mapModel 在每个请求上都会跑，若每次都走
	// modelList()（可能触发 FetchModels 上游调用），等于每个对话请求
	// 额外多打一次上游模型接口，还会随机消耗一个账号的配额。
	if id, ok := staticModelMatch(base); ok {
		return id, nil
	}
	// 动态表（上游新增模型才会走到这里，且有 1h 缓存 + singleflight）。
	if h.knownModel(base) {
		return base, nil
	}
	// 大小写不敏感地按真实 id 匹配（绝不改写 id 本身）：
	// 上游的 config_name 是大小写敏感的（glm-5.2 / DeepSeek-V4-Pro），
	// 之前的「下划线分段首字母大写」规则会把 custom_model_1M 这类 id 改写成
	// 并不存在的 Custom-Model-1m，属于有损映射。
	if id, ok := h.knownModelFold(base); ok {
		return id, nil
	}
	// 最后再试历史兼容规则（deepseek_v4_pro → DeepSeek-V4-Pro）。
	norm := normalizeModelName(base)
	if id, ok := staticModelMatch(norm); ok {
		return id, nil
	}
	if id, ok := h.knownModelFold(norm); ok {
		return id, nil
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

// staticModelMatch 在静态表里精确匹配（先精确、再大小写不敏感），返回原始 id。
// 静态表是编译期常量，纯内存查表，无锁无 IO。
func staticModelMatch(model string) (string, bool) {
	for _, m := range staticModels {
		if id, ok := m["id"].(string); ok && id == model {
			return id, true
		}
	}
	for _, m := range staticModels {
		if id, ok := m["id"].(string); ok && strings.EqualFold(id, model) {
			return id, true
		}
	}
	return "", false
}

// knownModel 判断 model 是否在动态/静态模型表中（精确匹配）。
func (h *Handler) knownModel(model string) bool {
	for _, m := range h.modelList() {
		if m["id"] == model {
			return true
		}
	}
	return false
}

// knownModelFold 大小写不敏感匹配，返回模型表里的**原始 id**（不做任何改写）。
// 用于兼容客户端传 glm-5.2 / GLM-5.2 这类大小写差异。
func (h *Handler) knownModelFold(model string) (string, bool) {
	for _, m := range h.modelList() {
		if id, ok := m["id"].(string); ok && strings.EqualFold(id, model) {
			return id, true
		}
	}
	return "", false
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

const (
	dynamicModelsTTL        = time.Hour
	modelsFetchFailCooldown = 30 * time.Second // 负缓存缩短：刚加完号不该 5 分钟看不到模型
)

func (h *Handler) models(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"object": "list",
		"data":   h.modelList(),
	})
}

// modelByID GET /v1/models/{id}：按 id 返回单个模型对象；不存在返回 404 not_found_error。
func (h *Handler) modelByID(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(r.PathValue("id"))
	for _, m := range h.modelList() {
		if mid, _ := m["id"].(string); mid == id {
			writeJSON(w, http.StatusOK, m)
			return
		}
	}
	writeOpenAIError(w, http.StatusNotFound, "not_found", "model "+id+" not found")
}

// modelList 动态获取模型列表并包装成 OpenAI 格式；失败回退静态表。
func (h *Handler) modelList() []map[string]any {
	return h.withAliases(h.baseModelList())
}

// withAliases 把别名作为额外的模型条目追加到 /v1/models。
//
// 为什么必须加：绝大多数客户端（LibreChat / OpenWebUI / Cline）只允许
// 从 /v1/models 的返回值里选模型。别名不在列表里，用户就「看得到却选不了」，
// 只能手填——那等于别名白配。owned_by 标成 alias，便于和真实模型区分。
func (h *Handler) withAliases(list []map[string]any) []map[string]any {
	aliases := h.aliasesSnapshot()
	if len(aliases) == 0 {
		return list
	}
	have := make(map[string]bool, len(list))
	for _, m := range list {
		if id, ok := m["id"].(string); ok {
			have[id] = true
		}
	}
	names := make([]string, 0, len(aliases))
	for k := range aliases {
		names = append(names, k)
	}
	sort.Strings(names) // 稳定顺序，避免每次请求列表抖动
	out := append([]map[string]any{}, list...)
	for _, k := range names {
		if have[k] {
			continue
		}
		out = append(out, map[string]any{
			"id": k, "object": "model", "created": 1753600000,
			"owned_by": "alias", "context_length": 131072,
		})
	}
	return out
}

// baseModelList 未叠加别名的原始模型表。
func (h *Handler) baseModelList() []map[string]any {
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
//
// 并发控制用 singleflight 语义：同一时刻只允许一个 goroutine 真正打上游，
// 其余请求阻塞等它出结果。否则 20 个并发 chat 请求会各自打一次上游模型表。
func (h *Handler) fetchDynamicModels() []upstream.ModelInfo {
	h.modelsMu.Lock()
	if len(h.modelsIDs) > 0 && time.Since(h.modelsFetched) < dynamicModelsTTL {
		out := h.modelsIDs
		h.modelsMu.Unlock()
		return out
	}
	if !h.modelsFail.IsZero() && time.Since(h.modelsFail) < modelsFetchFailCooldown {
		h.modelsMu.Unlock()
		return nil
	}
	if wait := h.modelsLoading; wait != nil {
		h.modelsMu.Unlock()
		<-wait
		h.modelsMu.Lock()
		out := h.modelsIDs
		h.modelsMu.Unlock()
		return out
	}
	done := make(chan struct{})
	h.modelsLoading = done
	h.modelsMu.Unlock()

	// 用 Peek（只读选号）：拉模型表是辅助调用，不能提交 lastUsed/usedSeq，
	// 否则每个 chat 请求都会因 mapModel→knownModel→modelList 触发一次隐式选号，
	// 污染闲置补偿并让轮换次序错乱。
	var infos []upstream.ModelInfo
	if acct := h.cfg.Pool.Peek(""); acct != nil {
		if got, err := h.cfg.Upstream.FetchModels(acct); err == nil && len(got) > 0 {
			infos = got
		}
	}

	h.modelsMu.Lock()
	if len(infos) > 0 {
		h.modelsIDs = infos
		h.modelsFetched = time.Now()
		h.modelsFail = time.Time{}
	} else {
		h.modelsFail = time.Now()
	}
	h.modelsLoading = nil
	h.modelsMu.Unlock()
	close(done)
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

// writeOpenAIError 按 OpenAI 规范写错误体。
//
// type 依据 code 前缀映射（OpenAI 的 error.type 是稳定枚举，客户端按它分支），
// code 原样回显本服务的细分码，便于排查。
func writeOpenAIError(w http.ResponseWriter, status int, code, msg string) {
	// code 映射不出来时，再用 HTTP 状态兜底（chat 路径的 openAIErrType 口径一致）。
	t := openAIErrorType(code)
	if t == "api_error" {
		t = openAIErrType(status)
	}
	writeJSON(w, status, map[string]any{
		"error": map[string]any{
			"message": msg,
			"type":    t,
			"code":    code,
		},
	})
}

// openAIErrorType 把本服务的细分 code 映射到 OpenAI 的 error.type 枚举。
func openAIErrorType(code string) string {
	c := strings.ToLower(strings.TrimSpace(code))
	switch {
	case strings.HasPrefix(c, "invalid_api_key"), strings.HasPrefix(c, "authentication"),
		c == "unauthorized", strings.HasPrefix(c, "invalid_token"):
		return "authentication_error"
	case strings.HasPrefix(c, "permission"), strings.HasPrefix(c, "forbidden"):
		return "permission_error"
	case strings.HasPrefix(c, "not_found"):
		return "not_found_error"
	case strings.HasPrefix(c, "rate_limit"):
		return "rate_limit_error"
	case strings.HasPrefix(c, "invalid_request"), strings.HasPrefix(c, "invalid_"),
		c == "bad_request", strings.HasPrefix(c, "import_failed"):
		return "invalid_request_error"
	case strings.HasPrefix(c, "model_not_found"), strings.HasPrefix(c, "unknown_model"):
		return "invalid_request_error"
	default:
		return "api_error"
	}
}
