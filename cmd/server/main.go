// main.go trae2api-web 入口：加载配置 → 构建 pool/session/reqlog → 起 HTTP 服务。
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"trae2api-web/internal/auth"
	"trae2api-web/internal/pool"
	"trae2api-web/internal/reqlog"
	"trae2api-web/internal/scheduler"
	"trae2api-web/internal/server"
	"trae2api-web/internal/session"
	"trae2api-web/internal/upstream"
	"trae2api-web/internal/usage"
)

func main() {
	cfgPath := flag.String("config", "config.json", "path to config json")
	flag.Parse()

	cfg, err := Load(*cfgPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	// 启动自检：账号目录不存在/为空时明确提示，而不是静默空跑（所有请求都会
	// 503「无可用账号」，不看日志很难定位）。
	authCount, authDiag := inspectAuthDir(cfg.AuthDir)
	if authCount == 0 {
		log.Printf("!!! 无可用账号：%s", authDiag)
		log.Printf("!!! 请先启动后用面板导入账号（POST /admin/api/accounts/import），或把凭证文件放进 %s", cfg.AuthDir)
	}
	auths, err := auth.LoadDir(cfg.AuthDir)
	if err != nil {
		log.Fatalf("load auths: %v", err)
	}
	if n := len(auths); n != authCount {
		log.Printf("[warn] %s 下有 %d 个凭证文件，但只有 %d 个可解析（其余已跳过，见上方 auth: skip 日志）",
			cfg.AuthDir, authCount, n)
	}
	log.Printf("loaded %d account(s) from %s", len(auths), cfg.AuthDir)

	p := pool.New(cfg.StateFile)
	p.SetLimits(pool.Limits{
		MaxInFlight:   cfg.Pool.MaxInFlight,
		SoftRateBase:  cfg.SoftRateDur,
		SoftRateMax:   cfg.SoftRateMaxDur,
		BreakerThresh: cfg.Pool.BreakerThreshold,
		BreakerBase:   cfg.BreakerDur,
		BreakerMax:    cfg.BreakerMaxDur,
		DegradeThresh: cfg.Pool.DegradeThreshold,
		DegradeWindow: cfg.DegradeWindowDur,
	})
	// 对齐：剔除 state.json 中已删除 auth 文件的幽灵账号
	p.SyncToDir(auths)

	up := upstream.New()
	up.HTTP.Timeout = time.Duration(cfg.Upstream.TimeoutSeconds) * time.Second
	// 流式客户端无总超时，仅用首字节兜底（时长由 SSE 流本身决定）。
	if tr, ok := up.StreamHTTP.Transport.(*http.Transport); ok {
		tr.ResponseHeaderTimeout = time.Duration(cfg.Upstream.TimeoutSeconds) * time.Second
	}
	// 模型级错误规则（默认空 = 永不触发）。抓到真实报文后在 config.json 的
	// upstream.model_error_rules 里加规则即可，无需改代码。
	if rules := cfg.modelErrorRules(); len(rules) > 0 {
		up.SetModelErrorRules(rules)
		log.Printf("loaded %d model error rule(s)", len(rules))
	}

	// 会话粘性路由（可用性判定直接接 pool）。
	var sess *session.Router
	if cfg.Session.Enabled {
		sess = session.New(session.Config{
			TTL:               cfg.SessionTTL,
			GCInterval:        cfg.SessionGCDuration,
			Available:         p.AvailableUIDs,
			AvailableForModel: p.AvailableForModel,
			CanRoute:          func(model, uid string) bool { return p.UIDAvailableForModel(uid, model) },
		})
		sess.StartGC()
	}

	// 请求流水（内存 + 可选 JSONL 归档）。
	rl := reqlog.New(reqlog.Config{
		Dir:          cfg.ReqLog.Dir,
		Enabled:      cfg.ReqLog.Enabled,
		FileMaxBytes: int64(cfg.ReqLog.FileMaxMB) << 20,
		RetainDays:   cfg.ReqLog.RetainDays,
	})

	sch := scheduler.New(scheduler.Config{
		Pool:         p,
		Upstream:     up,
		CheckinHour:  cfg.Schedule.CheckinHour,
		RefreshHours: cfg.Schedule.RefreshHours,
		BalanceHours: cfg.Schedule.BalanceHours,
		RefreshSkew:  24 * time.Hour,
		// 运行记录与 state.json 同目录：重启后据此补跑「今天该跑但没跑过」的任务，
		// 否则进程在计划整点之后重启就会整天空转。
		StateFile:   filepath.Join(filepath.Dir(cfg.StateFile), "schedule.json"),
		Concurrency: cfg.Schedule.Concurrency,
	})

	// 用量统计（按模型累计 token/缓存；积分靠采样估算）。
	var usageTracker *usage.Tracker
	var creditSampler *usage.Sampler
	if cfg.Usage.Enabled {
		usageTracker = usage.New(cfg.Usage.File, cfg.CreditSamplingDur)
		// 积分采样需要读账号级累计计数器；采样器自己保证只在
		// 「该账号仅本请求在途」时采信差值。
		if cfg.CreditSamplingDur > 0 {
			creditSampler = usage.NewSampler(
				func(uid string) (float64, bool) {
					a := p.AuthByUID(uid)
					if a == nil {
						return 0, false
					}
					return up.ConsumedCredits(a)
				},
				cfg.CreditSamplingDur,
			)
		}
		log.Printf("usage stats enabled (file=%s, credit_sampling=%s)", cfg.Usage.File, cfg.CreditSamplingDur)
	}

	// 思考呈现方式：把配置字符串映射成 upstream.ThinkingMode。
	thinkingMode := upstream.ThinkingReasoning
	switch cfg.Stream.Thinking {
	case StreamThinkingContent:
		thinkingMode = upstream.ThinkingContent
	case StreamThinkingNone:
		thinkingMode = upstream.ThinkingNone
	}

	h := server.NewHandler(server.Config{
		Pool:            p,
		Upstream:        up,
		APIKey:          cfg.APIKey,
		AuthDir:         cfg.AuthDir,
		PlanCooldown:    cfg.PlanCreditDur,
		SoftCooldown:    cfg.SoftRateDur,
		ErrThreshold:    cfg.Pool.BreakerThreshold,
		ErrCooldown:     cfg.ErrCooldownDur,
		DefaultModel:    cfg.DefaultModel,
		RefreshSkew:     24 * time.Hour,
		Session:         sess,
		RequestLog:      rl,
		Usage:           usageTracker,
		CreditSampler:   creditSampler,
		ConfigPath:      *cfgPath,
		StreamThinking:  thinkingMode,
		StreamHeartbeat: cfg.StreamHeartbeat,
		PublicBaseURL:   cfg.PublicBaseURL,
		CORSOrigins:     cfg.CORSOrigins,
	})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go sch.Run(ctx)

	srv := &http.Server{
		Addr:              cfg.Listen,
		Handler:           h,
		ReadHeaderTimeout: 30 * time.Second,
	}
	// 停机第一步：停止接收新请求并等在途请求收尾（流式请求可能很长，给 15s）。
	// shutdownDone 让主 goroutine 能等「在途请求真的收完」再 flush 持久化——
	// ListenAndServe 在 Shutdown 一开始就返回 ErrServerClosed，不等待收尾。
	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Printf("[warn] http shutdown: %v（超时被强制中断）", err)
		}
	}()

	// 第二个 http.Server：监听 CallbackPort（默认 18080），只处理 /authorize 回调。
	// 复用同一 Handler（/authorize 已在主 mux 注册）。
	// cfg.CallbackPort == "0" 时不启动（纯手动粘贴模式）。
	var cbSrv *http.Server
	if cfg.CallbackPort != "" && cfg.CallbackPort != "0" {
		// 绑 0.0.0.0 而不是 127.0.0.1：容器/反代场景下绑回环地址外部永远进不来，
		// 回调必然超时。需要只允许本机访问时把 callback_bind 设成 127.0.0.1。
		cbAddr := net.JoinHostPort(cfg.CallbackBind, cfg.CallbackPort)
		cbSrv = &http.Server{
			Addr:              cbAddr,
			Handler:           h,
			ReadHeaderTimeout: 30 * time.Second,
		}
		go func() {
			<-ctx.Done()
			sc, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = cbSrv.Shutdown(sc)
		}()
		go func() {
			log.Printf("trae2api-web callback server on %s (TRAE login /authorize)", cbAddr)
			if err := cbSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				// 端口被占用（login.sh / 旧实例）不致命，降级为手动粘贴模式。
				log.Printf("callback server (%s) failed: %v — web 登录降级为手动粘贴回调链接", cbAddr, err)
			}
		}()
	}

	log.Printf("trae2api-web listening on %s (api_key=%v)", cfg.Listen, cfg.APIKey != "")
	log.Printf("startup summary: listen=%s accounts=%d api_key=%v reqlog=%v usage=%v session=%v schedule=checkin %02d:00 / refresh %v / balance %v "+
		"callback=%s public_base_url=%q auth_dir=%s ignored_env=%d config_warnings=%d",
		cfg.Listen, len(auths), cfg.APIKey != "",
		cfg.ReqLog.Enabled, cfg.Usage.Enabled, cfg.Session.Enabled,
		cfg.Schedule.CheckinHour, cfg.Schedule.RefreshHours, cfg.Schedule.BalanceHours,
		callbackSummary(cfg), cfg.PublicBaseURL,
		cfg.AuthDir, len(cfg.IgnoredEnv), len(cfg.Warnings))
	// 不用 log.Fatalf：监听失败（端口占用等）也必须走完下面的停机刷盘流程，
	// 否则刚加载进来的 token 轮换/池状态会直接丢。
	var serveErr error
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Printf("[fatal] http serve: %v", err)
		serveErr = err
	}

	// 停机顺序：先等 HTTP 收尾（上面 goroutine），再停后台任务，最后刷盘持久化。
	// 顺序反了会在停机瞬间丢 token 轮换/用量/冷却是最痛的事故。
	stop() // 幂等：确保后台 goroutine 全部收到取消信号
	if serveErr == nil {
		// 正常收到信号退出：等 srv.Shutdown 把在途请求收干净。
		// 启动即失败（端口被占）时没有在途请求，不必白等 20s。
		select {
		case <-shutdownDone:
		case <-time.After(20 * time.Second):
			log.Printf("[warn] 等待在途请求收尾超时（20s），继续停机")
		}
	}
	if sess != nil {
		sess.StopGC()
	}
	if usageTracker != nil {
		usageTracker.Close() // 停后台 flusher（其退出前会 Flush 一次）
		// Close 只发停止信号，不等待；这里显式再 Flush 一次，保证进程退出前
		// 用量/TPS 聚合一定落到磁盘（Flush 幂等、带自己的锁）。
		if err := usageTracker.Flush(); err != nil {
			log.Printf("[warn] usage flush: %v", err)
		}
	}
	rl.Close() // flush 归档写队列
	p.Flush()  // 冷却/额度/状态落盘
	log.Printf("bye")
	if serveErr != nil {
		os.Exit(1)
	}
}

// inspectAuthDir 检查账号目录状态，返回目录下 *-*.json 凭证文件数量与一句可读诊断。
func inspectAuthDir(dir string) (int, string) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, fmt.Sprintf("账号目录 %s 不存在", dir)
		}
		return 0, fmt.Sprintf("无法读取账号目录 %s: %v", dir, err)
	}
	n := 0
	for _, e := range ents {
		if e.IsDir() || !strings.HasPrefix(e.Name(), "trae-") || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		n++
	}
	if n == 0 {
		return 0, fmt.Sprintf("账号目录 %s 为空（没有 trae-*.json 凭证文件）", dir)
	}
	return n, fmt.Sprintf("%s 下有 %d 个凭证文件", dir, n)
}

// callbackSummary 生成启动摘要里的回调说明。
func callbackSummary(cfg *Config) string {
	if cfg.CallbackPort == "0" {
		return "off(手动粘贴)"
	}
	if cfg.PublicBaseURL != "" {
		return cfg.PublicBaseURL + "/authorize"
	}
	return "http://" + net.JoinHostPort(cfg.CallbackBind, cfg.CallbackPort) + "/authorize"
}
