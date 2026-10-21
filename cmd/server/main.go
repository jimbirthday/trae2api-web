// main.go trae2api-web 入口：加载配置 → 构建 pool/session/reqlog → 起 HTTP 服务。
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
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

	auths, err := auth.LoadDir(cfg.AuthDir)
	if err != nil {
		log.Fatalf("load auths: %v", err)
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
		defer sess.StopGC()
	}

	// 请求流水（内存 + 可选 JSONL 归档）。
	rl := reqlog.New(reqlog.Config{
		Dir:          cfg.ReqLog.Dir,
		Enabled:      cfg.ReqLog.Enabled,
		FileMaxBytes: int64(cfg.ReqLog.FileMaxMB) << 20,
		RetainDays:   cfg.ReqLog.RetainDays,
	})
	defer rl.Close()

	sch := scheduler.New(scheduler.Config{
		Pool:         p,
		Upstream:     up,
		CheckinHour:  cfg.Schedule.CheckinHour,
		RefreshHours: cfg.Schedule.RefreshHours,
		BalanceHours: cfg.Schedule.BalanceHours,
		RefreshSkew:  24 * time.Hour,
	})

	// 用量统计（按模型累计 token/缓存；积分靠采样估算）。
	var usageTracker *usage.Tracker
	var creditSampler *usage.Sampler
	if cfg.Usage.Enabled {
		usageTracker = usage.New(cfg.Usage.File, cfg.CreditSamplingDur)
		defer usageTracker.Close()
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

	h := server.NewHandler(server.Config{
		Pool:          p,
		Upstream:      up,
		APIKey:        cfg.APIKey,
		AuthDir:       cfg.AuthDir,
		PlanCooldown:  cfg.PlanCreditDur,
		SoftCooldown:  cfg.SoftRateDur,
		ErrThreshold:  cfg.Pool.BreakerThreshold,
		ErrCooldown:   cfg.ErrCooldownDur,
		DefaultModel:  cfg.DefaultModel,
		RefreshSkew:   24 * time.Hour,
		Session:       sess,
		RequestLog:    rl,
		Usage:         usageTracker,
		CreditSampler: creditSampler,
		ConfigPath:    *cfgPath,
	})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go sch.Run(ctx)

	srv := &http.Server{
		Addr:              cfg.Listen,
		Handler:           h,
		ReadHeaderTimeout: 30 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	// 第二个 http.Server：监听 CallbackPort（默认 18080），只处理 /authorize 回调。
	// 复用同一 Handler（/authorize 已在主 mux 注册）。
	// cfg.CallbackPort == "0" 时不启动（纯手动粘贴模式）。
	var cbSrv *http.Server
	if cfg.CallbackPort != "" && cfg.CallbackPort != "0" {
		cbSrv = &http.Server{
			Addr:              "127.0.0.1:" + cfg.CallbackPort,
			Handler:           h,
			ReadHeaderTimeout: 30 * time.Second,
		}
		go func() {
			<-ctx.Done()
			sc, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_ = cbSrv.Shutdown(sc)
		}()
		go func() {
			log.Printf("trae2api-web callback server on 127.0.0.1:%s (TRAE login /authorize)", cfg.CallbackPort)
			if err := cbSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				// 端口被占用（login.sh / 旧实例）不致命，降级为手动粘贴模式。
				log.Printf("callback server (:%s) failed: %v — web 登录降级为手动粘贴回调链接", cfg.CallbackPort, err)
			}
		}()
	}

	log.Printf("trae2api-web listening on %s (api_key=%v)", cfg.Listen, cfg.APIKey != "")
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("http: %v", err)
	}
	// 停机：把 pool 内存状态刷盘，避免丢冷却/额度。
	p.Flush()
	log.Printf("bye")
}
