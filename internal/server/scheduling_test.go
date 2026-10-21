package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"trae2api-web/internal/auth"
	"trae2api-web/internal/pool"
	"trae2api-web/internal/reqlog"
	"trae2api-web/internal/session"
	"trae2api-web/internal/upstream"
)

func newSchedPool(t *testing.T, uids ...int) (*pool.Pool, []*auth.Auth) {
	t.Helper()
	p := pool.New("")
	p.SetLimits(pool.Limits{MaxInFlight: 1, SoftRateBase: 100 * time.Millisecond, SoftRateMax: time.Second,
		BreakerThresh: 3, BreakerBase: 50 * time.Millisecond, BreakerMax: time.Second,
		DegradeThresh: 2, DegradeWindow: time.Second})
	t.Cleanup(p.Flush)
	var out []*auth.Auth
	for i := 0; i < uids[0]; i++ {
		a := &auth.Auth{UID: "u" + string(rune('0'+i)), AccessToken: "at" + string(rune('0'+i)), ExpiresAt: 9999999999}
		p.Add(a)
		p.SetCredits(a.UID, 1000, 1000)
		out = append(out, a)
	}
	return p, out
}

// 成功请求后：在途租约必须归还（否则账号会被永久判为占满）。
func TestChatReleasesLeaseOnSuccess(t *testing.T) {
	up := newFakeUpstream(t, func(string) (int, string, bool) { return 200, soloSSE, true })
	p, _ := newSchedPool(t, 1)
	h := NewHandler(Config{Pool: p, Upstream: up})

	for i := 0; i < 5; i++ { // 串行 5 次，若租约泄漏第 2 次就会 503
		req := httptest.NewRequest("POST", "/v1/chat/completions",
			strings.NewReader(`{"model":"glm-5.2","messages":[{"role":"user","content":"hi"}]}`))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != 200 {
			t.Fatalf("req %d: code=%d body=%s", i, rec.Code, rec.Body)
		}
	}
	if n := p.InFlight("u0"); n != 0 {
		t.Fatalf("in-flight leaked: %d", n)
	}
}

// 在途上限生效：并发请求超过 max_in_flight 时，多出的请求不应打到同一账号。
func TestChatRespectsMaxInFlight(t *testing.T) {
	var mu sync.Mutex
	inFlightByAuth := map[string]int{}
	var peak int64
	up := newFakeUpstream(t, func(authz string) (int, string, bool) {
		mu.Lock()
		inFlightByAuth[authz]++
		if int64(inFlightByAuth[authz]) > atomic.LoadInt64(&peak) {
			atomic.StoreInt64(&peak, int64(inFlightByAuth[authz]))
		}
		mu.Unlock()
		time.Sleep(30 * time.Millisecond)
		mu.Lock()
		inFlightByAuth[authz]--
		mu.Unlock()
		return 200, soloSSE, true
	})
	p, _ := newSchedPool(t, 2) // 2 个号，每号上限 1 → 并发上限 2
	h := NewHandler(Config{Pool: p, Upstream: up, MaxRotate: 3})

	var wg sync.WaitGroup
	var failed int64
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := httptest.NewRequest("POST", "/v1/chat/completions",
				strings.NewReader(`{"model":"glm-5.2","messages":[{"role":"user","content":"hi"}]}`))
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != 200 {
				atomic.AddInt64(&failed, 1)
			}
		}()
	}
	wg.Wait()
	if got := atomic.LoadInt64(&peak); got > 1 {
		t.Fatalf("single account saw %d concurrent requests, want <=1", got)
	}
}

// 上游超时不得罚号：账号既不应进冷却，也不应累计错误。
func TestChatUpstreamTimeoutDoesNotPenalize(t *testing.T) {
	up := newFakeUpstream(t, func(string) (int, string, bool) {
		return 0, "", false
	})
	up.HTTP = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, timeoutErr{}
	})}
	p, _ := newSchedPool(t, 1)
	h := NewHandler(Config{Pool: p, Upstream: up})

	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"glm-5.2","messages":[]}`))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 503 {
		t.Fatalf("code=%d", rec.Code)
	}
	st, _ := p.Status("u0")
	// 「不罚号」的准确含义：不影响选号资格。errTotal 允许 +1（仅用于观测，
	// 见 pool.NoteTransportError），但冷却/熔断/连败降权一律不得触发。
	if st.Cooling || st.ConsecutiveFails > 0 || st.BreakerFails > 0 || st.ErrCount > 0 {
		t.Fatalf("timeout must not penalize account: %+v", st)
	}
	if st.ErrTotal != 1 {
		t.Fatalf("transport error should still be counted once for observability: errTotal=%d", st.ErrTotal)
	}
	if p.InFlight("u0") != 0 {
		t.Fatalf("lease leaked after timeout")
	}
}

type timeoutErr struct{}

func (timeoutErr) Error() string   { return "i/o timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

// 429 走软冷却指数退避，且不把 errCount 累加到熔断。
func TestChat429UsesSoftCooldown(t *testing.T) {
	up := newFakeUpstream(t, func(string) (int, string, bool) {
		return 429, `{"code":429,"message":"rate limited"}`, false
	})
	p, _ := newSchedPool(t, 1)
	h := NewHandler(Config{Pool: p, Upstream: up, MaxRotate: 1})

	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"glm-5.2","messages":[]}`))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 503 {
		t.Fatalf("code=%d", rec.Code)
	}
	st, _ := p.Status("u0")
	if !st.Cooling || st.CoolKind != "soft_rate" {
		t.Fatalf("want soft_rate cooldown: %+v", st)
	}
	if st.SoftStreak != 1 {
		t.Fatalf("soft streak=%d want 1", st.SoftStreak)
	}
	if st.BreakerFails != 0 {
		t.Fatalf("429 must not feed breaker: %+v", st)
	}
}

// 模型级规则命中时只锁该模型，其他模型仍可路由。
func TestChatModelErrorRuleScopedToModel(t *testing.T) {
	up := newFakeUpstream(t, func(string) (int, string, bool) {
		return 403, `{"code":9001,"message":"model not available for this account"}`, false
	})
	up.SetModelErrorRules([]upstream.ModelErrorRule{
		{Code: 9001, Kind: "model_unavailable"},
	})
	p, _ := newSchedPool(t, 1)
	h := NewHandler(Config{Pool: p, Upstream: up, MaxRotate: 1, ErrCooldown: time.Minute})

	req := httptest.NewRequest("POST", "/v1/chat/completions",
		strings.NewReader(`{"model":"glm-5.2","messages":[]}`))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	st, _ := p.Status("u0")
	if st.Cooling {
		t.Fatalf("model-scoped error must not cool the whole account: %+v", st)
	}
	if len(st.RateLimitedModels) != 1 || st.RateLimitedModels[0].Model != "glm-5.2" {
		t.Fatalf("want glm-5.2 model cooldown: %+v", st.RateLimitedModels)
	}
	// 同一账号在其他模型上仍可用。
	if p.PickByUIDForModel("u0", "kimi-k3") == nil {
		t.Fatal("account should still route other models")
	}
	if p.PickByUIDForModel("u0", "glm-5.2") != nil {
		t.Fatal("blocked model should not route")
	}
	// 全池对该模型都被锁时，错误码应明确指出是模型阻塞而非账号不可用。
	req = httptest.NewRequest("POST", "/v1/chat/completions",
		strings.NewReader(`{"model":"glm-5.2","messages":[]}`))
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if !strings.Contains(rec.Body.String(), "model_blocked") {
		t.Fatalf("want model_blocked error, got %s", rec.Body)
	}
}

// 空规则表时模型级分类永不触发（默认行为，防止误伤）。
func TestChatNoModelRuleByDefault(t *testing.T) {
	up := newFakeUpstream(t, func(string) (int, string, bool) {
		return 403, `{"code":9001,"message":"model not available"}`, false
	})
	p, _ := newSchedPool(t, 1)
	h := NewHandler(Config{Pool: p, Upstream: up, MaxRotate: 1})
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"glm-5.2","messages":[]}`))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	st, _ := p.Status("u0")
	if len(st.RateLimitedModels) != 0 {
		t.Fatalf("no rules => no model cooldown: %+v", st.RateLimitedModels)
	}
}

// 会话粘性：同一会话应始终打同一账号。
func TestChatStickySession(t *testing.T) {
	var mu sync.Mutex
	seen := map[string]int{}
	up := newFakeUpstream(t, func(authz string) (int, string, bool) {
		mu.Lock()
		seen[authz]++
		mu.Unlock()
		return 200, soloSSE, true
	})
	p, _ := newSchedPool(t, 3)
	sess := session.New(session.Config{
		TTL: time.Minute, Available: p.AvailableUIDs,
		AvailableForModel: p.AvailableForModel,
		CanRoute:          func(model, uid string) bool { return p.UIDAvailableForModel(uid, model) },
	})
	h := NewHandler(Config{Pool: p, Upstream: up, Session: sess})

	// 预热模型表缓存：首次请求会额外发一次 get_detail_param（Peek 选号），
	// 那次不能计入粘性统计。
	warm := httptest.NewRequest("POST", "/v1/chat/completions",
		strings.NewReader(`{"model":"glm-5.2","user":"warmup","messages":[{"role":"user","content":"hi"}]}`))
	h.ServeHTTP(httptest.NewRecorder(), warm)
	mu.Lock()
	seen = map[string]int{}
	mu.Unlock()

	body := `{"model":"glm-5.2","user":"session-abc","messages":[{"role":"user","content":"hi"}]}`
	for i := 0; i < 8; i++ {
		req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(body))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != 200 {
			t.Fatalf("code=%d", rec.Code)
		}
	}
	if len(seen) != 1 {
		t.Fatalf("sticky session spread across %d accounts: %v", len(seen), seen)
	}
}

// 请求流水：一次成功请求必须落一条记录，且带 uid/模型/耗时。
func TestChatRecordsRequestLog(t *testing.T) {
	up := newFakeUpstream(t, func(string) (int, string, bool) { return 200, soloSSE, true })
	p, _ := newSchedPool(t, 1)
	rl := reqlog.New(reqlog.Config{Enabled: false})
	defer rl.Close()
	h := NewHandler(Config{Pool: p, Upstream: up, RequestLog: rl})

	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"glm-5.2","messages":[]}`))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	recent := rl.Recent(10)
	if len(recent) != 1 {
		t.Fatalf("want 1 request log, got %d", len(recent))
	}
	e := recent[0]
	if e.UID != "u0" || e.Model != "glm-5.2" || e.Outcome != reqlog.OutcomeSuccess {
		t.Fatalf("bad event: %+v", e)
	}
	if e.TotalTokens != 7 {
		t.Fatalf("usage not recorded: %+v", e)
	}
	if m := rl.Metrics(); m.Total != 1 || m.Succeeded != 1 {
		t.Fatalf("metrics=%+v", m)
	}
}

// credits 回写：成功的额度查询必须更新 pool，使加权选号真正生效（旧实现是死代码）。
func TestAdminBalanceWritesBackCredits(t *testing.T) {
	up := newFakeUpstream(t, func(authz string) (int, string, bool) {
		return 200, `{"user_entitlement_pack_list":[{"entitlement_base_info":{"quota":{"credits_limit":800}},"usage":{"credits_amount":300}}]}`, false
	})
	p, _ := newSchedPool(t, 1)
	p.SetCredits("u0", 0, 0)
	h := NewHandler(Config{Pool: p, Upstream: up})

	req := httptest.NewRequest("POST", "/admin/api/accounts/u0/balance", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body)
	}
	st, _ := p.Status("u0")
	if st.Credits != 500 {
		t.Fatalf("credits=%d want 500 (800-300)", st.Credits)
	}
	if st.CreditsTotal != 800 {
		t.Fatalf("credits_total=%d want 800", st.CreditsTotal)
	}
}

// 面板状态接口应包含调度相关字段（在途/权重/冷却类型）。
func TestAdminAccountsExposeSchedulingFields(t *testing.T) {
	p, _ := newSchedPool(t, 1)
	p.CooldownSoftRate("u0", 100*time.Millisecond, time.Time{}, "test")
	h := NewHandler(Config{Pool: p, Upstream: upstream.New()})

	req := httptest.NewRequest("GET", "/admin/api/accounts", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var resp struct {
		Accounts []map[string]any `json:"accounts"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Accounts) != 1 {
		t.Fatalf("accounts=%v", resp.Accounts)
	}
	a := resp.Accounts[0]
	for _, k := range []string{"in_flight", "weight", "cool_kind", "max_in_flight"} {
		if _, ok := a[k]; !ok {
			t.Errorf("missing field %q in %v", k, a)
		}
	}
	if a["cool_kind"] != "soft_rate" {
		t.Errorf("cool_kind=%v", a["cool_kind"])
	}
}

// 请求流水接口必须能看到「刚发生、尚未异步落盘」的请求（内存 + 归档合并）。
func TestRequestsAPIIncludesRecentNotYetFlushed(t *testing.T) {
	up := newFakeUpstream(t, func(string) (int, string, bool) { return 200, soloSSE, true })
	p, _ := newSchedPool(t, 1)
	rl := reqlog.New(reqlog.Config{Enabled: true, Dir: t.TempDir()})
	defer rl.Close()
	h := NewHandler(Config{Pool: p, Upstream: up, RequestLog: rl})

	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"glm-5.2","messages":[]}`))
	h.ServeHTTP(httptest.NewRecorder(), req)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/admin/api/requests?limit=10", nil))
	var resp struct {
		Events []reqlog.Event `json:"events"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Events) != 1 || resp.Events[0].Outcome != reqlog.OutcomeSuccess {
		t.Fatalf("events=%+v", resp.Events)
	}
}

// 控制台页面必须包含新增的运维视图与动作（防止模板改动漏接）。
func TestAdminPageHasSchedulingViews(t *testing.T) {
	h := NewHandler(Config{Pool: pool.New(""), Upstream: upstream.New()})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/admin", nil))
	body := rec.Body.String()
	for _, want := range []string{
		"请求流水", "全部签到", "/admin/api/requests", "pauseAcct", "editProfile", "balanceAcct",
		"max_in_flight", "cool_kind",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("admin page missing %q", want)
		}
	}
}
