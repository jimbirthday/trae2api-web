package server

import (
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"trae2api-web/internal/pool"
	"trae2api-web/internal/session"
)

// 回归：会话粘性开启时，新会话的选号必须仍走 pool 加权算法。
//
// 曾经的缺陷：chat 路径用 session.Resolve（未命中就按 hash 直接分配账号），
// 把新会话的选号完全架空 —— 实测分布 15/18/18（近似均匀），余额/闲置/权重全失效。
// 正确分工：session.Lookup 只查 → pool 加权选号 → 成功后 session.Bind 回写。
func TestStickyRespectsWeight(t *testing.T) {
	var mu sync.Mutex
	seen := map[string]int{}
	up := newFakeUpstream(t, func(authz string) (int, string, bool) {
		mu.Lock()
		seen[authz]++
		mu.Unlock()
		return 200, soloSSE, true
	})
	p, auths := newSchedPool(t, 3)
	// u0 余额 1, u1 余额 1000, u2 余额 1_000_000
	p.SetCredits(auths[0].UID, 1, 1000)
	p.SetCredits(auths[1].UID, 1000, 1000)
	p.SetCredits(auths[2].UID, 1000000, 1000000)
	sess := session.New(session.Config{
		TTL: time.Minute, Available: p.AvailableUIDs,
		AvailableForModel: p.AvailableForModel,
		CanRoute:          func(model, uid string) bool { return p.UIDAvailableForModel(uid, model) },
	})
	h := NewHandler(Config{Pool: p, Upstream: up, Session: sess})

	// 预热模型表缓存（首次请求会额外发 get_detail_param），避免污染分布统计。
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("POST", "/v1/chat/completions",
		strings.NewReader(`{"model":"glm-5.2","user":"warmup","messages":[{"role":"user","content":"hi"}]}`)))
	mu.Lock()
	seen = map[string]int{}
	mu.Unlock()

	// 50 个不同会话，各自只发 1 次（都是新键 → 会走 session 分配）
	for i := 0; i < 50; i++ {
		body := `{"model":"glm-5.2","user":"sess-` + string(rune('a'+i%26)) + string(rune('0'+i/26)) + `","messages":[{"role":"user","content":"hi"}]}`
		req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(body))
		h.ServeHTTP(httptest.NewRecorder(), req)
	}
	t.Logf("distribution=%v (u2=at2 余额最高，若权重生效应占绝大多数)", seen)
	if seen["Cloud-IDE-JWT at2"] < 30 {
		t.Fatalf("权重未生效：高余额账号只被选中 %d/50 次, dist=%v", seen["Cloud-IDE-JWT at2"], seen)
	}
}

// 回归：粘性绑定必须真正生效（Lookup 命中同一账号），且绑定后不再漂移。
func TestStickyBindsAfterSuccess(t *testing.T) {
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

	// 预热模型表缓存，避免把辅助调用误算成粘性漂移。
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("POST", "/v1/chat/completions",
		strings.NewReader(`{"model":"glm-5.2","user":"warmup","messages":[{"role":"user","content":"hi"}]}`)))
	mu.Lock()
	seen = map[string]int{}
	mu.Unlock()

	body := `{"model":"glm-5.2","user":"bind-check","messages":[{"role":"user","content":"hi"}]}`
	for i := 0; i < 10; i++ {
		req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(body))
		if rec := httptest.NewRecorder(); func() bool { h.ServeHTTP(rec, req); return rec.Code == 200 }() == false {
			t.Fatalf("req %d failed", i)
		}
	}
	if len(seen) != 1 {
		t.Fatalf("sticky drift: %v", seen)
	}
	// 预热键 "warmup" 也会留一条绑定，故这里是 2（预热 + bind-check）。
	if n := sess.Count(); n != 2 {
		t.Fatalf("session not bound: count=%d", n)
	}
	if _, ok := sess.Lookup(session.ExtractKey([]byte(body)), "glm-5.2"); !ok {
		t.Fatal("current session not bound")
	}
}

// 绑定号一旦进入冷却，必须解绑并漂移到别的号（不能死守）。
func TestStickyUnbindsWhenAccountUnavailable(t *testing.T) {
	var mu sync.Mutex
	seen := map[string]int{}
	up := newFakeUpstream(t, func(authz string) (int, string, bool) {
		mu.Lock()
		seen[authz]++
		mu.Unlock()
		return 200, soloSSE, true
	})
	p, auths := newSchedPool(t, 2)
	sess := session.New(session.Config{
		TTL: time.Minute, Available: p.AvailableUIDs,
		AvailableForModel: p.AvailableForModel,
		CanRoute:          func(model, uid string) bool { return p.UIDAvailableForModel(uid, model) },
	})
	h := NewHandler(Config{Pool: p, Upstream: up, Session: sess})

	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("POST", "/v1/chat/completions",
		strings.NewReader(`{"model":"glm-5.2","user":"warmup","messages":[{"role":"user","content":"hi"}]}`)))

	body := `{"model":"glm-5.2","user":"drift","messages":[{"role":"user","content":"hi"}]}`
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(body)))
	if rec.Code != 200 {
		t.Fatalf("first req code=%d", rec.Code)
	}
	key := session.ExtractKey([]byte(body))
	bound, ok := sess.Lookup(key, "glm-5.2")
	if !ok {
		t.Fatal("not bound after success")
	}
	// 绑定的号进冷却 → 下次请求必须漂移，且不能报错
	p.Cooldown(bound, pool.CoolPlan, time.Hour, "test")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(body)))
	if rec.Code != 200 {
		t.Fatalf("drift req code=%d body=%s", rec.Code, rec.Body)
	}
	if cur, _ := sess.Lookup(key, "glm-5.2"); cur == bound {
		t.Fatalf("sticky did not drift away from cooling account %s", bound)
	}
	_ = auths
}
