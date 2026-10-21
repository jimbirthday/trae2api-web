package session

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestConcurrentSameKeyBindsOnce(t *testing.T) {
	var mu sync.Mutex
	calls := 0
	avail := []string{"a", "b", "c", "d"}
	r := New(Config{
		TTL: time.Minute,
		Available: func() []string {
			mu.Lock()
			calls++
			mu.Unlock()
			return avail
		},
	})
	var wg sync.WaitGroup
	got := make([]string, 64)
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			uid, ok := r.Resolve("same-session")
			if !ok {
				t.Error("resolve failed")
				return
			}
			got[i] = uid
		}(i)
	}
	wg.Wait()
	for _, uid := range got {
		if uid != got[0] {
			t.Fatalf("same key bound to multiple accounts: %q vs %q", uid, got[0])
		}
	}
	if n := r.Count(); n != 1 {
		t.Fatalf("count=%d want 1", n)
	}
	// 每个账号最多一个绑定，说明分配没有再重复占用。
	for _, uid := range avail {
		if c := r.BoundCount(uid); c > 1 {
			t.Fatalf("uid %s bound %d times", uid, c)
		}
	}
}

func TestTTLExpiry(t *testing.T) {
	r := New(Config{TTL: 20 * time.Millisecond, GCInterval: time.Hour, Available: func() []string { return []string{"a", "b"} }})
	first, ok := r.Resolve("k")
	if !ok {
		t.Fatal("resolve failed")
	}
	time.Sleep(40 * time.Millisecond)
	second, ok := r.Resolve("k")
	if !ok {
		t.Fatal("resolve after expiry failed")
	}
	_ = first
	_ = second
	// 过期后必须重新绑定：Count 仍为 1，但旧绑定已被清理（boundUIDs 不泄漏）。
	r.mu.RLock()
	bound := len(r.boundUIDs)
	r.mu.RUnlock()
	if bound != 1 {
		t.Fatalf("boundUIDs=%d want 1 (stale binding leaked)", bound)
	}
}

func TestGCExpiresIdleBindings(t *testing.T) {
	r := New(Config{TTL: 10 * time.Millisecond, GCInterval: 5 * time.Millisecond, Available: func() []string { return []string{"a"} }})
	r.StartGC()
	defer r.StopGC()
	if _, ok := r.Resolve("k"); !ok {
		t.Fatal("resolve failed")
	}
	deadline := time.Now().Add(time.Second)
	for r.Count() > 0 {
		if time.Now().After(deadline) {
			t.Fatal("GC did not expire binding")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestStartStopGCIdempotent(t *testing.T) {
	r := New(Config{GCInterval: time.Millisecond, Available: func() []string { return []string{"a"} }})
	r.StartGC()
	r.StartGC()
	r.StopGC()
	r.StopGC()
	// 再次启动必须可用。
	r.StartGC()
	r.StopGC()
}

func TestMaxBindingsEviction(t *testing.T) {
	r := New(Config{TTL: time.Hour, MaxBindings: 10, Available: func() []string { return []string{"a"} }})
	for i := 0; i < 40; i++ {
		r.Bind(fmt.Sprintf("k%d", i), "a")
		if n := r.Count(); n > 10 {
			t.Fatalf("bindings=%d exceeded max 10", n)
		}
	}
	if n := r.Count(); n == 0 {
		t.Fatal("all bindings evicted")
	}
	// 计数与条目必须同步，否则空闲判定会算错。
	r.mu.RLock()
	total := 0
	for _, c := range r.boundUIDs {
		total += c
	}
	r.mu.RUnlock()
	if total != r.Count() {
		t.Fatalf("boundUIDs total=%d count=%d mismatch", total, r.Count())
	}
}

func TestResolveForModelUnbindsWhenUnavailable(t *testing.T) {
	allowed := map[string]bool{"a": true}
	r := New(Config{
		TTL:               time.Hour,
		Available:         func() []string { return []string{"a", "b"} },
		AvailableForModel: func(model string) []string { return []string{"a", "b"} },
		CanRoute:          func(model, uid string) bool { return allowed[uid] },
	})
	uid, ok := r.ResolveForModel("s", "m1")
	if !ok || uid != "a" {
		t.Fatalf("resolve=%q,%v want a", uid, ok)
	}
	allowed["a"] = false
	if _, ok := r.ResolveForModel("s", "m1"); ok {
		t.Fatal("expected miss after account became unroutable")
	}
	if r.Count() != 0 {
		t.Fatalf("binding not removed, count=%d", r.Count())
	}
}

func TestResolveForModelFallsBackToAvailableForModel(t *testing.T) {
	r := New(Config{
		TTL:               time.Hour,
		AvailableForModel: func(model string) []string { return []string{"a", "b"} },
	})
	uid, ok := r.ResolveForModel("s", "m")
	if !ok || uid == "" {
		t.Fatalf("resolve failed: %q", uid)
	}
	r2 := New(Config{
		TTL:               time.Hour,
		AvailableForModel: func(model string) []string { return nil },
	})
	if _, ok := r2.ResolveForModel(uid, "m-other"); ok {
		t.Fatal("expected miss when model has no candidates")
	}
}

func TestResolveNoAvailabilityCallbacksDoesNotValidate(t *testing.T) {
	r := New(Config{TTL: time.Hour})
	r.Bind("s", "a")
	if uid, ok := r.Resolve("s"); !ok || uid != "a" {
		t.Fatalf("resolve=%q,%v want a,true (nil callbacks must not invalidate)", uid, ok)
	}
	if uid, ok := r.ResolveForModel("s", "any"); !ok || uid != "a" {
		t.Fatalf("ResolveForModel=%q,%v want a,true", uid, ok)
	}
	// 无候选来源时不能凭空分配。
	if _, ok := r.Resolve("new"); ok {
		t.Fatal("allocated without any candidate source")
	}
}

func TestEmptyKeyNeverBinds(t *testing.T) {
	r := New(Config{TTL: time.Hour, Available: func() []string { return []string{"a"} }})
	if _, ok := r.Resolve(""); ok {
		t.Fatal("empty key resolved")
	}
	if r.Count() != 0 {
		t.Fatal("empty key created binding")
	}
}

func TestExtractKeyPriorities(t *testing.T) {
	cases := []struct {
		name string
		body string
		want string
	}{
		// user 字段不再直接当键：必须与内容一起 hash，否则同一客户端复用的 user 值
		// 会让不同对话互相抢绑定。
		{"user field", `{"user":"u-1","messages":[{"role":"user","content":"hi"}]}`, ExtractKeyFromFields(KeyFields{User: "u-1", FirstUser: "hi"})},
		{"empty body", ``, ""},
		{"invalid json", `{`, ""},
		{"no messages", `{"model":"x"}`, ""},
	}
	for _, c := range cases {
		if got := ExtractKey([]byte(c.body)); got != c.want {
			t.Fatalf("%s: got %q want %q", c.name, got, c.want)
		}
	}

	first := ExtractKey([]byte(`{"messages":[{"role":"system","content":"S"},{"role":"user","content":"first"}]}`))
	second := ExtractKey([]byte(`{"messages":[{"role":"system","content":"S"},{"role":"user","content":"first"},{"role":"user","content":"second"}]}`))
	if first == "" || first != second {
		t.Fatalf("first-user key must ignore later messages: %q vs %q", first, second)
	}

	// 内容变化必须换键。
	if a, b := ExtractKey([]byte(`{"messages":[{"role":"user","content":"a"}]}`)),
		ExtractKey([]byte(`{"messages":[{"role":"user","content":"b"}]}`)); a == b {
		t.Fatal("different first user content produced same key")
	}

	// content 为 parts 数组时取 text 片段。
	parts := `{"messages":[{"role":"user","content":[{"type":"text","text":"hello"},{"type":"image_url","image_url":{"url":"x"}}]}]}`
	plain := `{"messages":[{"role":"user","content":"hello"}]}`
	if ExtractKey([]byte(parts)) != ExtractKey([]byte(plain)) {
		t.Fatal("parts text extraction mismatch")
	}

	// 无 user 文本时回落全量 messages 签名。
	full := ExtractKey([]byte(`{"messages":[{"role":"assistant","content":"only assistant"}]}`))
	if full == "" {
		t.Fatal("expected fallback hash for messages without user text")
	}
}

// 键必须有足够熵：不能是固定常量，也不能让不同会话撞到同一个键。
func TestExtractKeyHasNoConstantCollision(t *testing.T) {
	// 无任何可推导信号时必须返回空串（不粘），而不是 "default" 这类共享常量。
	if got := ExtractKey([]byte(`{"model":"x"}`)); got != "" {
		t.Fatalf("no signal should yield empty key, got %q", got)
	}
	if got := ExtractKey([]byte(`{"messages":[]}`)); got != "" {
		t.Fatalf("empty messages should yield empty key, got %q", got)
	}
	// 同一 user 值 + 不同对话内容 → 不同键。
	a := ExtractKey([]byte(`{"user":"u","messages":[{"role":"user","content":"topic A"}]}`))
	b := ExtractKey([]byte(`{"user":"u","messages":[{"role":"user","content":"topic B"}]}`))
	if a == "" || b == "" || a == b {
		t.Fatalf("same user different topic collided: %q vs %q", a, b)
	}
	// conversationId 优先，且不同会话不撞。
	c1 := ExtractKey([]byte(`{"metadata":{"conversation_id":"c1"},"messages":[{"role":"user","content":"x"}]}`))
	c2 := ExtractKey([]byte(`{"metadata":{"conversation_id":"c2"},"messages":[{"role":"user","content":"x"}]}`))
	if c1 == "" || c2 == "" || c1 == c2 {
		t.Fatalf("different conversations collided: %q vs %q", c1, c2)
	}
}

func TestResolveConversationID(t *testing.T) {
	body := []byte(`{"metadata":{"conversationId":"c-1"}}`)
	if got := ResolveConversationID(body); got != "c-1" {
		t.Fatalf("got %q", got)
	}
	if got := ResolveConversationID([]byte(`{"user":"u"}`)); got != "" {
		t.Fatalf("must not fall back to user: %q", got)
	}
}

func TestLastActiveTouchKeepsSessionAlive(t *testing.T) {
	r := New(Config{TTL: 80 * time.Millisecond, GCInterval: time.Hour, Available: func() []string { return []string{"a", "b"} }})
	first, _ := r.Resolve("k")
	for i := 0; i < 6; i++ {
		time.Sleep(20 * time.Millisecond)
		uid, ok := r.Resolve("k")
		if !ok || uid != first {
			t.Fatalf("touch lost binding: %q want %q", uid, first)
		}
	}
}

func TestBindUnbind(t *testing.T) {
	r := New(Config{TTL: time.Hour})
	r.Bind("k", "a")
	r.Bind("k", "b") // 覆盖
	if uid, ok := r.Resolve("k"); !ok || uid != "b" {
		t.Fatalf("got %q,%v want b", uid, ok)
	}
	if !r.Unbind("k") || r.Unbind("k") {
		t.Fatal("Unbind must report existence exactly once")
	}
	if r.Count() != 0 {
		t.Fatalf("count=%d", r.Count())
	}
}

func BenchmarkResolveHit(b *testing.B) {
	avail := make([]string, 470)
	for i := range avail {
		avail[i] = fmt.Sprintf("u%d", i)
	}
	r := New(Config{TTL: time.Hour, Available: func() []string { return avail }})
	if _, ok := r.Resolve("s"); !ok {
		b.Fatal("warmup failed")
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := r.Resolve("s"); !ok {
			b.Fatal("miss")
		}
	}
}

// TestGCNoGoroutineLeak 验证 StopGC 后 GC goroutine 真正退出（不依赖第三方 leak 检测）。
func TestGCNoGoroutineLeak(t *testing.T) {
	r := New(Config{GCInterval: time.Millisecond, Available: func() []string { return []string{"a"} }})
	before := runtime.NumGoroutine()
	for i := 0; i < 20; i++ {
		r.StartGC()
		r.StopGC()
	}
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before+2 {
		if time.Now().After(deadline) {
			t.Fatalf("goroutines=%d before=%d (GC goroutine leaked)", runtime.NumGoroutine(), before)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestConcurrentBindAndResolve 混合并发下 boundUIDs 与 items 必须一致。
func TestConcurrentBindAndResolve(t *testing.T) {
	r := New(Config{TTL: time.Hour, Available: func() []string { return []string{"a", "b", "c"} }})
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				key := fmt.Sprintf("k%d-%d", i, j%7)
				if j%3 == 0 {
					r.Bind(key, "a")
				} else if j%11 == 0 {
					r.Unbind(key)
				} else {
					r.Resolve(key)
				}
			}
		}(i)
	}
	wg.Wait()
	r.mu.RLock()
	total := 0
	for _, c := range r.boundUIDs {
		total += c
	}
	items := len(r.items)
	r.mu.RUnlock()
	if total != items {
		t.Fatalf("boundUIDs total=%d items=%d mismatch", total, items)
	}
}
