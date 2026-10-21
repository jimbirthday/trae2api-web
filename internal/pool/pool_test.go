package pool

import (
	"sync"
	"testing"
	"time"

	"trae2api-web/internal/auth"
)

func newTestPool(t *testing.T) *Pool {
	t.Helper()
	p := New("")
	t.Cleanup(p.closeFlusher)
	p.SetLimits(Limits{
		MaxInFlight:   2,
		SoftRateBase:  time.Second,
		SoftRateMax:   10 * time.Second,
		BreakerThresh: 3,
		BreakerBase:   time.Minute,
		BreakerMax:    10 * time.Minute,
		DegradeThresh: 2,
		DegradeWindow: time.Minute,
	})
	return p
}

func addAcct(p *Pool, uid string, credits int64) {
	p.Add(&auth.Auth{UID: uid})
	p.SetCredits(uid, credits, 0)
}

// --- 选号 ---

func TestPickReturnsAvailable(t *testing.T) {
	p := newTestPool(t)
	addAcct(p, "u1", 100)
	if got := p.Pick(); got == nil || got.UID != "u1" {
		t.Fatalf("pick=%+v want u1", got)
	}
}

func TestPickSkipsCoolingDisabledPaused(t *testing.T) {
	p := newTestPool(t)
	addAcct(p, "cool", 100)
	addAcct(p, "dis", 100)
	addAcct(p, "pau", 100)
	addAcct(p, "ok", 100)
	p.Cooldown("cool", CoolPlan, time.Hour, "plan")
	p.Disable("dis", "session dead")
	p.Pause("pau")
	for i := 0; i < 20; i++ {
		got := p.Pick()
		if got == nil || got.UID != "ok" {
			t.Fatalf("pick=%+v want ok", got)
		}
	}
}

func TestPickExcluding(t *testing.T) {
	p := newTestPool(t)
	addAcct(p, "u1", 100)
	addAcct(p, "u2", 100)
	got := p.PickExcluding(map[string]bool{"u1": true})
	if got == nil || got.UID != "u2" {
		t.Fatalf("pick=%+v want u2", got)
	}
	if got := p.PickExcluding(map[string]bool{"u1": true, "u2": true}); got != nil {
		t.Fatalf("want nil, got %+v", got)
	}
}

func TestPickExpiredCooldownReturns(t *testing.T) {
	p := newTestPool(t)
	addAcct(p, "u1", 100)
	p.Cooldown("u1", CoolSoft, time.Millisecond, "429")
	time.Sleep(5 * time.Millisecond)
	if got := p.Pick(); got == nil || got.UID != "u1" {
		t.Fatalf("pick=%+v want u1 after cooldown expiry", got)
	}
}

// 加权不会长期只打一个号：两个同质账号应都被选中。
func TestPickSpreadsAcrossAccounts(t *testing.T) {
	p := newTestPool(t)
	addAcct(p, "u1", 1000)
	addAcct(p, "u2", 1000)
	hits := map[string]int{}
	for i := 0; i < 400; i++ {
		a := p.Pick()
		if a == nil {
			t.Fatal("unexpected nil pick")
		}
		hits[a.UID]++
	}
	if hits["u1"] == 0 || hits["u2"] == 0 {
		t.Fatalf("weighted pick never hit one account: %v", hits)
	}
	for uid, n := range hits {
		if n < 40 {
			t.Fatalf("account %s starved: %v", uid, hits)
		}
	}
}

// credits 影响选号：同权重下高余额应显著更常被选中。
func TestCreditsInfluencePick(t *testing.T) {
	p := newTestPool(t)
	addAcct(p, "rich", 100000)
	addAcct(p, "poor", 10)
	hits := map[string]int{}
	for i := 0; i < 600; i++ {
		hits[p.Pick().UID]++
	}
	if hits["rich"] <= hits["poor"]*2 {
		t.Fatalf("credits should bias pick: %v", hits)
	}
}

// 手动权重生效。
func TestManualWeightInfluence(t *testing.T) {
	p := newTestPool(t)
	p.Add(&auth.Auth{UID: "heavy", Weight: 100})
	p.Add(&auth.Auth{UID: "light", Weight: 1})
	p.SetCredits("heavy", 1000, 0)
	p.SetCredits("light", 1000, 0)
	hits := map[string]int{}
	for i := 0; i < 400; i++ {
		hits[p.Pick().UID]++
	}
	if hits["heavy"] <= hits["light"] {
		t.Fatalf("manual weight ignored: %v", hits)
	}
}

// --- 在途租约 ---

func TestInFlightLimitConcurrent(t *testing.T) {
	p := newTestPool(t)
	p.SetLimits(Limits{MaxInFlight: 3})
	addAcct(p, "u1", 100)

	var wg sync.WaitGroup
	var mu sync.Mutex
	ok := 0
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if p.Acquire("u1") {
				mu.Lock()
				ok++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if ok != 3 {
		t.Fatalf("acquired=%d want 3", ok)
	}
	if got := p.InFlight("u1"); got != 3 {
		t.Fatalf("inflight=%d want 3", got)
	}
}

func TestReleaseIdempotentAndClamps(t *testing.T) {
	p := newTestPool(t)
	addAcct(p, "u1", 100)
	if !p.Acquire("u1") {
		t.Fatal("acquire failed")
	}
	p.Release("u1")
	p.Release("u1")
	p.Release("u1")
	if got := p.InFlight("u1"); got != 0 {
		t.Fatalf("inflight=%d want 0", got)
	}
	if p.Acquire("missing") {
		t.Fatal("acquire on missing should be false")
	}
}

func TestAcquireUnlimitedWhenZero(t *testing.T) {
	p := newTestPool(t)
	p.SetLimits(Limits{MaxInFlight: 0})
	addAcct(p, "u1", 100)
	for i := 0; i < 50; i++ {
		if !p.Acquire("u1") {
			t.Fatalf("acquire #%d should succeed when unlimited", i)
		}
	}
	// 负值同样表示不限。
	p.SetLimits(Limits{MaxInFlight: -1})
	if !p.Acquire("u1") {
		t.Fatal("negative MaxInFlight should mean unlimited")
	}
}

func TestPickSkipsFullInFlight(t *testing.T) {
	p := newTestPool(t)
	p.SetLimits(Limits{MaxInFlight: 1})
	addAcct(p, "u1", 100)
	addAcct(p, "u2", 100)
	if !p.Acquire("u1") {
		t.Fatal("acquire u1")
	}
	for i := 0; i < 20; i++ {
		if got := p.Pick(); got == nil || got.UID != "u2" {
			t.Fatalf("pick=%+v want u2 (u1 full)", got)
		}
	}
}
