package pool

import (
	"sync"
	"testing"
	"time"

	"trae2api-web/internal/auth"
)

func TestSetCreditsWritesBack(t *testing.T) {
	p := newTestPool(t)
	addAcct(p, "u1", 0)
	p.SetCredits("u1", 777, 1000)
	st, _ := p.Status("u1")
	if st.Credits != 777 || st.CreditsTotal != 1000 {
		t.Fatalf("credits not written: %+v", st)
	}
	p.SetCreditsDetailed("u1", 42, 2000, 10, time.Now().Add(time.Hour), 10)
	st, _ = p.Status("u1")
	if st.Credits != 42 || st.CreditsTotal != 2000 {
		t.Fatalf("detailed credits not written: %+v", st)
	}
}

func TestNoteUsageAccumulates(t *testing.T) {
	p := newTestPool(t)
	addAcct(p, "u1", 100)
	p.NoteUsage("u1", TokenUsageDelta{Model: "m", PromptTokens: 10, CompletionTokens: 5, TotalTokens: 15, LatencyMs: 120, HasUsage: true})
	p.NoteUsage("u1", TokenUsageDelta{Model: "m2", LatencyMs: 80})
	st, _ := p.Status("u1")
	if st.TokenUsage.RequestCount != 2 || st.TokenUsage.TotalTokens != 15 {
		t.Fatalf("usage=%+v", st.TokenUsage)
	}
	if st.TokenUsage.LastModel != "m2" || st.TokenUsage.LastLatencyMs != 80 {
		t.Fatalf("usage last=%+v", st.TokenUsage)
	}
	if st.Credits != 100 {
		t.Fatalf("NoteUsage must not touch credits: %d", st.Credits)
	}
}

func TestReenableIfCredits(t *testing.T) {
	p := newTestPool(t)
	addAcct(p, "u1", 0)
	p.Cooldown("u1", CoolPlan, time.Hour, "plan")
	p.ReenableIfCredits("u1", 500)
	if p.Pick() == nil {
		t.Fatal("should re-enable with credits")
	}
	p.Cooldown("u1", CoolPlan, time.Hour, "plan")
	p.ReenableIfCredits("u1", 0)
	if p.Pick() != nil {
		t.Fatal("zero credits should stay cooling")
	}
}

func TestReenableDoesNotTouchDisabled(t *testing.T) {
	p := newTestPool(t)
	addAcct(p, "u1", 0)
	p.Disable("u1", "session dead")
	p.ReenableIfCredits("u1", 500)
	if p.Pick() != nil {
		t.Fatal("disabled must not auto-reenable")
	}
}

func TestRevive(t *testing.T) {
	p := newTestPool(t)
	addAcct(p, "u1", 100)
	p.Disable("u1", "dead")
	if p.Pick() != nil {
		t.Fatal("disabled must be skipped")
	}
	if !p.Revive("u1") {
		t.Fatal("revive existing should return true")
	}
	if p.Pick() == nil {
		t.Fatal("revived account should be pickable")
	}
	if p.Revive("nope") {
		t.Fatal("revive missing should return false")
	}
}

func TestSetEnabledSoftSwitch(t *testing.T) {
	p := newTestPool(t)
	addAcct(p, "u1", 100)
	if !p.SetEnabled("u1", false, "user paused") {
		t.Fatal("set enabled should return true")
	}
	if p.Pick() != nil {
		t.Fatal("disabled-by-user account should be skipped")
	}
	st, _ := p.Status("u1")
	if st.Enabled || st.Disabled || st.Reason != "user paused" {
		t.Fatalf("status=%+v", st)
	}
	p.SetEnabled("u1", true, "")
	if p.Pick() == nil {
		t.Fatal("re-enabled account should be pickable")
	}
	if p.SetEnabled("nope", false, "x") {
		t.Fatal("set enabled on missing should be false")
	}
}

func TestPauseResume(t *testing.T) {
	p := newTestPool(t)
	addAcct(p, "u1", 100)
	if !p.Pause("u1") {
		t.Fatal("pause")
	}
	if p.Pick() != nil {
		t.Fatal("paused should be skipped")
	}
	if st, _ := p.Status("u1"); !st.Paused {
		t.Fatalf("paused flag not set: %+v", st)
	}
	if !p.Resume("u1") {
		t.Fatal("resume")
	}
	if p.Pick() == nil {
		t.Fatal("resumed should be pickable")
	}
}

func TestListCountsAndStatus(t *testing.T) {
	p := newTestPool(t)
	p.Add(&auth.Auth{UID: "a", Nickname: "nick-a"})
	p.Add(&auth.Auth{UID: "b"})
	p.Add(&auth.Auth{UID: "c"})
	p.Add(&auth.Auth{UID: "d"})
	p.SetCredits("a", 42, 100)
	p.Cooldown("b", CoolSoft, time.Minute, "429")
	p.Disable("c", "dead")
	p.Pause("d")

	list := p.List()
	if len(list) != 4 || list[0].UID != "a" || list[3].UID != "d" {
		t.Fatalf("list not sorted: %+v", list)
	}
	st, _ := p.Status("a")
	if st.Credits != 42 || st.Nickname != "nick-a" || st.Cooling || st.Disabled {
		t.Fatalf("a=%+v", st)
	}
	if st.Weight != 1 || !st.HasTokens {
		// Auth 无 token 时 HasTokens=false，这里仅断言 Weight 归一化。
		if st.Weight != 1 {
			t.Fatalf("weight not normalized: %+v", st)
		}
	}
	st, _ = p.Status("b")
	if !st.Cooling || st.CoolKind != "soft_rate" || st.Reason != "429" {
		t.Fatalf("b=%+v", st)
	}
	st, _ = p.Status("c")
	if !st.Disabled || st.DisabledReason != "dead" {
		t.Fatalf("c=%+v", st)
	}
	cnt := p.Counts()
	if cnt.Total != 4 || cnt.Healthy != 1 || cnt.Cooling != 1 || cnt.Disabled != 1 || cnt.Paused != 1 {
		t.Fatalf("counts=%+v", cnt)
	}
	if _, ok := p.Status("missing"); ok {
		t.Fatal("missing status should be false")
	}
}

func TestAuthByUIDAndAvailable(t *testing.T) {
	p := newTestPool(t)
	addAcct(p, "u1", 100)
	addAcct(p, "u2", 100)
	p.Cooldown("u2", CoolPlan, time.Hour, "plan")
	if a := p.AuthByUID("u1"); a == nil || a.UID != "u1" {
		t.Fatalf("auth=%+v", a)
	}
	if a := p.AuthByUID("nope"); a != nil {
		t.Fatalf("auth for missing=%+v", a)
	}
	uids := p.AvailableUIDs()
	if len(uids) != 1 || uids[0] != "u1" {
		t.Fatalf("available=%+v", uids)
	}
}

func TestSyncToDirAndRemove(t *testing.T) {
	p := newTestPool(t)
	addAcct(p, "u1", 100)
	addAcct(p, "u2", 50)
	p.SyncToDir([]*auth.Auth{{UID: "u2"}})
	if _, ok := p.Status("u1"); ok {
		t.Fatal("u1 should be removed")
	}
	if !p.Remove("u2") {
		t.Fatal("remove existing should be true")
	}
	if p.Remove("u2") {
		t.Fatal("remove missing should be false")
	}
}

func TestNoteCheckinDone(t *testing.T) {
	p := newTestPool(t)
	addAcct(p, "u1", 100)
	p.NoteCheckinDone("u1")
	if st, _ := p.Status("u1"); !st.CheckinDone {
		t.Fatalf("checkin not recorded: %+v", st)
	}
}

// 并发选号 + 在途：不 panic、不超限、无数据竞争（配合 -race）。
func TestConcurrentPickAndAcquireRace(t *testing.T) {
	p := newTestPool(t)
	p.SetLimits(Limits{MaxInFlight: 4})
	for _, uid := range []string{"a", "b", "c", "d", "e"} {
		addAcct(p, uid, 100)
	}
	var wg sync.WaitGroup
	for i := 0; i < 60; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 30; j++ {
				a := p.Pick()
				if a == nil {
					continue
				}
				if p.Acquire(a.UID) {
					_, _ = p.Status(a.UID)
					p.Release(a.UID)
				}
				_ = p.List()
				_ = p.Counts()
			}
		}()
	}
	wg.Wait()
	for _, uid := range []string{"a", "b", "c", "d", "e"} {
		if n := p.InFlight(uid); n != 0 {
			t.Fatalf("%s leaked in-flight=%d", uid, n)
		}
	}
}

// 人工复活必须清掉冷却/熔断/降权，否则面板点「复活」后账号仍选不中。
func TestReviveClearsAllBlockingState(t *testing.T) {
	p := newTestPool(t)
	addAcct(p, "u1", 100)
	p.Cooldown("u1", CoolPlan, time.Hour, "plan")
	p.NoteError("u1")
	p.NoteError("u1")
	p.NoteError("u1") // 触发熔断
	p.NoteFailures("u1")
	p.NoteFailures("u1") // 触发降权
	p.Pause("u1")
	p.Disable("u1", "session dead")

	if p.Pick() != nil {
		t.Fatal("account should be unavailable before revive")
	}
	if !p.Revive("u1") {
		t.Fatal("revive failed")
	}
	st, _ := p.Status("u1")
	if st.Cooling || st.Disabled || st.Paused || !st.BreakerUntil.IsZero() || !st.DegradeUntil.IsZero() {
		t.Fatalf("revive left blocking state: %+v", st)
	}
	if p.Pick() == nil {
		t.Fatal("account should be routable right after revive")
	}
}
