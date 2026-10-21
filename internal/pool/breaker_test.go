package pool

import (
	"testing"
	"time"

	"trae2api-web/internal/auth"
)

func TestNoteErrorBreaker(t *testing.T) {
	p := newTestPool(t)
	addAcct(p, "u1", 100)
	for i := 0; i < 2; i++ {
		p.NoteError("u1")
		if p.Pick() == nil {
			t.Fatalf("breaker too early at %d", i+1)
		}
	}
	p.NoteError("u1")
	if p.Pick() != nil {
		t.Fatal("3 errors should open breaker")
	}
	st, _ := p.Status("u1")
	if st.BreakerFails != 1 || !st.Cooling {
		t.Fatalf("status=%+v", st)
	}
	if st.ErrTotal != 3 {
		t.Fatalf("err total=%d want 3", st.ErrTotal)
	}
	p.NoteSuccess("u1")
	if p.Pick() == nil {
		t.Fatal("success should close breaker")
	}
	st, _ = p.Status("u1")
	if st.BreakerFails != 0 || st.SuccessCount != 1 {
		t.Fatalf("status after success=%+v", st)
	}
}

func TestNoteErrorBreakerEscalatesAndCaps(t *testing.T) {
	p := newTestPool(t)
	p.SetLimits(Limits{BreakerThresh: 1, BreakerBase: time.Minute, BreakerMax: 2 * time.Minute})
	addAcct(p, "u1", 100)
	p.NoteError("u1")
	st, _ := p.Status("u1")
	first := time.Until(st.BreakerUntil)
	p.NoteSuccess("u1")
	p.NoteError("u1")
	p.NoteError("u1")
	st, _ = p.Status("u1")
	second := time.Until(st.BreakerUntil)
	if second < first {
		t.Fatalf("breaker should escalate: %v -> %v", first, second)
	}
	if second > 2*time.Minute+time.Second {
		t.Fatalf("breaker exceeded cap: %v", second)
	}
}

func TestNoteFailuresDegrade(t *testing.T) {
	p := newTestPool(t)
	addAcct(p, "u1", 100)
	p.NoteFailures("u1")
	if p.Pick() == nil {
		t.Fatal("should still be pickable after 1 failure")
	}
	p.NoteFailures("u1")
	st, _ := p.Status("u1")
	if st.DegradeUntil.IsZero() {
		t.Fatalf("degrade not set: %+v", st)
	}
	if st.ConsecutiveFails != 0 {
		t.Fatalf("fails should reset: %+v", st)
	}
	// 降权只压权重（0.2），不出池：账号仍可被选中、也不该显示为 Cooling。
	if st.Cooling {
		t.Fatalf("degrade must not report as cooling: %+v", st)
	}
	if p.Pick() == nil {
		t.Fatal("degraded account should still be pickable (lower weight only)")
	}
	p.NoteSuccess("u1")
	st, _ = p.Status("u1")
	if !st.DegradeUntil.IsZero() {
		t.Fatalf("success should clear degrade: %+v", st)
	}
}

// 降权中的账号系数 0.2：与正常号共存时应显著更少被选中（但不是 0 次）。
func TestDegradeLowersWeight(t *testing.T) {
	p := newTestPool(t)
	p.Add(&auth.Auth{UID: "bad"})
	p.SetCredits("bad", 1000, 0)
	p.Add(&auth.Auth{UID: "good"})
	p.SetCredits("good", 1000, 0)
	p.NoteFailures("bad")
	p.NoteFailures("bad")
	hits := map[string]int{}
	for i := 0; i < 500; i++ {
		got := p.Pick()
		if got == nil {
			t.Fatal("unexpected nil pick")
		}
		hits[got.UID]++
	}
	if hits["good"] <= hits["bad"]*2 {
		t.Fatalf("degrade should cut weight: %v", hits)
	}
	if hits["bad"] == 0 {
		t.Fatalf("degraded account should not be starved: %v", hits)
	}
}

func TestNoteTransportErrorDoesNotBreak(t *testing.T) {
	p := newTestPool(t)
	addAcct(p, "u1", 100)
	for i := 0; i < 10; i++ {
		p.NoteTransportError("u1")
	}
	if p.Pick() == nil {
		t.Fatal("transport errors must not open breaker")
	}
	st, _ := p.Status("u1")
	if st.ErrTotal != 10 || !st.BreakerUntil.IsZero() {
		t.Fatalf("status=%+v", st)
	}
}
