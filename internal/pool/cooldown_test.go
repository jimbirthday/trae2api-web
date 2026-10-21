package pool

import (
	"testing"
	"time"
)

func TestSoftCooldownBackoffCapped(t *testing.T) {
	p := newTestPool(t)
	p.SetLimits(Limits{SoftRateBase: time.Second, SoftRateMax: 8 * time.Second})
	addAcct(p, "u1", 100)

	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 8 * time.Second}
	for i, w := range want {
		p.CooldownSoftRate("u1", time.Second, time.Time{}, "429")
		st, _ := p.Status("u1")
		if st.SoftStreak != i+1 {
			t.Fatalf("step %d streak=%d", i, st.SoftStreak)
		}
		got := time.Until(st.Until)
		if got > w+300*time.Millisecond || got < w-1200*time.Millisecond {
			t.Fatalf("step %d cooldown=%v want ~%v", i, got, w)
		}
	}
}

func TestSoftCooldownResetAtHonoredButCapped(t *testing.T) {
	p := newTestPool(t)
	p.SetLimits(Limits{SoftRateBase: time.Second, SoftRateMax: 30 * time.Second})
	addAcct(p, "u1", 100)

	p.CooldownSoftRate("u1", time.Second, time.Now().Add(20*time.Second), "429")
	st, _ := p.Status("u1")
	if d := time.Until(st.Until); d < 15*time.Second {
		t.Fatalf("resetAt not honored: %v", d)
	}
	// 超过封顶的 resetAt 应被钳到 SoftRateMax。
	p.CooldownSoftRate("u1", time.Second, time.Now().Add(time.Hour), "429")
	st, _ = p.Status("u1")
	if d := time.Until(st.Until); d > 32*time.Second {
		t.Fatalf("resetAt not capped: %v", d)
	}
}

func TestSoftCooldownBaseZeroUsesDefault(t *testing.T) {
	p := newTestPool(t)
	p.SetLimits(Limits{SoftRateBase: 2 * time.Second, SoftRateMax: time.Minute})
	addAcct(p, "u1", 100)
	p.CooldownSoftRate("u1", 0, time.Time{}, "429")
	st, _ := p.Status("u1")
	if d := time.Until(st.Until); d < 1500*time.Millisecond {
		t.Fatalf("zero base should fall back to SoftRateBase: %v", d)
	}
}

func TestCooldownDoesNotShortenLonger(t *testing.T) {
	p := newTestPool(t)
	addAcct(p, "u1", 100)
	p.Cooldown("u1", CoolPlan, time.Hour, "plan")
	p.Cooldown("u1", CoolSoft, time.Second, "429")
	st, _ := p.Status("u1")
	if d := time.Until(st.Until); d < 50*time.Minute {
		t.Fatalf("long cooldown shortened: %v", d)
	}
}

func TestModelCooldownIsolated(t *testing.T) {
	p := newTestPool(t)
	addAcct(p, "u1", 100)
	p.CooldownModel("u1", "glm-5", time.Hour, "model limited")

	if p.UIDAvailableForModel("u1", "glm-5") {
		t.Fatal("glm-5 should be blocked")
	}
	if !p.UIDAvailableForModel("u1", "glm-4") {
		t.Fatal("other model must stay available")
	}
	if got := p.PickExcludingForModel(nil, "glm-5"); got != nil {
		t.Fatalf("PickExcludingForModel(glm-5)=%+v want nil", got)
	}
	if got := p.PickExcludingForModel(nil, "glm-4"); got == nil || got.UID != "u1" {
		t.Fatalf("PickExcludingForModel(glm-4)=%+v want u1", got)
	}
	if !p.HasRotatable(nil, "glm-4") {
		t.Fatal("HasRotatable(glm-4) should be true")
	}
	if p.HasRotatable(nil, "glm-5") {
		t.Fatal("HasRotatable(glm-5) should be false")
	}
	st, _ := p.Status("u1")
	if st.Cooling {
		t.Fatalf("model cooldown must not cool the account: %+v", st)
	}
	if len(st.RateLimitedModels) != 1 || st.RateLimitedModels[0].Model != "glm-5" {
		t.Fatalf("rate limited models=%+v", st.RateLimitedModels)
	}
}

func TestModelCooldownNotShortened(t *testing.T) {
	p := newTestPool(t)
	addAcct(p, "u1", 100)
	p.CooldownModel("u1", "m", time.Hour, "long")
	p.CooldownModel("u1", "m", time.Minute, "short")
	st, _ := p.Status("u1")
	if len(st.RateLimitedModels) != 1 {
		t.Fatalf("models=%+v", st.RateLimitedModels)
	}
	if d := time.Until(st.RateLimitedModels[0].Until); d < 50*time.Minute {
		t.Fatalf("model cooldown shortened: %v", d)
	}
}

func TestPickByUIDForModelCooledReturnsNil(t *testing.T) {
	p := newTestPool(t)
	addAcct(p, "u1", 100)
	if got := p.PickByUIDForModel("u1", "m1"); got == nil {
		t.Fatal("want account before cooldown")
	}
	p.CooldownModel("u1", "m1", time.Hour, "blocked")
	if got := p.PickByUIDForModel("u1", "m1"); got != nil {
		t.Fatalf("want nil for cooled model, got %+v", got)
	}
	if got := p.PickByUIDForModel("u1", "m2"); got == nil {
		t.Fatal("other model should still resolve")
	}
	if got := p.PickByUIDForModel("nope", "m2"); got != nil {
		t.Fatal("missing uid should be nil")
	}
}

// 规则为空（不调用 CooldownModel）时模型级冷却永不触发。
func TestModelCooldownNeverTriggersWithoutRules(t *testing.T) {
	p := newTestPool(t)
	addAcct(p, "u1", 100)
	for _, m := range []string{"glm-5", "gpt-4o", "", "unknown-model"} {
		if !p.UIDAvailableForModel("u1", m) {
			t.Fatalf("model %q should be available", m)
		}
	}
	if mb := p.ModelBlocked("glm-5"); mb.Blocked {
		t.Fatalf("ModelBlocked without rules: %+v", mb)
	}
}

func TestModelBlockedAggregates(t *testing.T) {
	p := newTestPool(t)
	addAcct(p, "u1", 100)
	addAcct(p, "u2", 100)
	p.CooldownModel("u1", "m", time.Hour, "x")
	if mb := p.ModelBlocked("m"); mb.Blocked {
		t.Fatalf("one of two accounts available, should not be blocked: %+v", mb)
	}
	p.CooldownModel("u2", "m", time.Hour, "x")
	mb := p.ModelBlocked("m")
	if !mb.Blocked || mb.Until.IsZero() {
		t.Fatalf("all accounts cooled, should be blocked: %+v", mb)
	}
}
