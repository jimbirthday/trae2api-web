package pool

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"trae2api-web/internal/auth"
)

func TestStateRoundTrip(t *testing.T) {
	dir := t.TempDir()
	fp := filepath.Join(dir, "state.json")
	p := New(fp)
	t.Cleanup(p.closeFlusher)
	p.Add(&auth.Auth{UID: "u1"})
	p.SetCredits("u1", 321, 999)
	p.Cooldown("u1", CoolPlan, time.Hour, "plan limit")
	p.NoteUsage("u1", TokenUsageDelta{Model: "m", TotalTokens: 7, HasUsage: true})
	p.CooldownModel("u1", "m1", 30*time.Minute, "limited")
	p.NoteCheckinDone("u1")
	p.Flush()

	p2 := New(fp)
	t.Cleanup(p2.closeFlusher)
	p2.Add(&auth.Auth{UID: "u1"})
	st, ok := p2.Status("u1")
	if !ok {
		t.Fatal("u1 should load from state")
	}
	if st.Credits != 321 || st.CreditsTotal != 999 {
		t.Fatalf("credits lost: %+v", st)
	}
	if !st.Cooling || st.Reason != "plan limit" {
		t.Fatalf("cooldown lost: %+v", st)
	}
	if st.TokenUsage.TotalTokens != 7 || !st.CheckinDone {
		t.Fatalf("usage lost: %+v", st)
	}
	if len(st.RateLimitedModels) != 1 || st.RateLimitedModels[0].Model != "m1" {
		t.Fatalf("model cooldown lost: %+v", st.RateLimitedModels)
	}
	if p2.Pick() != nil {
		t.Fatal("cooling account should stay unpickable after reload")
	}
}

func TestStatePreservesEnabledPausedAndBreaker(t *testing.T) {
	dir := t.TempDir()
	fp := filepath.Join(dir, "state.json")
	p := New(fp)
	t.Cleanup(p.closeFlusher)
	p.Add(&auth.Auth{UID: "u1"})
	p.Add(&auth.Auth{UID: "u2"})
	p.SetEnabled("u1", false, "manual off")
	p.Pause("u2")
	p.NoteError("u1")
	p.NoteError("u1")
	p.NoteError("u1") // 达默认阈值 3 → 熔断
	p.Flush()

	p2 := New(fp)
	t.Cleanup(p2.closeFlusher)
	p2.Add(&auth.Auth{UID: "u1"})
	p2.Add(&auth.Auth{UID: "u2"})
	st1, _ := p2.Status("u1")
	if st1.Enabled || st1.Reason != "manual off" {
		t.Fatalf("enabled flag lost: %+v", st1)
	}
	if st1.BreakerFails != 1 || st1.BreakerUntil.IsZero() {
		t.Fatalf("breaker lost: %+v", st1)
	}
	st2, _ := p2.Status("u2")
	if !st2.Paused {
		t.Fatalf("paused lost: %+v", st2)
	}
}

func TestStateBackwardCompat(t *testing.T) {
	dir := t.TempDir()
	fp := filepath.Join(dir, "state.json")
	old := `{"accounts":{"u1":{"credits":100,"disabled":false,"reason":""}},"unrelated":1}`
	if err := os.WriteFile(fp, []byte(old), 0o600); err != nil {
		t.Fatal(err)
	}
	p := New(fp)
	t.Cleanup(p.closeFlusher)
	p.Add(&auth.Auth{UID: "u1"})
	st, ok := p.Status("u1")
	if !ok || !st.Enabled {
		t.Fatalf("missing enabled should default true: %+v ok=%v", st, ok)
	}
	if st.Credits != 100 {
		t.Fatalf("credits=%d", st.Credits)
	}
	if p.Pick() == nil {
		t.Fatal("old-format account should be pickable")
	}
}

func TestStateFilePermsAndAtomic(t *testing.T) {
	dir := t.TempDir()
	fp := filepath.Join(dir, "sub", "state.json")
	p := New(fp)
	t.Cleanup(p.closeFlusher)
	addAcct(p, "u1", 1)
	p.Flush()
	info, err := os.Stat(fp)
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Fatalf("perm=%o want 600", perm)
	}
	raw, err := os.ReadFile(fp)
	if err != nil {
		t.Fatal(err)
	}
	var sf stateFile
	if err := json.Unmarshal(raw, &sf); err != nil {
		t.Fatalf("state.json invalid: %v", err)
	}
	if _, ok := sf.Accounts["u1"]; !ok {
		t.Fatalf("u1 missing in %s", raw)
	}
	if _, err := os.Stat(fp + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("tmp file left behind: %v", err)
	}
}

func TestRemoveClearsStateEntry(t *testing.T) {
	dir := t.TempDir()
	fp := filepath.Join(dir, "state.json")
	p := New(fp)
	t.Cleanup(p.closeFlusher)
	addAcct(p, "u1", 100)
	addAcct(p, "u2", 50)
	p.Remove("u1")
	p.Flush()

	p2 := New(fp)
	t.Cleanup(p2.closeFlusher)
	if _, ok := p2.Status("u1"); ok {
		t.Fatal("u1 should be gone")
	}
	st, ok := p2.Status("u2")
	if !ok || st.Credits != 50 {
		t.Fatalf("u2 lost: %+v ok=%v", st, ok)
	}
}

func TestCorruptStateFileIgnored(t *testing.T) {
	dir := t.TempDir()
	fp := filepath.Join(dir, "state.json")
	if err := os.WriteFile(fp, []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	p := New(fp)
	t.Cleanup(p.closeFlusher)
	addAcct(p, "u1", 10)
	if p.Pick() == nil {
		t.Fatal("pool should start clean on corrupt state")
	}
	// 损坏文件不被覆盖除非真的有变更落盘；这里显式 Flush 后应可读。
	p.Flush()
	raw, err := os.ReadFile(fp)
	if err != nil {
		t.Fatal(err)
	}
	var sf stateFile
	if err := json.Unmarshal(raw, &sf); err != nil {
		t.Fatalf("state.json should be valid after flush: %v", err)
	}
}
