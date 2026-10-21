package pool

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"trae2api-web/internal/auth"
)

// ModelBlocked 的分母必须是「本来能接流量的号」：paused / 余额地板 / 账号级冷却
// 中的号对任何模型都不可用，算进分母会造成「全池该模型被封」的误报。
func TestModelBlockedExcludesPausedAndFloor(t *testing.T) {
	p := newTestPool(t)
	p.SetLimits(Limits{CreditFloor: 100})
	addAcct(p, "u1", 500)
	addAcct(p, "paused", 500)
	addAcct(p, "poor", 10) // 低于地板：本来就不参与选号

	p.CooldownModel("u1", "m", time.Hour, "x")
	p.Pause("paused")

	mb := p.ModelBlocked("m")
	if !mb.Blocked {
		t.Fatalf("only routable account is model-cooled → should be blocked: %+v", mb)
	}

	// 加一个可用号后立刻解除。
	addAcct(p, "u2", 500)
	if mb := p.ModelBlocked("m"); mb.Blocked {
		t.Fatalf("u2 is available, must not report blocked: %+v", mb)
	}
}

// 全池真不可用（没有可路由号）时不能报「模型被封」——那是「无号可用」。
func TestModelBlockedFalseWhenNoRoutableAccounts(t *testing.T) {
	p := newTestPool(t)
	p.SetLimits(Limits{CreditFloor: 100})
	addAcct(p, "poor", 1)
	if mb := p.ModelBlocked("m"); mb.Blocked {
		t.Fatalf("no routable accounts at all must not report model blocked: %+v", mb)
	}
}

// HasRotatable 必须与主选号口径一致（含余额地板）。
func TestHasRotatableRespectsCreditFloor(t *testing.T) {
	p := newTestPool(t)
	p.SetLimits(Limits{CreditFloor: 100})
	addAcct(p, "poor", 5)
	if p.HasRotatable(nil, "") {
		t.Fatal("credit-floor account must not count as rotatable")
	}
	addAcct(p, "rich", 500)
	if !p.HasRotatable(nil, "") {
		t.Fatal("rich account should be rotatable")
	}
}

// 粘性命中路径同样受余额地板约束，且必须更新 lastUsed（它也是真实流量）。
func TestPickByUIDUpdatesLastUsedAndRespectsFloor(t *testing.T) {
	p := newTestPool(t)
	p.SetLimits(Limits{CreditFloor: 100})
	addAcct(p, "u1", 500)
	addAcct(p, "u2", 500)

	p.mu.Lock()
	before := p.byUID["u1"].lastUsed
	seqBefore := p.byUID["u1"].usedSeq
	p.mu.Unlock()
	if !before.IsZero() || seqBefore != 0 {
		t.Fatal("precondition: account should be unused")
	}
	if p.PickByUIDForModel("u1", "") == nil {
		t.Fatal("sticky pick should succeed")
	}
	p.mu.RLock()
	after := p.byUID["u1"].lastUsed
	seq := p.byUID["u1"].usedSeq
	p.mu.RUnlock()
	if after.IsZero() || seq == 0 {
		t.Fatal("sticky path must commit lastUsed/usedSeq")
	}

	// 掉到地板以下后粘性命中必须失败（否则会挑出主路径不会选的号）。
	p.SetCredits("u1", 1, 0)
	if p.PickByUIDForModel("u1", "") != nil {
		t.Fatal("sticky pick must respect credit floor")
	}
}

// 换凭证重新导入：清自动锁，保留用量与余额。
func TestAddWithNewCredentialsResetsLocksKeepsUsage(t *testing.T) {
	p := newTestPool(t)
	old := &auth.Auth{UID: "u1", AccessToken: "at-old", RefreshToken: "rt-old"}
	p.Add(old)
	p.SetCredits("u1", 321, 4800)
	p.NoteUsage("u1", TokenUsageDelta{Model: "m", TotalTokens: 42, HasUsage: true})
	p.Cooldown("u1", CoolPlan, 12*time.Hour, "plan")
	p.NoteError("u1")
	p.NoteError("u1")
	p.NoteError("u1")
	p.SetEnabled("u1", false, "manual off")

	p.Add(&auth.Auth{UID: "u1", AccessToken: "at-new", RefreshToken: "rt-new"})

	p.mu.RLock()
	e := p.byUID["u1"]
	cfg := struct {
		until        time.Time
		coolKind     string
		breakerUntil time.Time
		breakerFails int
		errCount     int
	}{e.until, e.coolKind, e.breakerUntil, e.breakerFails, e.errCount}
	p.mu.RUnlock()
	if !cfg.until.IsZero() || cfg.coolKind != "" || !cfg.breakerUntil.IsZero() ||
		cfg.breakerFails != 0 || cfg.errCount != 0 {
		t.Fatalf("new credentials should reset auto locks: %+v", cfg)
	}

	st, _ := p.Status("u1")
	if st.TokenUsage.TotalTokens != 42 || st.Credits != 321 {
		t.Fatalf("usage/credits must survive re-import: %+v", st)
	}
	if st.Enabled {
		t.Fatal("manual enable/disable intent must not be overridden by re-import")
	}
	if e.a.AccessTokenValue() != "at-new" {
		t.Fatalf("credentials not replaced: %s", e.a.AccessTokenValue())
	}
}

// 启动恢复（占位 Auth 无凭证 → 真实凭证）不算换号，不得洗白状态。
func TestAddWithSameCredentialsKeepsState(t *testing.T) {
	dir := t.TempDir()
	fp := filepath.Join(dir, "state.json")
	p := New(fp)
	t.Cleanup(p.closeFlusher)
	p.Add(&auth.Auth{UID: "u1", AccessToken: "at", RefreshToken: "rt"})
	p.Cooldown("u1", CoolPlan, 12*time.Hour, "plan")
	p.Flush()

	p2 := New(fp)
	t.Cleanup(p2.closeFlusher)
	p2.Add(&auth.Auth{UID: "u1", AccessToken: "at", RefreshToken: "rt"})
	if p2.Pick() != nil {
		t.Fatal("restart with identical credentials must keep cooldown")
	}
}

// Disable 必须让 Enabled 变 false（对外契约里两者不能自相矛盾）。
func TestDisableClearsEnabled(t *testing.T) {
	p := newTestPool(t)
	addAcct(p, "u1", 100)
	p.Disable("u1", "session dead")
	st, _ := p.Status("u1")
	if !st.Disabled || st.Enabled {
		t.Fatalf("disable should clear enabled: %+v", st)
	}
}

// Revive 不得解除人工 Pause（自动状态不能覆盖人的意图）。
func TestReviveKeepsManualPause(t *testing.T) {
	p := newTestPool(t)
	addAcct(p, "u1", 100)
	p.Pause("u1")
	p.Disable("u1", "session dead")
	if !p.Revive("u1") {
		t.Fatal("revive failed")
	}
	st, _ := p.Status("u1")
	if st.Disabled {
		t.Fatalf("revive should clear disabled: %+v", st)
	}
	if !st.Paused {
		t.Fatalf("revive must not clear manual pause: %+v", st)
	}
	if p.Pick() != nil {
		t.Fatal("paused account must not be routable after revive")
	}
}

// 持久化补字段：errCount/lastUsed/usedSeq/lastSuccess/lastErr 必须能跨重启保留。
func TestStatePersistsFailureAndUsageSignals(t *testing.T) {
	dir := t.TempDir()
	fp := filepath.Join(dir, "state.json")
	p := New(fp)
	t.Cleanup(p.closeFlusher)
	p.Add(&auth.Auth{UID: "u1", AccessToken: "at", RefreshToken: "rt"})
	p.NoteError("u1") // errCount=1（未达熔断阈值）
	p.Add(&auth.Auth{UID: "u2", AccessToken: "at2", RefreshToken: "rt2"})
	if p.Pick() == nil {
		t.Fatal("pick failed")
	}
	p.mu.RLock()
	var usedUID string
	var seq uint64
	for uid, e := range p.byUID {
		if !e.lastUsed.IsZero() {
			usedUID, seq = uid, e.usedSeq
		}
	}
	p.mu.RUnlock()
	p.Flush()

	p2 := New(fp)
	t.Cleanup(p2.closeFlusher)
	p2.Add(&auth.Auth{UID: "u1", AccessToken: "at", RefreshToken: "rt"})
	p2.Add(&auth.Auth{UID: "u2", AccessToken: "at2", RefreshToken: "rt2"})
	st1, _ := p2.Status("u1")
	if st1.ErrCount != 1 || st1.LastErrTime.IsZero() {
		t.Fatalf("errCount/lastErr lost: %+v", st1)
	}
	p2.mu.RLock()
	e := p2.byUID[usedUID]
	lastUsed, usedSeq := e.lastUsed, e.usedSeq
	curSeq := p2.mutSeq
	p2.mu.RUnlock()
	if lastUsed.IsZero() || usedSeq != seq {
		t.Fatalf("lastUsed/usedSeq lost: %v %d want %d", lastUsed, usedSeq, seq)
	}
	if curSeq < usedSeq {
		t.Fatalf("mutSeq must catch up to restored usedSeq: %d < %d", curSeq, usedSeq)
	}
}

// 旧 state.json 的 checkin_done:true（bool）必须能解析，且不能丢掉整份状态。
func TestStateCheckinBoolBackwardCompat(t *testing.T) {
	dir := t.TempDir()
	fp := filepath.Join(dir, "state.json")
	raw := `{"accounts":{"u1":{"credits":77,"checkin_done":true,"err_count":2}}}`
	if err := os.WriteFile(fp, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	p := New(fp)
	t.Cleanup(p.closeFlusher)
	p.Add(&auth.Auth{UID: "u1"})
	st, ok := p.Status("u1")
	if !ok {
		t.Fatal("account lost")
	}
	if st.Credits != 77 || st.ErrCount != 2 {
		t.Fatalf("state lost: %+v", st)
	}
	if !st.CheckinDone {
		t.Fatalf("legacy bool true should map to checked-in today: %+v", st)
	}
}

// 启用账号不得清掉冷却/熔断写下的 reason（面板会显示「启用中」但选不中）。
func TestSetEnabledKeepsCooldownReason(t *testing.T) {
	p := newTestPool(t)
	addAcct(p, "u1", 100)
	p.Cooldown("u1", CoolPlan, time.Hour, "plan 权益不足")
	p.NoteError("u1")
	p.NoteError("u1")
	p.NoteError("u1")

	// 手动关闭再启用：冷却原因必须还在，手动原因才清。
	p.SetEnabled("u1", false, "user paused")
	p.SetEnabled("u1", true, "")
	st, _ := p.Status("u1")
	if st.Cooling && st.Reason == "" {
		t.Fatalf("cooldown reason lost while still cooling: %+v", st)
	}
	if !st.Cooling {
		t.Fatal("cooldown must survive enable toggle")
	}
	if p.Pick() != nil {
		t.Fatal("cooling account must not be pickable after re-enable")
	}
	drainBreaker(p, "u1")
}

// drainBreaker 等到熔断/冷却都过期（用极短时限重设，避免测试等待）。
func drainBreaker(p *Pool, uid string) {
	p.mu.Lock()
	if e, ok := p.byUID[uid]; ok {
		e.until = time.Time{}
		e.breakerUntil = time.Time{}
		e.coolKind = ""
		e.reason = ""
	}
	p.mu.Unlock()
}
