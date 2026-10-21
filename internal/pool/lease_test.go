package pool

import "testing"

// 租约必须绑定所有者：A 的 Release 不能释放 B 的名额。
func TestLeaseTokenBinding(t *testing.T) {
	p := newTestPool(t)
	p.SetLimits(Limits{MaxInFlight: 2})
	addAcct(p, "u1", 100)

	tokA, okA := p.AcquireLease("u1")
	tokB, okB := p.AcquireLease("u1")
	if !okA || !okB || tokA == tokB {
		t.Fatalf("acquire: %v %v tokens %q %q", okA, okB, tokA, tokB)
	}
	if got := p.InFlight("u1"); got != 2 {
		t.Fatalf("inflight=%d want 2", got)
	}

	// 重复释放同一 token 只能减一次。
	p.ReleaseToken(tokA)
	p.ReleaseToken(tokA)
	if got := p.InFlight("u1"); got != 1 {
		t.Fatalf("inflight=%d want 1 after releasing A", got)
	}
	p.ReleaseToken(tokB)
	if got := p.InFlight("u1"); got != 0 {
		t.Fatalf("inflight=%d want 0", got)
	}
	if n := p.LeaseCount(); n != 0 {
		t.Fatalf("leases leaked: %d", n)
	}
	// 未知 token 不产生副作用。
	p.ReleaseToken("bogus")
	if got := p.InFlight("u1"); got != 0 {
		t.Fatalf("inflight=%d want 0 after bogus release", got)
	}
}

// 跨账号的 Release 不能动别人的名额。
func TestLeaseCrossAccountIsolation(t *testing.T) {
	p := newTestPool(t)
	p.SetLimits(Limits{MaxInFlight: 1})
	addAcct(p, "u1", 100)
	addAcct(p, "u2", 100)

	if _, ok := p.AcquireLease("u1"); !ok {
		t.Fatal("acquire u1")
	}
	if _, ok := p.AcquireLease("u2"); !ok {
		t.Fatal("acquire u2")
	}
	p.ReleaseToken("u1#1")
	if got := p.InFlight("u2"); got != 1 {
		t.Fatalf("u2 released by u1's token: %d", got)
	}
}

// 重复 Release 不能多减（否则会偷走同账号其他请求的名额）。
func TestLegacyReleaseIsIdempotent(t *testing.T) {
	p := newTestPool(t)
	p.SetLimits(Limits{MaxInFlight: 2})
	addAcct(p, "u1", 100)

	if !p.Acquire("u1") {
		t.Fatal("acquire 1")
	}
	if !p.Acquire("u1") {
		t.Fatal("acquire 2")
	}
	p.Release("u1") // 只释放一个
	if got := p.InFlight("u1"); got != 1 {
		t.Fatalf("inflight=%d want 1", got)
	}
	p.Release("u1") // 再释放剩下的那个
	if got := p.InFlight("u1"); got != 0 {
		t.Fatalf("inflight=%d want 0", got)
	}
	p.Release("u1") // 多余调用：不得变成负数或偷名额
	if got := p.InFlight("u1"); got != 0 {
		t.Fatalf("inflight=%d want 0 (idempotent)", got)
	}
}
