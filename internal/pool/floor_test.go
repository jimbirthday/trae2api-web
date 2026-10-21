package pool

import (
	"testing"

	"trae2api-web/internal/auth"
)

// credit_floor：余额低于下限的账号不得参与选号（0 = 关闭）。
func TestCreditFloorExcludesLowBalance(t *testing.T) {
	p := New("")
	t.Cleanup(p.Flush)
	rich := &auth.Auth{UID: "rich", AccessToken: "a", ExpiresAt: 9999999999}
	poor := &auth.Auth{UID: "poor", AccessToken: "b", ExpiresAt: 9999999999}
	p.Add(rich)
	p.Add(poor)
	p.SetCredits("rich", 5000, 5000)
	p.SetCredits("poor", 10, 5000)

	// 关闭（0）：两个都可选
	p.SetLimits(Limits{MaxInFlight: 2, CreditFloor: 0})
	if len(p.AvailableForModel("m")) != 2 {
		t.Fatalf("floor=0 should allow both, got %v", p.AvailableForModel("m"))
	}

	// 开启 100：poor 被摘出
	p.SetLimits(Limits{MaxInFlight: 2, CreditFloor: 100})
	got := p.AvailableForModel("m")
	if len(got) != 1 || got[0] != "rich" {
		t.Fatalf("floor=100 should keep only rich, got %v", got)
	}
	for i := 0; i < 20; i++ {
		if a := p.PickExcludingForModel(nil, "m"); a == nil || a.UID != "rich" {
			t.Fatalf("pick returned %v, want rich", a)
		}
	}
}
