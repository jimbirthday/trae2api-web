package pool

import (
	"testing"
	"time"
)

// randInt64N 注入点：固定返回 0 时总选到权重最高的候选。
func TestRandInjectionPicksTopWeight(t *testing.T) {
	p := newTestPool(t)
	p.randInt64N = func(int64) int64 { return 0 }
	addAcct(p, "small", 10)
	addAcct(p, "big", 100000)
	for i := 0; i < 30; i++ {
		if got := p.Pick(); got == nil || got.UID != "big" {
			t.Fatalf("pick=%+v want big", got)
		}
	}
}

// randInt64N 注入点：序列推进时 TopK 内都应有被选中的机会。
func TestRandInjectionSpreadsTopK(t *testing.T) {
	p := newTestPool(t)
	var i int64
	p.randInt64N = func(n int64) int64 {
		i++
		return i % n
	}
	for _, uid := range []string{"a", "b", "c"} {
		addAcct(p, uid, 100)
	}
	hits := map[string]int{}
	for j := 0; j < 200; j++ {
		hits[p.Pick().UID]++
	}
	if len(hits) != 3 {
		t.Fatalf("all three accounts should be selected: %v", hits)
	}
}

// 余额 <= 0 仍有小的正基数，不会被彻底饿死。
func TestZeroCreditsStillPickable(t *testing.T) {
	p := newTestPool(t)
	addAcct(p, "broke", 0)
	if got := p.Pick(); got == nil || got.UID != "broke" {
		t.Fatalf("pick=%+v want broke", got)
	}
	if f := balanceFactor(0); f <= 0 {
		t.Fatalf("balanceFactor(0)=%v must be positive", f)
	}
	if f := balanceFactor(100); f <= balanceFactor(0) {
		t.Fatalf("balanceFactor not monotonic: %v vs %v", balanceFactor(100), balanceFactor(0))
	}
}

// chooseWeighted 只从 TopK 中选：第 6 名及以后即使被随机指向也不会中选。
func TestChooseWeightedTopKTruncation(t *testing.T) {
	now := time.Now()
	cands := make([]pickCandidate, 0, 8)
	for i := 0; i < 8; i++ {
		cands = append(cands, pickCandidate{
			uid:          string(rune('a' + i)),
			credits:      int64(1_000_000 - i*100_000),
			manualWeight: 1,
			degrade:      1,
			lastUsedNano: now.UnixNano(), // 全部同样「刚用过」，只靠 credits 分层
		})
	}
	// 随机值指向最末（TopK 之外的权重区间）。
	picked := chooseWeighted(cands, func(int64) int64 { return scaleForRand - 1 })
	// 总权重只在 Top5 内归一：最大累计和之前的候选必然在 Top5。
	inTop5 := false
	for i := 0; i < pickTopK; i++ {
		if picked.uid == cands[i].uid {
			inTop5 = true
		}
	}
	if !inTop5 {
		t.Fatalf("picked %q outside Top%d", picked.uid, pickTopK)
	}
}

// 闲置补偿：同余额下更久未用的号权重更高。
func TestIdleBoost(t *testing.T) {
	now := time.Now()
	fresh := pickCandidate{uid: "fresh", credits: 1000, manualWeight: 1, degrade: 1, lastUsedNano: now.UnixNano()}
	idle := pickCandidate{uid: "idle", credits: 1000, manualWeight: 1, degrade: 1, lastUsedNano: now.Add(-2 * time.Hour).UnixNano()}
	fw := routeWeight(&fresh, now)
	iw := routeWeight(&idle, now)
	if iw <= fw {
		t.Fatalf("idle should weigh more: idle=%v fresh=%v", iw, fw)
	}
	// 补偿上限：1 + min(idleHours*0.5, 2) = 3；4h 与 240h 应同权重。
	capped := pickCandidate{uid: "capped", credits: 1000, manualWeight: 1, degrade: 1, lastUsedNano: now.Add(-4 * time.Hour).UnixNano()}
	veryIdle := pickCandidate{uid: "very", credits: 1000, manualWeight: 1, degrade: 1, lastUsedNano: now.Add(-240 * time.Hour).UnixNano()}
	if vw, cw := routeWeight(&veryIdle, now), routeWeight(&capped, now); vw != cw {
		t.Fatalf("idle boost should cap at 3x: %v vs %v", vw, cw)
	}
}

// 降权系数把权重压到 0.2 倍。
func TestDegradeFactorWeight(t *testing.T) {
	now := time.Now()
	normal := pickCandidate{uid: "n", credits: 1000, manualWeight: 1, degrade: 1, lastUsedNano: now.UnixNano()}
	degraded := pickCandidate{uid: "d", credits: 1000, manualWeight: 1, degrade: degradeFactorActive, lastUsedNano: now.UnixNano()}
	nw, dw := routeWeight(&normal, now), routeWeight(&degraded, now)
	if ratio := dw / nw; ratio < 0.19 || ratio > 0.21 {
		t.Fatalf("degrade ratio=%v want ~0.2", ratio)
	}
}

// 手动权重直接乘在总权重上。
func TestManualWeightMultiplies(t *testing.T) {
	now := time.Now()
	one := pickCandidate{uid: "one", credits: 1000, manualWeight: 1, degrade: 1, lastUsedNano: now.UnixNano()}
	ten := pickCandidate{uid: "ten", credits: 1000, manualWeight: 10, degrade: 1, lastUsedNano: now.UnixNano()}
	if r := routeWeight(&ten, now) / routeWeight(&one, now); r < 9.9 || r > 10.1 {
		t.Fatalf("manual weight ratio=%v want ~10", r)
	}
}
