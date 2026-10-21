package usage

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCacheRateAndCreditsPerMillion(t *testing.T) {
	tr := New("", time.Minute)
	defer tr.Close()
	// 实测形态：prompt 2023 里 1920 命中缓存；一次请求消耗 0.54 积分
	tr.Add(Delta{Model: "glm-5.2", PromptTokens: 2023, CompletionTokens: 133,
		TotalTokens: 2156, CacheReadTokens: 1920, ReasoningTokens: 127})
	tr.AddCreditSample("glm-5.2", 0.54, 2156)

	s := tr.Snapshot()
	if len(s.Models) != 1 {
		t.Fatalf("models=%v", s.Models)
	}
	m := s.Models[0]
	wantCache := 1920.0 / 2023.0 * 100
	if diff := m.CacheRatePct - wantCache; diff > 1e-9 || diff < -1e-9 {
		t.Fatalf("cache rate=%.4f want %.4f", m.CacheRatePct, wantCache)
	}
	wantCPM := 0.54 / 2156 * 1e6 // 单样本 → 中位数即该样本
	if diff := m.CreditsPerMillion - wantCPM; diff > 1e-6 || diff < -1e-6 {
		t.Fatalf("credits/M=%.4f want %.4f", m.CreditsPerMillion, wantCPM)
	}
	if m.Requests != 1 || m.CacheReadTokens != 1920 || m.ReasoningTokens != 127 {
		t.Fatalf("stat=%+v", m)
	}
}

// 无积分采样时，credits/M 必须是 0（前端显示 "-"），不能凭空造数。
func TestNoCreditSamplesYieldsZero(t *testing.T) {
	tr := New("", time.Minute)
	defer tr.Close()
	tr.Add(Delta{Model: "glm-5.2", PromptTokens: 100, CompletionTokens: 10, TotalTokens: 110})
	m := tr.Snapshot().Models[0]
	if m.CreditsPerMillion != 0 {
		t.Fatalf("credits/M=%v, want 0 when no samples", m.CreditsPerMillion)
	}
	if m.CacheRatePct != 0 {
		t.Fatalf("cache rate=%v, want 0 when no cache hits", m.CacheRatePct)
	}
}

// 离群样本必须被中位数压制：这是上游异步结算导致的真实问题。
// 实测案例：kimi-k3 真实 59~100 积分/百万，但某次差值并入了多笔消耗 → 867。
func TestMedianResistsAsyncSettlementOutlier(t *testing.T) {
	tr := New("", time.Minute)
	defer tr.Close()
	// 三次正常 + 一次离群（异步结算把多笔并入）
	tr.AddCreditSample("kimi-k3", 0.12, 2020) // 59.4
	tr.AddCreditSample("kimi-k3", 0.21, 2100) // 100.0
	tr.AddCreditSample("kimi-k3", 0.13, 2040) // 63.7
	tr.AddCreditSample("kimi-k3", 1.86, 2145) // 867.1 ← 离群

	m := tr.Snapshot().Models[0]
	// 中位数 = (63.7+100.0)/2 ≈ 81.9，被离群值影响很小
	if m.CreditsPerMillion > 110 {
		t.Fatalf("median failed to resist outlier: got %.1f (mean would be ~272)", m.CreditsPerMillion)
	}
	if m.CreditsPerMillion < 55 {
		t.Fatalf("median too low: %.1f", m.CreditsPerMillion)
	}
	if m.CreditSamples != 4 {
		t.Fatalf("samples=%d", m.CreditSamples)
	}
}

// 合计行必须是各模型的加权合并（不是简单平均）。
func TestTotalsAreWeighted(t *testing.T) {
	tr := New("", time.Minute)
	defer tr.Close()
	// A：1000 prompt，800 命中 → 80%
	tr.Add(Delta{Model: "A", PromptTokens: 1000, TotalTokens: 1000, CacheReadTokens: 800})
	// B：100 prompt，0 命中 → 0%
	tr.Add(Delta{Model: "B", PromptTokens: 100, TotalTokens: 100, CacheReadTokens: 0})

	s := tr.Snapshot()
	want := 800.0 / 1100.0 * 100 // 加权 72.7%，简单平均会得到 40%
	if diff := s.Totals.CacheRatePct - want; diff > 1e-9 || diff < -1e-9 {
		t.Fatalf("totals cache=%.4f want %.4f (must be weighted, not averaged)",
			s.Totals.CacheRatePct, want)
	}
	if s.Totals.Requests != 2 || s.Totals.PromptTokens != 1100 {
		t.Fatalf("totals=%+v", s.Totals)
	}
}

// 落盘 → 读回，原始累计量必须一致（派生指标重算）。
func TestPersistRoundtrip(t *testing.T) {
	fp := filepath.Join(t.TempDir(), "usage.json")
	tr := New(fp, time.Minute)
	tr.Add(Delta{Model: "glm-5.2", PromptTokens: 2023, CompletionTokens: 133,
		TotalTokens: 2156, CacheReadTokens: 1920})
	tr.AddCreditSample("glm-5.2", 0.54, 2156)
	if err := tr.Flush(); err != nil {
		t.Fatal(err)
	}
	before := tr.Snapshot().Models[0]
	tr.Close()

	raw, _ := os.ReadFile(fp)
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("bad json: %v", err)
	}

	tr2 := New(fp, time.Minute)
	defer tr2.Close()
	after := tr2.Snapshot().Models[0]
	if after.PromptTokens != before.PromptTokens ||
		after.CacheReadTokens != before.CacheReadTokens ||
		after.SampledCredits != before.SampledCredits ||
		after.SampledTokens != before.SampledTokens {
		t.Fatalf("before=%+v after=%+v", before, after)
	}
	// 派生指标读回后仍能算出
	if after.CreditSamples != 1 || after.CreditsPerMillion == 0 {
		t.Fatalf("derived lost: %+v", after)
	}
}

// 窗口采样：窗口内所有请求的 token 与窗口积分增量整体配对，按 token 分摊到模型。
func TestSamplerWindowAllocation(t *testing.T) {
	consumed := 100.0
	reads := 0
	s := NewSampler(func(uid string) (float64, bool) { reads++; return consumed, true }, 0)
	s.window = 1 * time.Millisecond

	var got = map[string]float64{}
	var gotTokens = map[string]int64{}
	alloc := func(m string, c float64, tk int64) { got[m] += c; gotTokens[m] += tk }

	// 第一笔开窗口（读基线 100）
	s.NoteCompletion("u1", "A", 1000, alloc)
	if reads != 1 {
		t.Fatalf("window open should read baseline once, reads=%d", reads)
	}
	// 窗口内继续累计
	s.NoteCompletion("u1", "A", 1000, alloc)
	s.NoteCompletion("u1", "B", 2000, alloc)
	time.Sleep(2 * time.Millisecond)
	// 到点：结算。上游实际消耗 3 积分（异步结算，一次性补上）
	consumed = 103.0
	s.NoteCompletion("u1", "A", 1000, alloc) // 触发结算（本笔 token 属于下一窗口）

	// 触发结算的那一笔也归入本窗口（差值是在它完成之后才读的，含它更准确）：
	// A=3000, B=2000，总 5000；Δ=3 → A 1.8，B 1.2
	if d := got["A"] - 1.8; d > 1e-9 || d < -1e-9 {
		t.Fatalf("A credits=%v want 1.8 (all=%v)", got["A"], got)
	}
	if d := got["B"] - 1.2; d > 1e-9 || d < -1e-9 {
		t.Fatalf("B credits=%v want 1.2 (all=%v)", got["B"], got)
	}
	if gotTokens["A"] != 3000 || gotTokens["B"] != 2000 {
		t.Fatalf("tokens=%v", gotTokens)
	}
}

// 异步结算的两种失真（滞后=0、补齐过大）都必须被窗口口径吸收或丢弃。
func TestSamplerWindowAbsorbsAsyncSettlement(t *testing.T) {
	consumed := 50.0
	s := NewSampler(func(uid string) (float64, bool) { return consumed, true }, 0)
	s.window = 1 * time.Millisecond

	var total float64
	alloc := func(m string, c float64, tk int64) { total += c }

	// 窗口 1：期间上游一笔都没结算（差值为 0）→ 丢弃，不污染
	s.NoteCompletion("u1", "A", 2000, alloc)
	time.Sleep(2 * time.Millisecond)
	s.NoteCompletion("u1", "A", 2000, alloc) // 结算窗口 1，delta=0 → 丢弃
	if total != 0 {
		t.Fatalf("zero-delta window must be discarded, got %v", total)
	}

	// 窗口 2：上游把之前几笔一起补上（delta 较大但仍在合理区间）→ 采纳。
	// 第 3 次调用开新窗口（读基线 50.6），第 4 次到点才结算。
	consumed = 50.6
	s.NoteCompletion("u1", "A", 2000, alloc) // 开窗口 2
	consumed = 51.2
	time.Sleep(2 * time.Millisecond)
	s.NoteCompletion("u1", "A", 2000, alloc) // 结算窗口 2，Δ=0.6
	if total <= 0 {
		t.Fatalf("plausible delta window should be accepted, got %v", total)
	}
	if d := total - 0.6; d > 1e-9 || d < -1e-9 {
		t.Fatalf("total=%v want 0.6", total)
	}
}

// 超过上限的窗口（账号被别处使用）必须整窗丢弃。
func TestSamplerWindowRejectsImplausible(t *testing.T) {
	consumed := 10.0
	s := NewSampler(func(uid string) (float64, bool) { return consumed, true }, 0)
	s.window = 1 * time.Millisecond
	s.MaxWindowCredits = 100

	called := false
	alloc := func(string, float64, int64) { called = true }
	s.NoteCompletion("u1", "A", 2000, alloc)
	consumed = 5000 // 远超上限
	time.Sleep(2 * time.Millisecond)
	s.NoteCompletion("u1", "A", 2000, alloc)
	if called {
		t.Fatal("implausible window must be discarded")
	}
}

// 不同账号的窗口互相独立，不会串。
func TestSamplerPerAccountWindows(t *testing.T) {
	vals := map[string]float64{"u1": 10, "u2": 20}
	reads := map[string]int{}
	s := NewSampler(func(uid string) (float64, bool) { reads[uid]++; return vals[uid], true }, 0)
	s.window = time.Hour

	alloc := func(string, float64, int64) {}
	s.NoteCompletion("u1", "A", 1000, alloc)
	s.NoteCompletion("u2", "A", 1000, alloc)
	s.NoteCompletion("u1", "A", 1000, alloc) // 窗口未到点
	if reads["u1"] != 1 || reads["u2"] != 1 {
		t.Fatalf("each account opens its own window once: %v", reads)
	}
}

// 整体「积分/百万 token」必须是加权口径（总积分/总token），
// 不能把不同模型的样本混在一起取中位数。
func TestTotalsCreditsPerMillionIsWeighted(t *testing.T) {
	tr := New("", time.Minute)
	defer tr.Close()
	// A：便宜模型，1M token / 10 积分 → 10 积分/百万
	tr.AddCreditSample("A", 10, 1_000_000)
	// B：贵模型，1M token / 90 积分 → 90 积分/百万
	tr.AddCreditSample("B", 90, 1_000_000)

	s := tr.Snapshot()
	if s.Models == nil || len(s.Models) != 2 {
		t.Fatalf("models=%+v", s.Models)
	}
	// 两个模型合计 100 积分 / 2M token = 50
	if diff := s.Totals.CreditsPerMillion - 50; diff > 1e-9 || diff < -1e-9 {
		t.Fatalf("totals cpm=%.4f want 50 (weighted), samples=%v",
			s.Totals.CreditsPerMillion, s.Totals.Samples)
	}
}

// 吞吐聚合必须是「总 token / 总生成时长」的加权口径，不能是各请求 TPS 的算术平均：
// 一条 1s 吐 10 token（10 tok/s）和一条 20s 吐 1000 token（50 tok/s）
// 加权后应是 1010/21 ≈ 48.1，而不是 (10+50)/2 = 30。
func TestTimingAggregationIsWeighted(t *testing.T) {
	tr := New("", time.Minute)
	defer tr.Close()
	tr.AddTiming("m", 10, 1000, 300, 250, 20)
	tr.AddTiming("m", 1000, 20000, 800, 800, 4000)
	// 无效样本：没有生成时长，必须被丢弃（不能拉低均值）
	tr.AddTiming("m", 500, 0, 100, 100, 500)

	s := tr.Snapshot()
	m := s.Models[0]
	if m.TimingSamples != 2 {
		t.Fatalf("timing samples=%d want 2", m.TimingSamples)
	}
	want := 1010.0 / 21.0
	if diff := m.AvgTPS - want; diff > 1e-9 || diff < -1e-9 {
		t.Fatalf("avg_tps=%.4f want %.4f", m.AvgTPS, want)
	}
	if m.AvgGenMs != 10500 {
		t.Fatalf("avg_gen_ms=%.1f want 10500", m.AvgGenMs)
	}
	if m.AvgTTFTMs != 550 {
		t.Fatalf("avg_ttft_ms=%.1f want 550", m.AvgTTFTMs)
	}
	if m.AvgCharPS != 4020.0/21.0 {
		t.Fatalf("avg_char_ps=%.4f want %.4f", m.AvgCharPS, 4020.0/21.0)
	}
	// 合计行与单模型一致（只有一个模型）
	if s.Totals.AvgTPS != m.AvgTPS {
		t.Fatalf("totals avg_tps=%.4f want %.4f", s.Totals.AvgTPS, m.AvgTPS)
	}
	// 没有任何计时样本时，派生指标必须是 0（前端显示 "-"），不能凭空造数
	tr2 := New("", time.Minute)
	defer tr2.Close()
	tr2.Add(Delta{Model: "m", PromptTokens: 10, CompletionTokens: 5, TotalTokens: 15})
	if got := tr2.Snapshot().Models[0].AvgTPS; got != 0 {
		t.Fatalf("avg_tps with no samples=%v want 0", got)
	}
}
