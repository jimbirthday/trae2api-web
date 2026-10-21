// Package usage 用量统计：按模型累计 token / 缓存命中，并采样估算「每百万 token 消耗积分」。
//
// 两类指标的数据来源不同，必须分清：
//
//  1. token 与缓存（精确）：每个请求的 SSE 里都有 event:token_usage，
//     含 prompt_tokens / completion_tokens / cache_read_input_tokens /
//     reasoning_tokens。这类计数零额外开销，全量记录。
//
//  2. 积分消耗（采样估算）：TRAE 上游**不在对话流里返回积分**，只提供
//     账号级累计计数器 /trae/api/v2/pay/ide_user_ent_usage 的
//     usage_summary.consumed_amount。因此只能在请求前后各读一次、取差值。
//
// 积分采样的偏差控制：
//   - 每个账号限流采样（默认 30s 内最多一次），把额外上游调用压到 1/min/账号；
//   - 只在「该账号当时只有这一个在途请求」时采样，避免并发请求的消耗串到一条上；
//   - 积分与 token 都只从**同一批被采样请求**里累加，比值估计因此无偏
//     （分子分母同源，不会因未采样请求只进分母而系统性偏低）。
package usage

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// ModelStat 单模型累计量。字段分三类：请求/token（全量精确）、
// 积分采样（分子分母同源）、派生指标（只在快照里算，不落盘）。
type ModelStat struct {
	Model            string    `json:"model"`
	Requests         int64     `json:"requests"`
	PromptTokens     int64     `json:"prompt_tokens,omitempty"`
	CompletionTokens int64     `json:"completion_tokens,omitempty"`
	TotalTokens      int64     `json:"total_tokens,omitempty"`
	CacheReadTokens  int64     `json:"cache_read_tokens,omitempty"`
	ReasoningTokens  int64     `json:"reasoning_tokens,omitempty"`
	LastUsedAt       time.Time `json:"last_used_at,omitempty"`

	// 流式吞吐聚合。只统计真正拿到了计时快照的流式请求
	// （st.HasTiming 为真且有 completion_tokens），非流式不会进来污染均值。
	//
	// 与 avg_tps 的差别：这里同时保留「分子总量」和「分母总量」，
	// 因此可以算加权吞吐 = 全部 completion_tokens / 全部生成时长，
	// 而不是各请求 TPS 的算术平均（后者会被「短请求」系统性拉偏）。
	TimingSamples      int64 `json:"timing_samples,omitempty"`
	GenMsSum           int64 `json:"gen_ms_sum,omitempty"`           // 首帧→末帧累计
	TTFTMsSum          int64 `json:"ttft_ms_sum,omitempty"`          // 客户端首字累计
	UpstreamTTFMsSum   int64 `json:"upstream_ttft_ms_sum,omitempty"` // 上游首帧累计
	TimedCompletionTok int64 `json:"timed_completion_tokens,omitempty"`
	CharsSum           int64 `json:"chars_sum,omitempty"` // 实际下发字符（正文+思考）

	// 积分采样：每次采样记录「本次请求消耗的积分」与「本次 token 数」。
	//
	// 为什么保留原始样本而不是只留累加值：上游 usage_summary.consumed_amount 是
	// **异步结算**的账号级累计值，偶尔会把多笔消耗并进同一次差值（实测出现过
	// 单样本 867 积分/百万、而独立复测只有 59~100 的离群情况），也会偶尔结算
	// 滞后导致差值为 0。单样本因此不可信，必须用中位数抗离群。
	CreditSamples  int64   `json:"credit_samples,omitempty"`
	SampledCredits float64 `json:"sampled_credits,omitempty"` // 原始累加（仅供参考）
	SampledTokens  int64   `json:"sampled_tokens,omitempty"`  // 原始累加（仅供参考）
	// Samples 最近若干次采样的单次比率（积分/百万 token），用于取中位数。
	Samples []float64 `json:"samples,omitempty"`

	// 派生指标（序列化给前端用，落盘时忽略）
	CacheRatePct      float64 `json:"cache_rate_pct"`
	CreditsPerMillion float64 `json:"credits_per_million"`
	// AvgTPS 加权吞吐（tokens/秒）= timed_completion_tokens / gen_ms_sum。
	// AvgCharPS 是同一口径的字符速率，用来交叉验证：
	// 若 token 速率正常但字符速率异常低，说明瓶颈在我们下发而非上游生成。
	AvgTPS     float64 `json:"avg_tps"`
	AvgCharPS  float64 `json:"avg_char_ps"`
	AvgGenMs   float64 `json:"avg_gen_ms"`
	AvgTTFTMs  float64 `json:"avg_ttft_ms"`
	AvgUpTTFMs float64 `json:"avg_upstream_ttft_ms"`
}

// Delta 一次请求的用量增量。
type Delta struct {
	Model            string
	PromptTokens     int64
	CompletionTokens int64
	TotalTokens      int64
	CacheReadTokens  int64
	ReasoningTokens  int64
}

// Snapshot 对外快照：逐模型 + 合计。
type Snapshot struct {
	StartedAt time.Time   `json:"started_at"`
	UpdatedAt time.Time   `json:"updated_at"`
	Models    []ModelStat `json:"models"`
	Totals    ModelStat   `json:"totals"`
	// CreditSamplingIntervalSec 当前采样节流窗口（秒），前端用来解释
	// 「为什么积分/百万可能暂时为空」。
	CreditSamplingIntervalSec int64 `json:"credit_sampling_interval_sec"`
}

// Tracker 并发安全的用量累积器，可持久化到 JSON 文件。
type Tracker struct {
	mu       sync.RWMutex
	models   map[string]*ModelStat
	started  time.Time
	updated  time.Time
	sampleIv time.Duration

	fp    string
	dirty bool

	saveMu  sync.Mutex
	stopCh  chan struct{}
	stopped sync.Once
}

// New 构建。fp 非空时启用持久化（启动时读回、后台定期落盘）。
func New(fp string, sampleInterval time.Duration) *Tracker {
	if sampleInterval <= 0 {
		sampleInterval = 30 * time.Second
	}
	t := &Tracker{
		models:   map[string]*ModelStat{},
		started:  time.Now(),
		sampleIv: sampleInterval,
		fp:       fp,
		stopCh:   make(chan struct{}),
	}
	if fp != "" {
		t.load()
		go t.flusher()
	}
	return t
}

// Add 记录一次请求的 token 用量（全量，零额外开销）。
func (t *Tracker) Add(d Delta) {
	if d.Model == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	m := t.modelLocked(d.Model)
	m.Requests++
	m.PromptTokens += d.PromptTokens
	m.CompletionTokens += d.CompletionTokens
	m.TotalTokens += d.TotalTokens
	m.CacheReadTokens += d.CacheReadTokens
	m.ReasoningTokens += d.ReasoningTokens
	m.LastUsedAt = time.Now()
	t.updated = m.LastUsedAt
	t.dirty = true
}

// AddTiming 记录一次流式请求的耗时快照（吞吐聚合用）。
//
// 由调用方在流结束后调用一次；completionTokens 与 genMs 必须来自**同一个**
// 请求，否则加权吞吐会失真。genMs<=0（例如只收到 1 帧）直接丢弃样本。
func (t *Tracker) AddTiming(model string, completionTokens, genMs, ttftMs, upstreamTTFMs, chars int64) {
	if model == "" || genMs <= 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	m := t.modelLocked(model)
	m.TimingSamples++
	m.GenMsSum += genMs
	if completionTokens > 0 {
		m.TimedCompletionTok += completionTokens
	}
	if ttftMs > 0 {
		m.TTFTMsSum += ttftMs
	}
	if upstreamTTFMs > 0 {
		m.UpstreamTTFMsSum += upstreamTTFMs
	}
	if chars > 0 {
		m.CharsSum += chars
	}
	t.updated = time.Now()
	t.dirty = true
}

// AddCreditSample 记录一次窗口分摊结果（credits 为该模型在本窗口分摊到的积分，
// tokens 为该模型在本窗口的 token 数）。分子分母同源同窗口，比值无偏。
func (t *Tracker) AddCreditSample(model string, credits float64, tokens int64) {
	if model == "" || tokens <= 0 || credits < 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	m := t.modelLocked(model)
	m.CreditSamples++
	m.SampledCredits += credits
	m.SampledTokens += tokens
	m.Samples = appendBounded(m.Samples, credits/float64(tokens)*1e6, maxSamples)
	t.updated = time.Now()
	t.dirty = true
}

// SampleInterval 采样节流窗口（供调用方判断能否采样）。
func (t *Tracker) SampleInterval() time.Duration { return t.sampleIv }

func (t *Tracker) modelLocked(name string) *ModelStat {
	if m, ok := t.models[name]; ok {
		return m
	}
	m := &ModelStat{Model: name}
	t.models[name] = m
	return m
}

// Snapshot 返回逐模型统计（按请求数降序）+ 合计行，并算好派生指标。
func (t *Tracker) Snapshot() Snapshot {
	t.mu.RLock()
	out := make([]ModelStat, 0, len(t.models))
	var totals ModelStat
	for _, m := range t.models {
		cp := *m
		cp.CacheRatePct = cacheRatePct(cp.CacheReadTokens, cp.PromptTokens)
		cp.CreditsPerMillion = medianOf(cp.Samples)
		cp.AvgTPS, cp.AvgCharPS, cp.AvgGenMs, cp.AvgTTFTMs, cp.AvgUpTTFMs = timingDerived(
			cp.TimedCompletionTok, cp.CharsSum, cp.GenMsSum, cp.TTFTMsSum, cp.UpstreamTTFMsSum, cp.TimingSamples)
		out = append(out, cp)
		totals.Requests += cp.Requests
		totals.PromptTokens += cp.PromptTokens
		totals.CompletionTokens += cp.CompletionTokens
		totals.TotalTokens += cp.TotalTokens
		totals.CacheReadTokens += cp.CacheReadTokens
		totals.ReasoningTokens += cp.ReasoningTokens
		totals.TimingSamples += cp.TimingSamples
		totals.GenMsSum += cp.GenMsSum
		totals.TTFTMsSum += cp.TTFTMsSum
		totals.UpstreamTTFMsSum += cp.UpstreamTTFMsSum
		totals.TimedCompletionTok += cp.TimedCompletionTok
		totals.CharsSum += cp.CharsSum
		totals.CreditSamples += cp.CreditSamples
		totals.SampledCredits += cp.SampledCredits
		totals.SampledTokens += cp.SampledTokens
		for _, v := range cp.Samples {
			totals.Samples = appendBounded(totals.Samples, v, maxSamples)
		}
		if cp.LastUsedAt.After(totals.LastUsedAt) {
			totals.LastUsedAt = cp.LastUsedAt
		}
	}
	started, updated := t.started, t.updated
	t.mu.RUnlock()

	sort.Slice(out, func(i, j int) bool {
		if out[i].Requests != out[j].Requests {
			return out[i].Requests > out[j].Requests
		}
		return out[i].Model < out[j].Model
	})
	totals.Model = "全部模型"
	totals.CacheRatePct = cacheRatePct(totals.CacheReadTokens, totals.PromptTokens)
	// 整体吞吐同样是加权口径（总 token / 总生成时长），跨模型可比。
	totals.AvgTPS, totals.AvgCharPS, totals.AvgGenMs, totals.AvgTTFTMs, totals.AvgUpTTFMs = timingDerived(
		totals.TimedCompletionTok, totals.CharsSum, totals.GenMsSum, totals.TTFTMsSum, totals.UpstreamTTFMsSum, totals.TimingSamples)
	// 整体积分/百万 token 用「加权」口径：总积分 / 总 token。
	// 不能对各模型的样本混在一起取中位数——不同模型的单价差异很大，
	// 混合中位数会给出一个谁都不对应的数字。
	if totals.SampledTokens > 0 {
		totals.CreditsPerMillion = totals.SampledCredits / float64(totals.SampledTokens) * 1e6
	} else {
		totals.CreditsPerMillion = medianOf(totals.Samples)
	}
	return Snapshot{
		StartedAt:                 started,
		UpdatedAt:                 updated,
		Models:                    out,
		Totals:                    totals,
		CreditSamplingIntervalSec: int64(t.sampleIv / time.Second),
	}
}

// timingDerived 把累加量折算成派生指标。
//
// TPS 用「总 token / 总生成时长」而不是「各请求 TPS 的算术平均」：
// 后者会让一条 1 秒吐 5 token 的短请求和一条 60 秒吐 3000 token 的长请求
// 权重相同，最后得出一个既不代表长请求也不代表短请求的数。
func timingDerived(completionTok, chars, genMsSum, ttftSum, upTTFSum, samples int64) (tps, charPS, avgGenMs, avgTTFTMs, avgUpTTFMs float64) {
	if samples <= 0 || genMsSum <= 0 {
		return 0, 0, 0, 0, 0
	}
	secs := float64(genMsSum) / 1000.0
	if completionTok > 0 {
		tps = float64(completionTok) / secs
	}
	if chars > 0 {
		charPS = float64(chars) / secs
	}
	avgGenMs = float64(genMsSum) / float64(samples)
	avgTTFTMs = float64(ttftSum) / float64(samples)
	avgUpTTFMs = float64(upTTFSum) / float64(samples)
	return
}

// cacheRatePct 缓存命中率 = cache_read / prompt。prompt 为上游给的输入 token 总数
// （已包含命中的那部分），所以这是标准的「输入 token 中命中缓存的比例」。
func cacheRatePct(cacheRead, prompt int64) float64 {
	if prompt <= 0 {
		return 0
	}
	v := float64(cacheRead) / float64(prompt) * 100
	if v < 0 {
		return 0
	}
	if v > 100 {
		return 100
	}
	return v
}

// maxSamples 每模型保留的采样数上限（中位数的窗口）。
const maxSamples = 32

// appendBounded 追加并按上限保留最近 n 个（FIFO 淘汰最旧）。
func appendBounded(s []float64, v float64, n int) []float64 {
	if v <= 0 {
		return s
	}
	s = append(s, v)
	if len(s) > n {
		s = append(s[:0], s[len(s)-n:]...)
	}
	return s
}

// medianOf 返回样本中位数；无样本返回 0（前端显示为 "-"）。
//
// 用中位数而非均值：上游积分计数器是异步结算的，会把多笔消耗并进一次差值，
// 产生远高于真实值的离群样本（实测 867 vs 真实 59~100）。中位数对这种
// 单侧离群是稳健的，均值会被明显拉偏。
func medianOf(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	cp := make([]float64, len(vals))
	copy(cp, vals)
	sort.Float64s(cp)
	n := len(cp)
	if n%2 == 1 {
		return cp[n/2]
	}
	return (cp[n/2-1] + cp[n/2]) / 2
}

// ---------------------------------------------------------------------------
// 持久化
// ---------------------------------------------------------------------------

type persistDoc struct {
	Version int                  `json:"version"`
	Started time.Time            `json:"started_at"`
	Updated time.Time            `json:"updated_at"`
	Models  map[string]ModelStat `json:"models"`
}

const persistVersion = 1

func (t *Tracker) load() {
	raw, err := os.ReadFile(t.fp)
	if err != nil {
		return
	}
	var doc persistDoc
	if json.Unmarshal(raw, &doc) != nil || doc.Version != persistVersion {
		return
	}
	if !doc.Started.IsZero() {
		t.started = doc.Started
	}
	t.updated = doc.Updated
	for name, m := range doc.Models {
		cp := m
		cp.Model = name
		t.models[name] = &cp
	}
}

// Flush 同步落盘（原子写）。dirty 为假时直接返回。
func (t *Tracker) Flush() error {
	if t.fp == "" {
		return nil
	}
	t.mu.Lock()
	if !t.dirty {
		t.mu.Unlock()
		return nil
	}
	doc := persistDoc{Version: persistVersion, Started: t.started, Updated: t.updated,
		Models: make(map[string]ModelStat, len(t.models))}
	for name, m := range t.models {
		cp := *m
		cp.CacheRatePct = 0      // 派生指标不落盘
		cp.CreditsPerMillion = 0 // 同上，读回后按原始量重算
		cp.AvgTPS, cp.AvgCharPS, cp.AvgGenMs, cp.AvgTTFTMs, cp.AvgUpTTFMs = 0, 0, 0, 0, 0
		doc.Models[name] = cp
	}
	t.dirty = false
	t.mu.Unlock()

	raw, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return err
	}
	t.saveMu.Lock()
	defer t.saveMu.Unlock()
	if err := os.MkdirAll(filepath.Dir(t.fp), 0o755); err != nil {
		return err
	}
	tmp := t.fp + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, t.fp)
}

func (t *Tracker) flusher() {
	tick := time.NewTicker(15 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-t.stopCh:
			_ = t.Flush()
			return
		case <-tick.C:
			_ = t.Flush()
		}
	}
}

// Close 停止后台落盘并做最后一次 Flush。
func (t *Tracker) Close() {
	if t.fp == "" {
		return
	}
	t.stopped.Do(func() { close(t.stopCh) })
}
