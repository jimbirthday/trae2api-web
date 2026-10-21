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
		out = append(out, cp)
		totals.Requests += cp.Requests
		totals.PromptTokens += cp.PromptTokens
		totals.CompletionTokens += cp.CompletionTokens
		totals.TotalTokens += cp.TotalTokens
		totals.CacheReadTokens += cp.CacheReadTokens
		totals.ReasoningTokens += cp.ReasoningTokens
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
	totals.CreditsPerMillion = medianOf(totals.Samples)
	return Snapshot{
		StartedAt:                 started,
		UpdatedAt:                 updated,
		Models:                    out,
		Totals:                    totals,
		CreditSamplingIntervalSec: int64(t.sampleIv / time.Second),
	}
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
