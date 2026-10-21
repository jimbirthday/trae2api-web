// Package reqlog 记录脱敏的请求级观测数据：内存环形缓冲（最近 100 条）+ 进程级
// 累计指标 + 可选 JSONL 归档（按天轮转、单文件容量上限、按天数清理）。
//
// 硬约束：
//   - Record 永不阻塞。归档写队列满时直接丢弃并计数，宁可丢日志也不拖慢请求；
//   - 绝不记录提示词、响应正文、Authorization 或任何 token；只记录 uid（面板需要）
//     与调用来源（ClientIP / UserAgent，由调用方决定是否填充）。
package reqlog

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// recentCap 内存最近事件条数。面板「请求流水」翻页时内存侧要够用，
	// 否则每翻一页都得回落到扫归档文件。512 条 Event 约几百 KB，可忽略。
	recentCap = 512
	// defaultFileMax 单归档文件默认上限 16MiB。
	defaultFileMax int64 = 16 << 20
	// defaultQueue 归档写队列默认深度。
	defaultQueue = 1024
	// defaultRetainDays 归档默认保留天数。
	defaultRetainDays = 3
	// cleanupInterval 归档清理检查间隔。
	cleanupInterval = time.Hour
)

// Outcome 取值（面板与指标按此分类统计）。
const (
	OutcomeSuccess     = "success"
	OutcomeHTTPError   = "http_error"
	OutcomeStreamError = "stream_error"
	OutcomeInterrupted = "interrupted"
)

// Config 归档配置。Enabled=false 时只保留内存缓冲与指标。
type Config struct {
	Dir          string
	Enabled      bool
	FileMaxBytes int64
	RetainDays   int
	QueueSize    int
}

// Event 是一条脱敏请求记录。
type Event struct {
	RequestID        string    `json:"request_id"`
	Time             time.Time `json:"time"`
	Path             string    `json:"path"`
	Model            string    `json:"model,omitempty"`
	UID              string    `json:"uid,omitempty"`
	Nickname         string    `json:"nickname,omitempty"`
	Status           int       `json:"status"`
	Outcome          string    `json:"outcome"` // success | upstream_error | client_error | canceled | ...
	LatencyMs        int64     `json:"latency_ms,omitempty"`
	PromptTokens     int64     `json:"prompt_tokens,omitempty"`
	CompletionTokens int64     `json:"completion_tokens,omitempty"`
	TotalTokens      int64     `json:"total_tokens,omitempty"`
	CacheReadTokens  int64     `json:"cache_read_tokens,omitempty"`
	Rotates          int       `json:"rotates,omitempty"`
	Stream           bool      `json:"stream,omitempty"`
	// 流式耗时拆解与吞吐。只对流式有意义；非流式为 0 且 omitempty 隐藏。
	//
	// GenMs 是「首帧→末帧」的纯生成时长，TPS 以它为分母。用总延迟当分母会把
	// 上游排队/首包/思考前置算进去，系统性低估吞吐。
	TTFTMs        int64   `json:"ttft_ms,omitempty"`         // 客户端看到第一个字
	UpstreamTTFMs int64   `json:"upstream_ttf_ms,omitempty"` // 上游首帧
	GenMs         int64   `json:"gen_ms,omitempty"`          // 首帧→末帧
	MaxGapMs      int64   `json:"max_gap_ms,omitempty"`      // 最大帧间隔
	TPS           float64 `json:"tps,omitempty"`             // completion_tokens / GenMs
	CharPS        float64 `json:"char_ps,omitempty"`         // 下发字符 / GenMs
	Error         string  `json:"error,omitempty"`
	ClientIP      string  `json:"client_ip,omitempty"`
	UserAgent     string  `json:"user_agent,omitempty"`
}

// Filter 归档查询条件；字符串字段为大小写不敏感的包含匹配，From/To 为闭区间。
type Filter struct {
	UID      string
	Model    string
	Outcome  string
	Path     string
	ClientIP string
	From     time.Time
	To       time.Time
}

// Metrics 进程级累计指标。
type Metrics struct {
	StartedAt    time.Time `json:"started_at"`
	Total        int64     `json:"total"`
	InFlight     int64     `json:"in_flight"`
	Succeeded    int64     `json:"succeeded"`
	Failed       int64     `json:"failed"`
	ClientErrors int64     `json:"client_errors"`
	Canceled     int64     `json:"canceled"`
	Rotates      int64     `json:"rotates"`
	PromptTokens int64     `json:"prompt_tokens"`
	ComplTokens  int64     `json:"completion_tokens"`
	TotalTokens  int64     `json:"total_tokens"`
	LatencySumMs int64     `json:"latency_sum_ms"`
	AvgLatencyMs float64   `json:"avg_latency_ms"`
	SuccessRate  float64   `json:"success_rate"`
	// 流式吞吐聚合。只统计「有 TPS 样本」的请求，避免非流式把均值拉低。
	TPSSamples    int64   `json:"tps_samples"`
	TPSSum        float64 `json:"tps_sum"`
	AvgTPS        float64 `json:"avg_tps"`
	AvgTTFTMs     float64 `json:"avg_ttft_ms"`
	AvgUpTTFMs    float64 `json:"avg_upstream_ttf_ms"`
	AvgGenMs      float64 `json:"avg_gen_ms"`
	DroppedWrites uint64  `json:"dropped_writes"`
}

// ArchiveStats 归档存储状态。
type ArchiveStats struct {
	Enabled   bool   `json:"enabled"`
	Dir       string `json:"dir,omitempty"`
	Files     int    `json:"files"`
	Bytes     int64  `json:"bytes"`
	Dropped   uint64 `json:"dropped_writes"`
	LastError string `json:"last_error,omitempty"`
}

// Snapshot 一次面板读取的完整快照。
type Snapshot struct {
	Metrics
	Recent  []Event      `json:"recent"`
	Archive ArchiveStats `json:"archive"`
}

// Recorder 并发安全的有界请求记录器。
type Recorder struct {
	mu      sync.Mutex
	recent  []Event // 环形缓冲
	head    int
	length  int
	started time.Time

	inFlight   atomic.Int64
	total      atomic.Int64
	succeeded  atomic.Int64
	failed     atomic.Int64
	clientErrs atomic.Int64
	canceled   atomic.Int64
	rotates    atomic.Int64
	promptTok  atomic.Int64
	complTok   atomic.Int64
	totalTok   atomic.Int64
	latencySum atomic.Int64
	dropped    atomic.Uint64

	// 流式吞吐聚合（只累加有 TPS 样本的请求）
	tpsSamples atomic.Int64
	tpsSumBits atomic.Uint64 // float64 bits
	ttftSum    atomic.Int64
	upTTFSum   atomic.Int64
	genSum     atomic.Int64

	arch *archive
}

// New 构建记录器。
func New(cfg Config) *Recorder {
	r := &Recorder{
		recent:  make([]Event, recentCap),
		started: time.Now(),
	}
	if cfg.Enabled && strings.TrimSpace(cfg.Dir) != "" {
		if cfg.FileMaxBytes <= 0 {
			cfg.FileMaxBytes = defaultFileMax
		}
		if cfg.RetainDays <= 0 {
			cfg.RetainDays = defaultRetainDays
		}
		if cfg.QueueSize <= 0 {
			cfg.QueueSize = defaultQueue
		}
		r.arch = newArchive(cfg)
	}
	return r
}

// NewRequestID 生成 16 hex 的请求 ID。
func NewRequestID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err == nil {
		return hex.EncodeToString(b)
	}
	return fmt.Sprintf("%016x", uint64(time.Now().UnixNano()))
}

// Begin 标记一条请求开始（用于 InFlight 指标），返回结束函数；
// 结束时自动补调 Record。调用方也可自行 Record，此时用 Begin 后直接 End。
func (r *Recorder) Begin() func(Event) {
	r.inFlight.Add(1)
	var once sync.Once
	return func(e Event) {
		once.Do(func() {
			r.inFlight.Add(-1)
			r.Record(e)
		})
	}
}

// Record 记录一条事件。永不阻塞：内存更新为常数时间，归档写入入队失败即丢弃。
func (r *Recorder) Record(e Event) {
	if e.Time.IsZero() {
		e.Time = time.Now()
	}
	if e.RequestID == "" {
		e.RequestID = NewRequestID()
	}
	switch e.Outcome {
	case "success":
		r.succeeded.Add(1)
	case "client_error":
		r.clientErrs.Add(1)
	case "canceled", "client_canceled":
		r.canceled.Add(1)
	default:
		r.failed.Add(1)
	}
	r.total.Add(1)
	r.latencySum.Add(e.LatencyMs)
	r.promptTok.Add(e.PromptTokens)
	r.complTok.Add(e.CompletionTokens)
	r.totalTok.Add(e.TotalTokens)
	if e.Rotates > 0 {
		r.rotates.Add(int64(e.Rotates))
	}
	// 只对「有生成时长 + 有 token 数」的流式请求采信 TPS 样本，
	// 否则非流式（GenMs=0）会把均值稀释成无意义的低值。
	if e.TPS > 0 && e.GenMs > 0 {
		r.tpsSamples.Add(1)
		addFloat(&r.tpsSumBits, e.TPS)
		r.genSum.Add(e.GenMs)
		if e.TTFTMs > 0 {
			r.ttftSum.Add(e.TTFTMs)
		}
		if e.UpstreamTTFMs > 0 {
			r.upTTFSum.Add(e.UpstreamTTFMs)
		}
	}

	r.mu.Lock()
	r.recent[r.head] = e
	r.head = (r.head + 1) % recentCap
	if r.length < recentCap {
		r.length++
	}
	r.mu.Unlock()

	if r.arch != nil {
		if !r.arch.enqueue(e) {
			r.dropped.Add(1)
			r.arch.noteDropped()
		}
	}
}

// Metrics 返回进程级累计指标。
func (r *Recorder) Metrics() Metrics {
	total := r.total.Load()
	m := Metrics{
		StartedAt:     r.started,
		Total:         total,
		InFlight:      r.inFlight.Load(),
		Succeeded:     r.succeeded.Load(),
		Failed:        r.failed.Load(),
		ClientErrors:  r.clientErrs.Load(),
		Canceled:      r.canceled.Load(),
		Rotates:       r.rotates.Load(),
		PromptTokens:  r.promptTok.Load(),
		ComplTokens:   r.complTok.Load(),
		TotalTokens:   r.totalTok.Load(),
		LatencySumMs:  r.latencySum.Load(),
		DroppedWrites: r.dropped.Load(),
	}
	if total > 0 {
		m.AvgLatencyMs = float64(m.LatencySumMs) / float64(total)
		m.SuccessRate = float64(m.Succeeded) / float64(total)
	}
	if n := r.tpsSamples.Load(); n > 0 {
		m.TPSSamples = n
		m.TPSSum = loadFloat(&r.tpsSumBits)
		m.AvgTPS = m.TPSSum / float64(n)
		m.AvgTTFTMs = float64(r.ttftSum.Load()) / float64(n)
		m.AvgUpTTFMs = float64(r.upTTFSum.Load()) / float64(n)
		m.AvgGenMs = float64(r.genSum.Load()) / float64(n)
	}
	return m
}

// Recent 返回最近 limit 条事件（最新在前）。
func (r *Recorder) Recent(limit int) []Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := r.length
	if limit > 0 && limit < n {
		n = limit
	}
	out := make([]Event, 0, n)
	for i := 1; i <= n; i++ {
		idx := (r.head - i + recentCap) % recentCap
		out = append(out, r.recent[idx])
	}
	return out
}

// Snapshot 返回指标 + 最近事件 + 归档状态。
func (r *Recorder) Snapshot() Snapshot {
	return Snapshot{
		Metrics: r.Metrics(),
		Recent:  r.Recent(recentCap),
		Archive: r.ArchiveStats(),
	}
}

// ArchiveStats 返回归档存储状态。
func (r *Recorder) ArchiveStats() ArchiveStats {
	if r.arch == nil {
		return ArchiveStats{Enabled: false}
	}
	return r.arch.stats(r.dropped.Load())
}

// Close 停止归档 goroutine 并 flush 剩余事件。
func (r *Recorder) Close() {
	if r.arch != nil {
		r.arch.close()
	}
}

// ReadArchive 读取归档事件（最新在前），支持时间范围/uid/model/outcome 过滤。
func (r *Recorder) ReadArchive(limit int, f Filter) ([]Event, error) {
	return r.ReadArchivePage(0, limit, f)
}

// ReadArchivePage 从归档读取一页（最新在前）：跳过最新 offset 条匹配，取 limit 条。
//
// 实现要点（上一版每次查询都正序扫完整个文件并逐行 Unmarshal，翻页/深 offset
// 时代价随文件大小线性增长）：
//   - 从文件尾部按块倒读，凑够 need 条匹配就停 —— 只解析真正要用的行；
//   - 多文件之间按文件名（日期+序号）从新到旧依次倒读。
func (r *Recorder) ReadArchivePage(offset, limit int, f Filter) ([]Event, error) {
	if r.arch == nil {
		return nil, nil
	}
	if limit <= 0 {
		limit = recentCap
	}
	if offset < 0 {
		offset = 0
	}
	need := offset + limit
	files, err := r.arch.listFiles()
	if err != nil {
		return nil, err
	}
	var out []Event
	for i := len(files) - 1; i >= 0 && len(out) < need; i-- {
		events, err := readTailMatches(files[i].path, f, need-len(out))
		if err != nil {
			return trimPage(out, offset, limit), err
		}
		out = append(out, events...)
	}
	return trimPage(out, offset, limit), nil
}

// trimPage 从「最新在前的全量匹配」里切出一页。
func trimPage(all []Event, offset, limit int) []Event {
	if offset >= len(all) {
		return []Event{}
	}
	all = all[offset:]
	if limit > 0 && len(all) > limit {
		all = all[:limit]
	}
	return all
}

// ArchiveTotal 返回归档中的事件总数及是否精确。
//
// 精确分支（无过滤条件）：只做换行符计数，且按 (path,size) 缓存，
// 除首次之外都是 O(文件数) 的 map 查表。
// 近似分支（带过滤条件）：不要为了一次翻页把七天日志全解析一遍，
// 直接返回 0/false，前端退化为「上一页/下一页」翻页。
func (r *Recorder) ArchiveTotal(f Filter) (int64, bool) {
	if r.arch == nil {
		return 0, false
	}
	if f != (Filter{}) {
		return 0, false
	}
	total, err := r.arch.totalLines()
	if err != nil {
		return 0, false
	}
	return total, true
}

// totalLines 汇总所有归档文件的行数（带缓存）。
func (a *archive) totalLines() (int64, error) {
	files, err := a.listFiles()
	if err != nil {
		return 0, err
	}
	var total int64
	for _, f := range files {
		n, err := a.linesOf(f)
		if err != nil {
			continue // 单个文件读失败不应让整页报错
		}
		total += n
	}
	return total, nil
}

// linesOf 返回单个文件的行数（按 size 缓存）。
func (a *archive) linesOf(f archiveFile) (int64, error) {
	a.cntMu.Lock()
	if c, ok := a.counts[f.path]; ok && c.size == f.size {
		a.cntMu.Unlock()
		return c.lines, nil
	}
	a.cntMu.Unlock()

	fh, err := os.Open(f.path)
	if err != nil {
		return 0, err
	}
	defer fh.Close()
	buf := make([]byte, 256<<10)
	var lines, n int64
	var last byte
	for {
		k, err := fh.Read(buf)
		if k > 0 {
			n += int64(k)
			lines += int64(bytes.Count(buf[:k], []byte{'\n'}))
			last = buf[k-1]
		}
		if err != nil {
			break
		}
	}
	if n > 0 && last != '\n' {
		lines++ // 最后一行没有换行符结尾（正在写入）
	}
	a.cntMu.Lock()
	a.counts[f.path] = fileLines{size: f.size, lines: lines}
	a.cntMu.Unlock()
	return lines, nil
}

// readTailMatches 从文件尾部倒读，返回最多 need 条匹配事件（最新在前）。
// 只解析凑够 need 条所需的行，不再无条件正序扫全文件。
func readTailMatches(path string, f Filter, need int) ([]Event, error) {
	if need <= 0 {
		return nil, nil
	}
	fh, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer fh.Close()
	st, err := fh.Stat()
	if err != nil {
		return nil, err
	}
	size := st.Size()
	if size == 0 {
		return nil, nil
	}
	const chunk = 64 << 10
	const maxLine = 4 << 20
	out := make([]Event, 0, need)
	var pending []byte // 行尾片段（属于更新一侧），等更早的数据拼齐
	pos := size
	for pos > 0 && len(out) < need {
		n := int64(chunk)
		if pos < n {
			n = pos
		}
		pos -= n
		buf := make([]byte, n)
		if _, rerr := fh.ReadAt(buf, pos); rerr != nil && rerr != io.EOF {
			return out, rerr
		}
		data := append(buf, pending...)
		start := 0
		if pos > 0 {
			i := bytes.IndexByte(data, '\n')
			if i < 0 {
				// 整块都是半行（超长行）：继续往前读，但要防内存失控。
				if len(data) <= maxLine {
					pending = data
				} else {
					pending = nil
				}
				continue
			}
			start = i + 1
			pending = append([]byte(nil), data[:start]...) // 半行留给更早的块拼齐
		} else {
			pending = nil
		}
		lines := bytes.Split(data[start:], []byte{'\n'})
		for i := len(lines) - 1; i >= 0 && len(out) < need; i-- {
			line := bytes.TrimSpace(lines[i])
			if len(line) == 0 {
				continue
			}
			var e Event
			if json.Unmarshal(line, &e) != nil {
				continue // 半行（进程被杀）等损坏行跳过
			}
			if !f.match(e) {
				continue
			}
			out = append(out, e)
		}
	}
	return out, nil
}

// match 判定事件是否满足过滤条件。
func (f Filter) match(e Event) bool {
	if !f.From.IsZero() && e.Time.Before(f.From) {
		return false
	}
	if !f.To.IsZero() && e.Time.After(f.To) {
		return false
	}
	if f.UID != "" && !containsFold(e.UID, f.UID) {
		return false
	}
	if f.Model != "" && !containsFold(e.Model, f.Model) {
		return false
	}
	if f.Outcome != "" && !containsFold(e.Outcome, f.Outcome) {
		return false
	}
	if f.Path != "" && !containsFold(e.Path, f.Path) {
		return false
	}
	if f.ClientIP != "" && !containsFold(e.ClientIP, f.ClientIP) {
		return false
	}
	return true
}

func containsFold(s, sub string) bool {
	return strings.Contains(strings.ToLower(s), strings.ToLower(sub))
}

// addFloat 用 atomic.Uint64 存 float64 位模式做无锁累加（CAS 循环）。
// 吞吐聚合是低频写（每请求一次）高频读（面板轮询），额外几次 CAS 无所谓。
func addFloat(bits *atomic.Uint64, delta float64) {
	for {
		old := bits.Load()
		next := math.Float64bits(math.Float64frombits(old) + delta)
		if bits.CompareAndSwap(old, next) {
			return
		}
	}
}

// loadFloat 读回 addFloat 累加的值。
func loadFloat(bits *atomic.Uint64) float64 {
	return math.Float64frombits(bits.Load())
}

type archiveFile struct {
	path string
	day  string
	seq  int
	size int64
}

// archive 是异步 JSONL 归档写入器。
type archive struct {
	cfg   Config
	queue chan Event
	done  chan struct{}
	wg    sync.WaitGroup

	mu        sync.Mutex
	curPath   string
	curDay    string
	curSeq    int
	curSize   int64
	fh        *os.File
	bw        *bufio.Writer
	dropCount atomic.Uint64
	lastErr   atomic.Value // string
	files     []archiveFile

	// cntMu/counts 是「每个归档文件的行数」缓存，只按 (path,size) 失效。
	// 面板分页要显示总数，逐次全量扫文件代价太高；文件只追加不修改，
	// size 不变即行数不变，因此这个缓存几乎不需要失效。
	cntMu  sync.Mutex
	counts map[string]fileLines
}

// fileLines 某归档文件的行数缓存（size 变化则重算）。
type fileLines struct {
	size  int64
	lines int64
}

func newArchive(cfg Config) *archive {
	a := &archive{cfg: cfg, queue: make(chan Event, cfg.QueueSize), done: make(chan struct{}),
		counts: make(map[string]fileLines)}
	_ = os.MkdirAll(cfg.Dir, 0o755)
	a.cleanup()
	a.wg.Add(1)
	go a.run()
	return a
}

// enqueue 非阻塞入队；队列满返回 false（调用方计入丢弃）。
func (a *archive) enqueue(e Event) bool {
	select {
	case a.queue <- e:
		return true
	default:
		return false
	}
}

func (a *archive) noteDropped() { a.dropCount.Add(1) }

func (a *archive) run() {
	defer a.wg.Done()
	// 清理节拍：每小时检查一次。按天轮转 + 保留 3 天，一小时精度足够，
	// 也不会因为文件少而白扫目录。
	tick := time.NewTicker(cleanupInterval)
	defer tick.Stop()
	for {
		select {
		case e := <-a.queue:
			a.write(e)
		case <-tick.C:
			a.cleanup()
		case <-a.done:
			// 关停时把队列里剩下的写完（有界，不会无限等待）。
			for {
				select {
				case e := <-a.queue:
					a.write(e)
				default:
					a.flush()
					return
				}
			}
		}
	}
}

func (a *archive) write(e Event) {
	line, err := json.Marshal(e)
	if err != nil {
		a.setErr(err)
		return
	}
	day := e.Time.Format("20060102")
	if a.fh == nil || day != a.curDay || (a.cfg.FileMaxBytes > 0 && a.curSize+int64(len(line))+1 > a.cfg.FileMaxBytes) {
		if err := a.rotate(day); err != nil {
			a.setErr(err)
			return
		}
	}
	if _, err := a.bw.Write(append(line, '\n')); err != nil {
		a.setErr(err)
		a.fh = nil
		return
	}
	a.curSize += int64(len(line)) + 1
}

func (a *archive) rotate(day string) error {
	a.flush()
	if a.fh != nil {
		_ = a.fh.Close()
		a.fh = nil
	}
	if day != a.curDay {
		a.curDay = day
		a.curSeq = 0
	} else {
		a.curSeq++
	}
	for {
		name := fmt.Sprintf("requests-%s-%d.jsonl", a.curDay, a.curSeq)
		p := filepath.Join(a.cfg.Dir, name)
		fh, err := os.OpenFile(p, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			return err
		}
		st, err := fh.Stat()
		if err != nil {
			fh.Close()
			return err
		}
		a.fh = fh
		a.bw = bufio.NewWriterSize(fh, 32*1024)
		a.curPath = p
		a.curSize = st.Size()
		if st.Size() > 0 && a.cfg.FileMaxBytes > 0 && st.Size() >= a.cfg.FileMaxBytes {
			fh.Close()
			a.fh = nil
			a.curSeq++
			continue
		}
		return nil
	}
}

func (a *archive) flush() {
	if a.bw != nil {
		_ = a.bw.Flush()
	}
}

func (a *archive) setErr(err error) {
	a.lastErr.Store(err.Error())
}

// cleanup 删除超过保留天数的归档文件。
//
// 保留 N 天 = 保留「今天在内最近 N 个自然日」的文件。因此截止日取 N-1 天前，
// 不能取 N 天前（那样实际会留下 N+1 天的数据）。
//
// 由 run() 里的 ticker 每小时触发一次：只在启动时清一次是不够的——容器连续
// 跑几天不重启，日志就会一直涨。
func (a *archive) cleanup() {
	entries, err := os.ReadDir(a.cfg.Dir)
	if err != nil {
		return
	}
	retain := a.cfg.RetainDays
	if retain <= 0 {
		retain = defaultRetainDays
	}
	cutoff := time.Now().AddDate(0, 0, -(retain - 1)).Format("20060102")
	for _, ent := range entries {
		name := ent.Name()
		if !strings.HasPrefix(name, "requests-") || !strings.HasSuffix(name, ".jsonl") {
			continue
		}
		day := strings.TrimSuffix(strings.TrimPrefix(name, "requests-"), ".jsonl")
		if len(day) < 8 {
			continue
		}
		if day[:8] >= cutoff {
			continue
		}
		path := filepath.Join(a.cfg.Dir, name)
		// 正在写的文件不删（理论上不会命中：当前文件一定是今天的）。
		if a.curPath == path {
			continue
		}
		if err := os.Remove(path); err == nil {
			a.forgetLines(path)
		}
	}
}

// forgetLines 清掉已删除文件的行数缓存，避免 map 随天数无限增长。
func (a *archive) forgetLines(path string) {
	a.cntMu.Lock()
	delete(a.counts, path)
	a.cntMu.Unlock()
}

func (a *archive) listFiles() ([]archiveFile, error) {
	entries, err := os.ReadDir(a.cfg.Dir)
	if err != nil {
		return nil, err
	}
	out := make([]archiveFile, 0, len(entries))
	for _, ent := range entries {
		name := ent.Name()
		if ent.IsDir() || !strings.HasPrefix(name, "requests-") || !strings.HasSuffix(name, ".jsonl") {
			continue
		}
		info, err := ent.Info()
		if err != nil {
			continue
		}
		base := strings.TrimSuffix(strings.TrimPrefix(name, "requests-"), ".jsonl")
		day, seq := base, 0
		if i := strings.LastIndexByte(base, '-'); i > 0 {
			day = base[:i]
			fmt.Sscanf(base[i+1:], "%d", &seq)
		}
		out = append(out, archiveFile{path: filepath.Join(a.cfg.Dir, name), day: day, seq: seq, size: info.Size()})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].day != out[j].day {
			return out[i].day < out[j].day
		}
		return out[i].seq < out[j].seq
	})
	return out, nil
}

func (a *archive) stats(dropped uint64) ArchiveStats {
	files, _ := a.listFiles()
	var bytes int64
	for _, f := range files {
		bytes += f.size
	}
	st := ArchiveStats{
		Enabled: true,
		Dir:     a.cfg.Dir,
		Files:   len(files),
		Bytes:   bytes,
		Dropped: dropped + a.dropCount.Load(),
	}
	if v, ok := a.lastErr.Load().(string); ok {
		st.LastError = v
	}
	return st
}

func (a *archive) close() {
	select {
	case <-a.done:
		return
	default:
	}
	close(a.done)
	a.wg.Wait()
	a.mu.Lock()
	if a.fh != nil {
		_ = a.fh.Close()
		a.fh = nil
	}
	a.mu.Unlock()
}
