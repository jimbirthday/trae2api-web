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
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// recentCap 内存最近事件条数。
	recentCap = 100
	// defaultFileMax 单归档文件默认上限 16MiB。
	defaultFileMax int64 = 16 << 20
	// defaultQueue 归档写队列默认深度。
	defaultQueue = 1024
	// defaultRetainDays 归档默认保留天数。
	defaultRetainDays = 7
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
	Error            string    `json:"error,omitempty"`
	ClientIP         string    `json:"client_ip,omitempty"`
	UserAgent        string    `json:"user_agent,omitempty"`
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
	StartedAt     time.Time `json:"started_at"`
	Total         int64     `json:"total"`
	InFlight      int64     `json:"in_flight"`
	Succeeded     int64     `json:"succeeded"`
	Failed        int64     `json:"failed"`
	ClientErrors  int64     `json:"client_errors"`
	Canceled      int64     `json:"canceled"`
	Rotates       int64     `json:"rotates"`
	PromptTokens  int64     `json:"prompt_tokens"`
	ComplTokens   int64     `json:"completion_tokens"`
	TotalTokens   int64     `json:"total_tokens"`
	LatencySumMs  int64     `json:"latency_sum_ms"`
	AvgLatencyMs  float64   `json:"avg_latency_ms"`
	SuccessRate   float64   `json:"success_rate"`
	DroppedWrites uint64    `json:"dropped_writes"`
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
	if r.arch == nil {
		return nil, nil
	}
	files, err := r.arch.listFiles()
	if err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = recentCap
	}
	var out []Event
	// 文件名含日期与序号，字典序即写入序；从新到旧扫描，够 limit 即停。
	for i := len(files) - 1; i >= 0 && len(out) < limit; i-- {
		events, err := readFile(files[i].path, f, limit-len(out))
		if err != nil {
			return out, err
		}
		out = append(out, events...)
	}
	return out, nil
}

// readFile 读取单个 JSONL 文件并倒序返回匹配事件（最多 need 条）。
func readFile(path string, f Filter, need int) ([]Event, error) {
	fh, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer fh.Close()
	var all []Event
	sc := bufio.NewScanner(fh)
	sc.Buffer(make([]byte, 0, 64*1024), 4<<20)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var e Event
		if err := json.Unmarshal(line, &e); err != nil {
			continue // 半行（进程被杀）等损坏行跳过
		}
		if f.match(e) {
			all = append(all, e)
		}
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	// 文件内时间递增，倒序取尾部 need 条。
	if len(all) > need {
		all = all[len(all)-need:]
	}
	for i, j := 0, len(all)-1; i < j; i, j = i+1, j-1 {
		all[i], all[j] = all[j], all[i]
	}
	return all, nil
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
}

func newArchive(cfg Config) *archive {
	a := &archive{cfg: cfg, queue: make(chan Event, cfg.QueueSize), done: make(chan struct{})}
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
	for {
		select {
		case e := <-a.queue:
			a.write(e)
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
func (a *archive) cleanup() {
	entries, err := os.ReadDir(a.cfg.Dir)
	if err != nil {
		return
	}
	cutoff := time.Now().AddDate(0, 0, -a.cfg.RetainDays).Format("20060102")
	for _, ent := range entries {
		name := ent.Name()
		if !strings.HasPrefix(name, "requests-") || !strings.HasSuffix(name, ".jsonl") {
			continue
		}
		day := strings.TrimSuffix(strings.TrimPrefix(name, "requests-"), ".jsonl")
		if len(day) < 8 {
			continue
		}
		if day[:8] < cutoff {
			_ = os.Remove(filepath.Join(a.cfg.Dir, name))
		}
	}
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
