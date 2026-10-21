package reqlog

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestRingBufferCaps(t *testing.T) {
	r := New(Config{})
	defer r.Close()
	for i := 0; i < recentCap+50; i++ {
		r.Record(Event{Path: "/v1/chat/completions", Outcome: "success", Status: 200, LatencyMs: int64(i)})
	}
	recent := r.Recent(0)
	if len(recent) != recentCap {
		t.Fatalf("recent=%d want %d", len(recent), recentCap)
	}
	// 最新在前。
	if recent[0].LatencyMs != int64(recentCap+49) {
		t.Fatalf("newest latency=%d want %d", recent[0].LatencyMs, recentCap+49)
	}
	if recent[len(recent)-1].LatencyMs != 50 {
		t.Fatalf("oldest latency=%d want 50", recent[len(recent)-1].LatencyMs)
	}
	if got := len(r.Recent(10)); got != 10 {
		t.Fatalf("limit ignored: %d", got)
	}
	m := r.Metrics()
	if m.Total != int64(recentCap+50) || m.Succeeded != m.Total || m.SuccessRate != 1 {
		t.Fatalf("metrics=%+v", m)
	}
}

func TestMetricsOutcomes(t *testing.T) {
	r := New(Config{})
	defer r.Close()
	r.Record(Event{Outcome: "success", PromptTokens: 10, CompletionTokens: 5, TotalTokens: 15, LatencyMs: 100})
	r.Record(Event{Outcome: "upstream_error", LatencyMs: 200})
	r.Record(Event{Outcome: "client_error"})
	r.Record(Event{Outcome: "canceled"})
	m := r.Metrics()
	if m.Total != 4 || m.Succeeded != 1 || m.Failed != 1 || m.ClientErrors != 1 || m.Canceled != 1 {
		t.Fatalf("metrics=%+v", m)
	}
	if m.TotalTokens != 15 || m.PromptTokens != 10 || m.ComplTokens != 5 {
		t.Fatalf("tokens=%+v", m)
	}
	if m.AvgLatencyMs != 75 {
		t.Fatalf("avg latency=%v", m.AvgLatencyMs)
	}
}

func TestBeginTracksInFlight(t *testing.T) {
	r := New(Config{})
	defer r.Close()
	end := r.Begin()
	if r.Metrics().InFlight != 1 {
		t.Fatal("in_flight not tracked")
	}
	end(Event{Outcome: "success"})
	end(Event{Outcome: "success"}) // 幂等
	if r.Metrics().InFlight != 0 || r.Metrics().Total != 1 {
		t.Fatalf("metrics=%+v", r.Metrics())
	}
}

func TestArchiveWriteReadFilter(t *testing.T) {
	dir := t.TempDir()
	r := New(Config{Dir: dir, Enabled: true, RetainDays: 7})
	now := time.Now()
	events := []Event{
		{RequestID: "1", Time: now.Add(-3 * time.Minute), Model: "m1", UID: "u1", Outcome: "success", Status: 200, Path: "/v1/chat/completions", ClientIP: "10.0.0.1"},
		{RequestID: "2", Time: now.Add(-2 * time.Minute), Model: "m2", UID: "u2", Outcome: "upstream_error", Status: 502, Path: "/v1/chat/completions"},
		{RequestID: "3", Time: now.Add(-time.Minute), Model: "m1", UID: "u2", Outcome: "success", Status: 200, Path: "/v1/messages"},
	}
	for _, e := range events {
		r.Record(e)
	}
	r.Close() // flush

	all, err := r.ReadArchive(0, Filter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 3 {
		t.Fatalf("archive events=%d want 3", len(all))
	}
	if all[0].RequestID != "3" || all[2].RequestID != "1" {
		t.Fatalf("archive order wrong: %v", []string{all[0].RequestID, all[1].RequestID, all[2].RequestID})
	}
	if got, _ := r.ReadArchive(0, Filter{UID: "u2"}); len(got) != 2 {
		t.Fatalf("uid filter=%d want 2", len(got))
	}
	if got, _ := r.ReadArchive(0, Filter{Model: "M1"}); len(got) != 2 {
		t.Fatalf("model filter (case-insensitive)=%d want 2", len(got))
	}
	if got, _ := r.ReadArchive(0, Filter{Outcome: "upstream"}); len(got) != 1 || got[0].RequestID != "2" {
		t.Fatalf("outcome filter=%+v", got)
	}
	if got, _ := r.ReadArchive(0, Filter{From: now.Add(-150 * time.Second)}); len(got) != 2 {
		t.Fatalf("from filter=%d want 2", len(got))
	}
	if got, _ := r.ReadArchive(0, Filter{To: now.Add(-150 * time.Second)}); len(got) != 1 || got[0].RequestID != "1" {
		t.Fatalf("to filter=%+v", got)
	}
	if got, _ := r.ReadArchive(1, Filter{}); len(got) != 1 || got[0].RequestID != "3" {
		t.Fatalf("limit=%+v", got)
	}

	stats := r.ArchiveStats()
	if !stats.Enabled || stats.Files == 0 || stats.Bytes == 0 {
		t.Fatalf("stats=%+v", stats)
	}
}

func TestArchiveNoSecrets(t *testing.T) {
	dir := t.TempDir()
	r := New(Config{Dir: dir, Enabled: true})
	r.Record(Event{
		Path:    "/v1/chat/completions",
		Model:   "m",
		UID:     "u1",
		Outcome: "success",
		Error:   "upstream 1005",
	})
	r.Close()
	files, _ := filepath.Glob(filepath.Join(dir, "*.jsonl"))
	if len(files) == 0 {
		t.Fatal("no archive file")
	}
	data, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	var e map[string]any
	line := strings.SplitN(strings.TrimSpace(string(data)), "\n", 2)[0]
	if err := json.Unmarshal([]byte(line), &e); err != nil {
		t.Fatalf("bad jsonl: %v", err)
	}
	for _, banned := range []string{"authorization", "token", "prompt", "messages", "content"} {
		if _, ok := e[banned]; ok {
			t.Fatalf("event leaked field %q: %v", banned, e)
		}
	}
}

func TestArchiveQueueFullNeverBlocks(t *testing.T) {
	dir := t.TempDir()
	r := New(Config{Dir: dir, Enabled: true, QueueSize: 1})
	defer r.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		// 远多于队列深度；若 Record 阻塞，这里会卡住。
		for i := 0; i < 5000; i++ {
			r.Record(Event{Outcome: "success", Path: "/v1/x"})
		}
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Record blocked on full queue")
	}
	// 丢弃是可接受的；关键是内存指标完整。
	if r.Metrics().Total != 5000 {
		t.Fatalf("total=%d want 5000", r.Metrics().Total)
	}
}

func TestArchiveRotation(t *testing.T) {
	dir := t.TempDir()
	r := New(Config{Dir: dir, Enabled: true, FileMaxBytes: 1}) // 每条都触发轮转
	for i := 0; i < 5; i++ {
		r.Record(Event{Outcome: "success", Path: "/v1/x", RequestID: NewRequestID()})
	}
	r.Close()
	files, _ := filepath.Glob(filepath.Join(dir, "requests-*-*.jsonl"))
	if len(files) < 2 {
		t.Fatalf("expected multiple files, got %v", files)
	}
}

func TestArchiveRetentionCleanup(t *testing.T) {
	dir := t.TempDir()
	old := filepath.Join(dir, "requests-20000101-0.jsonl")
	if err := os.WriteFile(old, []byte("{}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	r := New(Config{Dir: dir, Enabled: true, RetainDays: 1})
	r.Close()
	if _, err := os.Stat(old); !os.IsNotExist(err) {
		t.Fatalf("old archive not removed: %v", err)
	}
}

func TestArchiveDisabledNoFiles(t *testing.T) {
	dir := t.TempDir()
	r := New(Config{Dir: dir, Enabled: false})
	r.Record(Event{Outcome: "success"})
	r.Close()
	files, _ := filepath.Glob(filepath.Join(dir, "*"))
	if len(files) != 0 {
		t.Fatalf("disabled recorder wrote files: %v", files)
	}
	if r.ArchiveStats().Enabled {
		t.Fatal("stats claim enabled")
	}
	if got, err := r.ReadArchive(10, Filter{}); err != nil || len(got) != 0 {
		t.Fatalf("ReadArchive on disabled=%v,%v", got, err)
	}
}

func TestRecordConcurrent(t *testing.T) {
	r := New(Config{Dir: t.TempDir(), Enabled: true, QueueSize: 64})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				r.Record(Event{Outcome: "success", Path: "/v1/x"})
			}
		}()
	}
	wg.Wait()
	r.Close()
	if r.Metrics().Total != 1600 {
		t.Fatalf("total=%d want 1600", r.Metrics().Total)
	}
}

func TestNewRequestIDUnique(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 1000; i++ {
		id := NewRequestID()
		if len(id) != 16 {
			t.Fatalf("id len=%d", len(id))
		}
		if seen[id] {
			t.Fatal("duplicate request id")
		}
		seen[id] = true
	}
}

func TestCloseIsIdempotentAndFlushes(t *testing.T) {
	dir := t.TempDir()
	r := New(Config{Dir: dir, Enabled: true})
	r.Record(Event{Outcome: "success", Path: "/v1/x"})
	r.Close()
	r.Close()
	got, err := r.ReadArchive(0, Filter{})
	if err != nil || len(got) != 1 {
		t.Fatalf("after close=%v,%v", got, err)
	}
}

// 倒读实现必须与正序扫描结果一致：多分页、多文件、跨块边界都不能错。
func TestReadArchivePageAcrossChunks(t *testing.T) {
	dir := t.TempDir()
	r := New(Config{Dir: dir, Enabled: true, FileMaxBytes: 2 << 10}) // 小文件强制多文件
	now := time.Now()
	const n = 300
	for i := 0; i < n; i++ {
		r.Record(Event{
			RequestID: fmt.Sprintf("r%03d", i),
			Time:      now.Add(time.Duration(i) * time.Second),
			UID:       "u1",
			Model:     "m1",
			Outcome:   "success",
			Status:    200,
			Path:      "/v1/chat/completions",
		})
	}
	r.Close() // flush

	// 全量：最新在前
	all, err := r.ReadArchivePage(0, 1000, Filter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != n {
		t.Fatalf("all=%d want %d", len(all), n)
	}
	for i := 0; i < n; i++ {
		want := fmt.Sprintf("r%03d", n-1-i)
		if all[i].RequestID != want {
			t.Fatalf("index %d = %s want %s", i, all[i].RequestID, want)
		}
	}

	// 分页拼起来必须等于全量
	var glued []string
	for off := 0; off < n; off += 37 {
		pg, err := r.ReadArchivePage(off, 37, Filter{})
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range pg {
			glued = append(glued, e.RequestID)
		}
	}
	if len(glued) != n {
		t.Fatalf("glued=%d want %d", len(glued), n)
	}
	for i := 0; i < n; i++ {
		if glued[i] != all[i].RequestID {
			t.Fatalf("page glue mismatch at %d: %s vs %s", i, glued[i], all[i].RequestID)
		}
	}

	// 越界 offset 返回空切片（不是 nil，前端据此判断「该页无记录」）
	if got, _ := r.ReadArchivePage(10*n, 10, Filter{}); len(got) != 0 {
		t.Fatalf("out of range=%d want 0", len(got))
	}

	// 过滤 + 分页
	f := Filter{UID: "u1"}
	if got, _ := r.ReadArchivePage(0, 5, f); len(got) != 5 {
		t.Fatalf("filtered page=%d", len(got))
	}
	if got, _ := r.ReadArchivePage(0, 5, Filter{UID: "nope"}); len(got) != 0 {
		t.Fatalf("no match=%d", len(got))
	}
}

// 精确总数只对「无过滤条件」生效，且与真实行数一致。
func TestArchiveTotal(t *testing.T) {
	dir := t.TempDir()
	r := New(Config{Dir: dir, Enabled: true, FileMaxBytes: 2 << 10})
	defer r.Close()
	if n, ok := r.ArchiveTotal(Filter{}); n != 0 || !ok {
		t.Fatalf("empty total=%d,%v", n, ok)
	}
	for i := 0; i < 120; i++ {
		r.Record(Event{RequestID: fmt.Sprintf("x%03d", i), Path: "/v1/chat/completions", Outcome: "success"})
	}
	r.Close()

	if n, ok := r.ArchiveTotal(Filter{}); !ok || n != 120 {
		t.Fatalf("total=%d ok=%v want 120,true", n, ok)
	}
	// 带过滤：不做全量扫描，报告未知
	if n, ok := r.ArchiveTotal(Filter{UID: "u1"}); ok || n != 0 {
		t.Fatalf("filtered total should be unknown, got %d,%v", n, ok)
	}
	// 缓存生效后再调一次仍正确
	if n, ok := r.ArchiveTotal(Filter{}); !ok || n != 120 {
		t.Fatalf("cached total=%d ok=%v", n, ok)
	}
}

func TestOutcomeFilterIsExactForKnownValues(t *testing.T) {
	dir := t.TempDir()
	r := New(Config{Dir: dir, Enabled: true, RetainDays: 3})
	now := time.Now()
	r.Record(Event{Time: now.Add(-4 * time.Second), Outcome: OutcomeSuccess, Status: 200})
	r.Record(Event{Time: now.Add(-3 * time.Second), Outcome: OutcomeHTTPError, Status: 502})
	r.Record(Event{Time: now.Add(-2 * time.Second), Outcome: OutcomeStreamError, Status: 200})
	r.Record(Event{Time: now.Add(-time.Second), Outcome: OutcomeInterrupted})
	// 历史别名应归一为 interrupted 后被筛出。
	r.Record(Event{Time: now, Outcome: OutcomeClientCanceled})
	r.Close() // flush 归档（写缓冲只在滚动/关闭时落盘）

	// 已知常量必须精确匹配："stream_error" 不能把 http_error 也带出来。
	if got, _ := r.ReadArchive(0, Filter{Outcome: OutcomeStreamError}); len(got) != 1 || got[0].Outcome != OutcomeStreamError {
		t.Fatalf("stream_error filter got %d: %+v", len(got), got)
	}
	if got, _ := r.ReadArchive(0, Filter{Outcome: OutcomeHTTPError}); len(got) != 1 {
		t.Fatalf("http_error filter got %d", len(got))
	}
	if got, _ := r.ReadArchive(0, Filter{Outcome: OutcomeInterrupted}); len(got) != 2 {
		t.Fatalf("interrupted should include legacy alias, got %d", len(got))
	}
	// 自由文本仍走子串匹配（http_error + stream_error）。
	if got, _ := r.ReadArchive(0, Filter{Outcome: "error"}); len(got) != 2 {
		t.Fatalf("substring filter got %d", len(got))
	}
}

func TestArchiveFilesArePrivate(t *testing.T) {
	dir := t.TempDir()
	r := New(Config{Dir: dir, Enabled: true, RetainDays: 3})
	r.Record(Event{Path: "/v1/chat/completions", UID: "u1", Model: "m", Outcome: OutcomeSuccess, Status: 200})
	r.Close()

	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(ents) == 0 {
		t.Fatal("no archive file written")
	}
	for _, e := range ents {
		info, err := e.Info()
		if err != nil {
			t.Fatal(err)
		}
		if perm := info.Mode().Perm(); perm != 0o600 {
			t.Errorf("%s perm=%o, want 600", e.Name(), perm)
		}
	}
}

func TestBeginIsIdempotent(t *testing.T) {
	r := New(Config{})
	end := r.Begin()
	if got := r.Metrics().InFlight; got != 1 {
		t.Fatalf("in_flight=%d", got)
	}
	end(Event{Outcome: OutcomeSuccess})
	end(Event{Outcome: OutcomeSuccess})
	end(Event{Outcome: OutcomeSuccess})
	if got := r.Metrics().InFlight; got != 0 {
		t.Fatalf("in_flight after multiple end=%d (should not go negative)", got)
	}
	if got := r.Metrics().Total; got != 1 {
		t.Fatalf("total=%d, want 1 (end must only record once)", got)
	}
}

func TestKnownOutcomesCovered(t *testing.T) {
	for _, o := range KnownOutcomes() {
		if !knownOutcomeSet[o] {
			t.Errorf("KnownOutcomes contains %q not in knownOutcomeSet", o)
		}
	}
	if canonicalOutcome(OutcomeClientCanceled) != OutcomeInterrupted {
		t.Error("legacy alias not canonicalized")
	}
}

func TestCloseStopsBackgroundGoroutines(t *testing.T) {
	before := runtime.NumGoroutine()
	dir := t.TempDir()
	r := New(Config{Dir: dir, Enabled: true, RetainDays: 3})
	for i := 0; i < 50; i++ {
		r.Record(Event{Outcome: OutcomeSuccess, Path: "/v1/chat/completions"})
	}
	r.Close()
	r.Close() // 幂等：重复 Close 不能 panic/重复关 channel
	// 归档 goroutine（含 cleanup ticker）必须已退出。
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= before+1 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("goroutine leak: before=%d after=%d", before, runtime.NumGoroutine())
}
