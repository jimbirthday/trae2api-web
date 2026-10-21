package reqlog

import (
	"encoding/json"
	"os"
	"path/filepath"
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
