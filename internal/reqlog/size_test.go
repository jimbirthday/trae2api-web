package reqlog

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestArchiveSizeCapDuringWritesAndRestart(t *testing.T) {
	dir := t.TempDir()
	const fileMax, totalMax = 512, 1536
	day := time.Now().Format("20060102")
	// 模拟升级前同一天已经积累了超过总量上限的文件。
	for i := 0; i < 8; i++ {
		if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("requests-%s-%d.jsonl", day, i)), []byte(strings.Repeat("x", fileMax)), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	unrelated := filepath.Join(dir, "keep.txt")
	if err := os.WriteFile(unrelated, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	for restart := 0; restart < 2; restart++ {
		a := newArchive(Config{Dir: dir, FileMaxBytes: fileMax, TotalMaxBytes: totalMax, RetainDays: 3, QueueSize: 1})
		check := func() {
			t.Helper()
			if err := a.flush(); err != nil {
				t.Fatal(err)
			}
			if got := a.stats(0).Bytes; got > totalMax {
				t.Fatalf("archive exceeded cap: %d > %d", got, totalMax)
			}
		}
		check()
		for i := 0; i < 30; i++ {
			a.write(Event{Time: time.Now(), RequestID: fmt.Sprintf("%d-%d", restart, i), Model: strings.Repeat("m", 200), Outcome: OutcomeSuccess})
			check()
		}
		files, err := a.listFiles()
		if err != nil || len(files) == 0 {
			t.Fatalf("files=%v err=%v", files, err)
		}
		for _, f := range files {
			if f.size > fileMax {
				t.Fatalf("file exceeds limit: %s (%d)", f.path, f.size)
			}
		}
		a.close()
	}
	if _, err := os.Stat(unrelated); err != nil {
		t.Fatal("capacity cleanup removed unrelated file")
	}
	r := New(Config{Dir: dir, Enabled: true, FileMaxBytes: fileMax, TotalMaxBytes: totalMax})
	defer r.Close()
	rows, err := r.ReadArchive(100, Filter{})
	if err != nil || len(rows) == 0 || rows[0].RequestID != "1-29" {
		t.Fatalf("newest record was not retained: rows=%v err=%v", rows, err)
	}
}

func TestArchiveOversizedRecordIsDropped(t *testing.T) {
	r := New(Config{Dir: t.TempDir(), Enabled: true, FileMaxBytes: 512, TotalMaxBytes: 1024})
	r.Record(Event{Model: strings.Repeat("x", 2048), Outcome: OutcomeSuccess})
	r.Record(Event{RequestID: "keep", Outcome: OutcomeSuccess})
	r.Close()
	stats := r.ArchiveStats()
	rows, err := r.ReadArchive(10, Filter{})
	if stats.Bytes > 512 || stats.Dropped != 1 || stats.LastError == "" || err != nil || len(rows) != 1 || rows[0].RequestID != "keep" {
		t.Fatalf("stats=%+v rows=%v err=%v", stats, rows, err)
	}
}

func TestArchiveTotalCapCannotBeDisabled(t *testing.T) {
	for _, limit := range []int64{0, -1, MaxTotalBytes + 1} {
		r := New(Config{Dir: t.TempDir(), Enabled: true, TotalMaxBytes: limit, FileMaxBytes: MaxTotalBytes + 1})
		if r.arch.cfg.TotalMaxBytes != MaxTotalBytes || r.arch.cfg.FileMaxBytes != MaxTotalBytes {
			t.Fatal("unsafe archive limit was accepted")
		}
		r.Close()
	}
}

func TestArchivePrunesOldestSequenceAndCache(t *testing.T) {
	dir := t.TempDir()
	day := time.Now().Format("20060102")
	a := &archive{cfg: Config{Dir: dir, TotalMaxBytes: 6}, counts: map[string]fileLines{}}
	for _, seq := range []int{2, 10, 11} {
		p := filepath.Join(dir, fmt.Sprintf("requests-%s-%d.jsonl", day, seq))
		if err := os.WriteFile(p, []byte("{}\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		a.counts[p] = fileLines{size: 3, lines: 1}
	}
	if err := a.makeRoom(0); err != nil {
		t.Fatal(err)
	}
	oldest := filepath.Join(dir, fmt.Sprintf("requests-%s-2.jsonl", day))
	if _, err := os.Stat(oldest); !os.IsNotExist(err) {
		t.Fatal("oldest sequence was not removed")
	}
	if _, exists := a.counts[oldest]; exists {
		t.Fatal("removed file remained in line cache")
	}
}
