package reqlog

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// 保留 N 天 = 保留「今天在内最近 N 个自然日」。3 天时：今天、昨天、前天留；前天之前删。
func TestCleanupRetainsExactlyNDays(t *testing.T) {
	dir := t.TempDir()
	mk := func(offsetDays int) string {
		day := time.Now().AddDate(0, 0, -offsetDays).Format("20060102")
		p := filepath.Join(dir, "requests-"+day+"-0.jsonl")
		if err := os.WriteFile(p, []byte("{}\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}
	keep := []string{mk(0), mk(1), mk(2)}
	drop := mk(3) // 距今 3 天：超出 3 天窗口

	a := &archive{cfg: Config{Dir: dir, RetainDays: 3}, counts: map[string]fileLines{}}
	a.cleanup()

	for _, p := range keep {
		if _, err := os.Stat(p); err != nil {
			t.Errorf("should be kept: %s (%v)", filepath.Base(p), err)
		}
	}
	if _, err := os.Stat(drop); !os.IsNotExist(err) {
		t.Errorf("should be removed: %s", filepath.Base(drop))
	}
}

// 正在写的文件即使日期超期也不能删。
func TestCleanupSkipsCurrentFile(t *testing.T) {
	dir := t.TempDir()
	old := filepath.Join(dir, "requests-20000101-0.jsonl")
	if err := os.WriteFile(old, []byte("{}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	a := &archive{cfg: Config{Dir: dir, RetainDays: 3}, counts: map[string]fileLines{}, curPath: old}
	a.cleanup()
	if _, err := os.Stat(old); err != nil {
		t.Fatalf("current file must not be removed: %v", err)
	}
}

// 清理要同步丢掉行数缓存，否则面板总数会算上已删除的文件。
func TestCleanupForgetsLineCache(t *testing.T) {
	dir := t.TempDir()
	old := filepath.Join(dir, "requests-20000101-0.jsonl")
	if err := os.WriteFile(old, []byte("{}\n{}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	a := &archive{cfg: Config{Dir: dir, RetainDays: 3}, counts: map[string]fileLines{old: {size: 6, lines: 2}}}
	a.cleanup()
	if _, ok := a.counts[old]; ok {
		t.Fatal("line cache not cleared for removed file")
	}
}
