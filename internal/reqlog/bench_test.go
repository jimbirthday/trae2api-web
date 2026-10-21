package reqlog

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// oldReadFile 是改造前 readFile 的等价实现（整文件正序解析后截尾），仅用于基准对比。
func oldReadFile(path string, f Filter, need int) ([]Event, error) {
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
			continue
		}
		if f.match(e) {
			all = append(all, e)
		}
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if len(all) > need {
		all = all[len(all)-need:]
	}
	return all, nil
}

func BenchmarkTailVsFullScan(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "requests-2026-10-21.jsonl")
	fh, err := os.Create(path)
	if err != nil {
		b.Fatal(err)
	}
	now := time.Now()
	for i := 0; i < 200000; i++ {
		e := Event{
			RequestID: fmt.Sprintf("id%08d", i), Time: now.Add(time.Duration(i) * time.Millisecond),
			Path: "/v1/chat/completions", Model: "glm-5.2", UID: fmt.Sprintf("u%d", i%5),
			Outcome: "success", Status: 200, LatencyMs: 1200, PromptTokens: 3200, TotalTokens: 3600,
		}
		line, _ := json.Marshal(e)
		fh.Write(append(line, '\n'))
	}
	fh.Close()
	st, _ := os.Stat(path)
	b.Logf("file size = %.1f MB, 200k lines", float64(st.Size())/1048576)

	b.Run("tail-read-50", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			got, err := readTailMatches(path, Filter{}, 50)
			if err != nil || len(got) != 50 {
				b.Fatalf("got=%d err=%v", len(got), err)
			}
		}
	})
	b.Run("old-full-scan-50", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			got, err := oldReadFile(path, Filter{}, 50)
			if err != nil || len(got) != 50 {
				b.Fatalf("got=%d err=%v", len(got), err)
			}
		}
	})
}
