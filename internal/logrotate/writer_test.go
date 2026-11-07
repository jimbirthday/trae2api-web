package logrotate

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestRotationIsBoundedAcrossRestarts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "server.log")
	for restart := 0; restart < 2; restart++ {
		w, err := New(path, 16, 4)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 20; i++ {
			line := fmt.Sprintf("%d-%02d\n", restart, i)
			if n, err := w.Write([]byte(line)); err != nil || n != len(line) {
				t.Fatalf("write=%d err=%v", n, err)
			}
			checkSize(t, path, 16, 4)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}
	data, err := os.ReadFile(path)
	if err != nil || !strings.HasSuffix(string(data), "1-19\n") {
		t.Fatalf("newest record missing: %q err=%v", data, err)
	}
}

func TestStartupShrinksLegacyLogs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "server.log")
	for _, suffix := range []string{"", ".1", ".9"} {
		if err := os.WriteFile(path+suffix, []byte("old-data-newest"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(path+".pid", []byte("123"), 0o600); err != nil {
		t.Fatal(err)
	}
	w, err := New(path, 6, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	data, err := os.ReadFile(path)
	if err != nil || string(data) != "newest" {
		t.Fatalf("legacy tail=%q err=%v", data, err)
	}
	for _, suffix := range []string{".1", ".9"} {
		if _, err := os.Stat(path + suffix); !os.IsNotExist(err) {
			t.Fatalf("oversized/extra backup was retained: %s", suffix)
		}
	}
	if _, err := os.Stat(path + ".pid"); err != nil {
		t.Fatal("unrelated file was removed")
	}
}

func TestLargeConcurrentWritesStayBounded(t *testing.T) {
	path := filepath.Join(t.TempDir(), "server.log")
	w, err := New(path, 32, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p := []byte(strings.Repeat("x", 256))
			if n, err := w.Write(p); err != nil || n != len(p) {
				t.Errorf("write=%d err=%v", n, err)
			}
		}()
	}
	wg.Wait()
	checkSize(t, path, 32, 4)
}

func TestRotationFailureStopsGrowth(t *testing.T) {
	path := filepath.Join(t.TempDir(), "server.log")
	w, err := New(path, 8, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if _, err := w.Write([]byte("12345678")); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path+".1", 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path+".1", "block"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("9")); err == nil {
		t.Fatal("rotation error was ignored")
	}
	info, _ := os.Stat(path)
	if info.Size() != 8 {
		t.Fatal("failed rotation allowed file to grow")
	}
}

func checkSize(t *testing.T, path string, maxBytes int64, maxFiles int) {
	t.Helper()
	files, err := filepath.Glob(path + "*")
	if err != nil || len(files) > maxFiles {
		t.Fatalf("files=%v err=%v", files, err)
	}
	var total int64
	for _, f := range files {
		st, err := os.Stat(f)
		if err != nil || st.Size() > maxBytes || st.Mode().Perm() != 0o600 {
			t.Fatalf("file=%s stat=%v err=%v", f, st, err)
		}
		total += st.Size()
	}
	if total > maxBytes*int64(maxFiles) {
		t.Fatalf("total exceeds budget: %d", total)
	}
}
