// Package logrotate 为运行日志提供固定容量的文件轮转。
package logrotate

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

const (
	FileMaxBytes int64 = 16 << 20
	MaxFiles           = 4 // 含当前文件，共 64MiB。
)

type Writer struct {
	mu       sync.Mutex
	path     string
	maxBytes int64
	maxFiles int
	file     *os.File
	size     int64
}

func New(path string, maxBytes int64, maxFiles int) (*Writer, error) {
	if maxBytes <= 0 || maxFiles < 1 {
		return nil, fmt.Errorf("invalid log rotation limits")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(filepath.Dir(path))
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		prefix := filepath.Base(path) + "."
		if !strings.HasPrefix(e.Name(), prefix) {
			continue
		}
		n, err := strconv.Atoi(strings.TrimPrefix(e.Name(), prefix))
		if err != nil || n < 1 {
			continue
		}
		info, err := e.Info()
		if err != nil {
			return nil, err
		}
		if n >= maxFiles || info.Size() > maxBytes {
			if err := os.Remove(filepath.Join(filepath.Dir(path), e.Name())); err != nil {
				return nil, err
			}
		}
	}
	w := &Writer{path: path, maxBytes: maxBytes, maxFiles: maxFiles}
	if err := w.open(); err != nil {
		return nil, err
	}
	if w.size > maxBytes {
		// 升级时压缩旧的无限增长文件，只保留末尾，不创建临时磁盘副本。
		tail := make([]byte, maxBytes)
		_, err = w.file.ReadAt(tail, w.size-maxBytes)
		if err == nil {
			err = w.file.Truncate(0)
		}
		if err == nil {
			_, err = w.file.Write(tail)
		}
		if err != nil {
			w.file.Close()
			return nil, err
		}
		w.size = maxBytes
	}
	return w, nil
}

func (w *Writer) open() error {
	f, err := os.OpenFile(w.path, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	info, err := f.Stat()
	if err == nil && !info.Mode().IsRegular() {
		err = fmt.Errorf("log path is not a regular file: %s", w.path)
	}
	if err == nil {
		err = f.Chmod(0o600)
	}
	if err != nil {
		f.Close()
		return err
	}
	w.file, w.size = f, info.Size()
	return nil
}

func (w *Writer) rotate() error {
	oldest := w.path
	if w.maxFiles > 1 {
		oldest += "." + strconv.Itoa(w.maxFiles-1)
	}
	if err := os.Remove(oldest); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := w.file.Close(); err != nil {
		return err
	}
	w.file = nil
	for n := w.maxFiles - 2; n >= 0; n-- {
		from := w.path
		if n > 0 {
			from += "." + strconv.Itoa(n)
		}
		if err := os.Rename(from, w.path+"."+strconv.Itoa(n+1)); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return w.open()
}

func (w *Writer) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return 0, os.ErrClosed
	}
	written := 0
	for len(p) > 0 {
		n := len(p)
		if int64(n) > w.maxBytes {
			n = int(w.maxBytes)
		}
		if w.size+int64(n) > w.maxBytes {
			if err := w.rotate(); err != nil {
				return written, err
			}
		}
		count, err := w.file.Write(p[:n])
		w.size += int64(count)
		written += count
		if err != nil {
			return written, err
		}
		if count != n {
			return written, io.ErrShortWrite
		}
		p = p[n:]
	}
	return written, nil
}

func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}
	err := w.file.Close()
	w.file = nil
	return err
}
