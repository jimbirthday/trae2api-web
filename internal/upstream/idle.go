// idle.go 上游 SSE 读的「帧间空闲」看门狗。
//
// 为什么需要：StreamHTTP 刻意不设总超时（长推理动辄十分钟），但这样一来
// 「上游收下请求后既不吐帧也不断开」的连接会永久占住一个 goroutine + 一条
// 在途租约。Transport.ResponseHeaderTimeout 只能兜住「连响应头都拿不到」，
// 拿不到响应头之后的静默它管不了。这里用「距上一个字节超过 N 秒就取消请求」
// 兜底，超时后包装成 ErrStreamIdleTimeout，让上层按「不罚号、换号重试」处理。
package upstream

import (
	"context"
	"io"
	"sync"
	"time"
)

// DefaultStreamIdleTimeout 默认帧间空闲上限。
//
// 取值要同时满足两头：比上游最长的思考停顿长（实测思考期心跳/帧间隔可达
// 数十秒），又要比客户端耐心短得多。120s 是折中：真挂死的连接两分钟内释放，
// 正常长思考不会被误杀。
const DefaultStreamIdleTimeout = 120 * time.Second

// idleTimeoutBody 给底层 ReadCloser 加空闲超时。
//
// 并发：Read 与 Close 可能来自不同 goroutine（解析协程 + 上层 Cancel），
// 所有可变状态都走 mu；timer 回调只做一次 cancel，避免与 Read 抢锁死等。
type idleTimeoutBody struct {
	rc     io.ReadCloser
	d      time.Duration
	cancel context.CancelFunc

	mu     sync.Mutex
	timer  *time.Timer
	fired  bool
	closed bool
}

func newIdleTimeoutBody(rc io.ReadCloser, d time.Duration, cancel context.CancelFunc) io.ReadCloser {
	if rc == nil || d <= 0 {
		return rc
	}
	b := &idleTimeoutBody{rc: rc, d: d, cancel: cancel}
	b.timer = time.AfterFunc(d, func() {
		b.mu.Lock()
		if b.closed || b.fired {
			b.mu.Unlock()
			return
		}
		b.fired = true
		b.mu.Unlock()
		// 双保险：cancel 让 HTTP 传输层拆连接；Close 让「已经阻塞在 Read 上」
		// 的调用立刻返回。只做 cancel 时，若底层 reader 不感知 context
		// （pipe、部分自定义 body），Read 会一直挂着 —— 那这个看门狗就没用了。
		if b.cancel != nil {
			b.cancel()
		}
		_ = b.rc.Close()
	})
	return b
}

func (b *idleTimeoutBody) Read(p []byte) (int, error) {
	b.mu.Lock()
	fired := b.fired
	b.mu.Unlock()
	if fired {
		// 统一成可识别的哨兵错误：底层的 context.Canceled / closed pipe 会被
		// IsClientGone 误判成「客户端断开」，那样既不换号也不记失败，请求白挂。
		return 0, ErrStreamIdleTimeout
	}
	n, err := b.rc.Read(p)
	b.mu.Lock()
	fired = b.fired
	if n > 0 && !fired && !b.closed {
		b.timer.Reset(b.d)
	}
	b.mu.Unlock()
	if fired {
		return n, ErrStreamIdleTimeout
	}
	return n, err
}

func (b *idleTimeoutBody) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	b.timer.Stop()
	b.mu.Unlock()
	err := b.rc.Close()
	// 只取消子 context：父 ctx 由调用方持有，可能被别的逻辑复用。
	if b.cancel != nil {
		b.cancel()
	}
	return err
}
