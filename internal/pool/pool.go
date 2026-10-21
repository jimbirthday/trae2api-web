// Package pool 账号池：单一状态机（健康/冷却/熔断/连败降权）+ 在途租约
// + 加权挑选 + state.json 持久化。
//
// 分层：
//   - pool.go      结构体、构造、账号增删、在途租约
//   - entry.go     条目/对外状态类型与健康判定
//   - limits.go    调优参数与默认值
//   - pick.go      候选快照、权重、Top5 加权随机、可用性查询
//   - cooldown.go  冷却/熔断/模型级冷却
//   - degrade.go   连败降权
//   - state.go     开关/暂停/禁用/额度/用量等状态迁移
//   - persist.go   state.json 原子落盘与后台 flusher
//
// 依赖只允许 internal/auth 与 internal/logfmt；不依赖 upstream。
package pool

import (
	"sync"
	"sync/atomic"
	"time"

	"trae2api-web/internal/auth"
)

// Pool 账号池。
type Pool struct {
	mu      sync.RWMutex
	byUID   map[string]*entry
	stateFp string

	limits Limits
	// maxInFlight 是 limits.MaxInFlight 的 atomic 副本：Acquire 热路径不取 p.mu。
	maxInFlight atomic.Int32

	// randInt64N 仅供测试注入确定性随机源；nil 时用 math/rand/v2 全局源。
	randInt64N func(n int64) int64

	// mutSeq 由持 p.mu 的写路径自增，标记「内存状态版本」；
	// dirtySeq / writtenSeq 让落盘可丢弃「更晚采集、更早落盘」的乱序覆盖。
	mutSeq     uint64
	dirtySeq   atomic.Uint64
	writtenSeq atomic.Uint64

	// saveMu 串行化 state.json 落盘（tmp 写入 + rename 全在锁外做）。
	// 不得与 p.mu 形成嵌套：saveMu 只包住写盘本身。
	saveMu sync.Mutex

	stopFlusher chan struct{}
	flusherDone chan struct{}
	stopOnce    sync.Once
}

// New 构建池；stateFp 非空时尝试加载旧状态并启动后台 flusher。
func New(stateFp string) *Pool {
	p := &Pool{
		byUID:   map[string]*entry{},
		stateFp: stateFp,
		limits:  defaultLimits(),
	}
	p.maxInFlight.Store(int32(p.limits.MaxInFlight))
	if stateFp != "" {
		p.load()
		p.startFlusher()
	}
	return p
}

// SetLimits 覆盖调优参数；零值字段回落默认值。
func (p *Pool) SetLimits(l Limits) {
	l = normalizeLimits(l)
	p.mu.Lock()
	p.limits = l
	p.mutSeq++
	p.mu.Unlock()
	p.maxInFlight.Store(int32(l.MaxInFlight))
}

// Add 加入账号；已存在则保留原运行状态（credits/冷却/开关），只更新凭证。
func (p *Pool) Add(a *auth.Auth) {
	if a == nil || a.UID == "" {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if e, ok := p.byUID[a.UID]; ok {
		e.a = a
		p.markDirtyLocked()
		return
	}
	p.byUID[a.UID] = newEntry(a)
	p.markDirtyLocked()
}

// SyncToDir 用最新扫描结果对齐池：新账号加入、消失的账号剔除（状态保留）。
func (p *Pool) SyncToDir(auths []*auth.Auth) {
	p.mu.Lock()
	defer p.mu.Unlock()
	seen := make(map[string]bool, len(auths))
	for _, a := range auths {
		if a == nil || a.UID == "" {
			continue
		}
		seen[a.UID] = true
		if e, ok := p.byUID[a.UID]; ok {
			e.a = a
			continue
		}
		p.byUID[a.UID] = newEntry(a)
	}
	for uid := range p.byUID {
		if !seen[uid] {
			delete(p.byUID, uid)
		}
	}
	p.markDirtyLocked()
}

// Remove 删除账号（仅内存索引与 state.json 条目；凭证文件由调用方删）。
func (p *Pool) Remove(uid string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.byUID[uid]; !ok {
		return false
	}
	delete(p.byUID, uid)
	p.markDirtyLocked()
	return true
}

// entryOf 取条目指针（短读锁）。
func (p *Pool) entryOf(uid string) *entry {
	p.mu.RLock()
	e := p.byUID[uid]
	p.mu.RUnlock()
	return e
}

// ---------------------------------------------------------------------------
// 在途租约
// ---------------------------------------------------------------------------

// Acquire 抢占一个在途名额：CAS 自增，满/不存在返回 false。
// MaxInFlight<=0 时恒 true（租约关闭）。
func (p *Pool) Acquire(uid string) bool {
	e := p.entryOf(uid)
	if e == nil {
		return false
	}
	limit := int(p.maxInFlight.Load())
	if limit <= 0 {
		return true
	}
	for {
		cur := e.inFlight.Load()
		if cur >= int32(limit) {
			return false
		}
		if e.inFlight.CompareAndSwap(cur, cur+1) {
			return true
		}
	}
}

// Release 归还一个在途名额：CAS 自减并 clamp 到 0（重复调用安全）。
func (p *Pool) Release(uid string) {
	e := p.entryOf(uid)
	if e == nil {
		return
	}
	for {
		cur := e.inFlight.Load()
		if cur <= 0 {
			return
		}
		if e.inFlight.CompareAndSwap(cur, cur-1) {
			return
		}
	}
}

// InFlight 返回当前在途数。
func (p *Pool) InFlight(uid string) int {
	e := p.entryOf(uid)
	if e == nil {
		return 0
	}
	return int(e.inFlight.Load())
}

// inFlightFull 报告某条目是否已达在途上限（调用方需持 p.mu 或已取快照）。
func (p *Pool) inFlightFull(e *entry) bool {
	limit := int(p.maxInFlight.Load())
	if limit <= 0 {
		return false
	}
	return int(e.inFlight.Load()) >= limit
}

// closeFlusher 停掉后台 flusher 并等待其退出（测试/进程退出用）。
func (p *Pool) closeFlusher() {
	p.stopOnce.Do(func() {
		if p.stopFlusher == nil {
			return
		}
		close(p.stopFlusher)
		<-p.flusherDone
	})
}

// markDirtyLocked 标记有变更待落盘（调用方持 p.mu 写锁）。
func (p *Pool) markDirtyLocked() {
	p.mutSeq++
	p.dirtySeq.Store(p.mutSeq)
}

func nowFunc() time.Time { return time.Now() }

// MaxInFlight 返回单账号在途上限（<=0 = 不限）。
func (p *Pool) MaxInFlight() int { return int(p.maxInFlight.Load()) }
