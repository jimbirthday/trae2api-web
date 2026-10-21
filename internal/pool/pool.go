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
	"strconv"
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

	// leases 记录「在途租约」的所有权：token → uid。
	//
	// 为什么需要：旧实现只有每账号的原子计数，Release(uid) 谁调都能减一。
	// 于是 A 请求结束时的 Release("u1") 可能把 B 请求还没用完的名额释放掉，
	// 该账号的实际并发就会短暂超过上限（上游更容易 429）。
	// 现在每个 Acquire 发一个不可猜测的 token，Release 只认自己持有的那个。
	leaseSeq uint64
	leases   map[string]string
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
		leases:  map[string]string{},
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

// credentialChanged 报告「新旧凭证是否确实不同」。
//
// 冷却是「这个账号被打坏了」的结论，只有换了凭证（重登/换号）才失效。但启动时
// 从 state.json 恢复出来的占位 Auth 没有任何 token，此时 Add 用真实凭证替换它
// 显然不是「换号」——若在这条路径上也清锁，重启就等于把所有坏号洗白（正是
// 持久化要解决的问题）。因此只有「新旧都有凭证且至少一项不同」才算换号。
func credentialChanged(oldA, newA *auth.Auth) bool {
	if oldA == nil || newA == nil {
		return false
	}
	oldAccess, oldRefresh := oldA.AccessTokenValue(), oldA.RefreshTokenValue()
	newAccess, newRefresh := newA.AccessTokenValue(), newA.RefreshTokenValue()
	if newAccess == "" && newRefresh == "" {
		return false
	}
	if oldAccess == "" && oldRefresh == "" {
		return false
	}
	return oldAccess != newAccess || oldRefresh != newRefresh
}

// Add 加入账号。
//
// 已存在时：
//   - 凭证确实换了（重登/重新导入）→ 清掉自动锁（冷却/熔断/软退避/连败降权/
//     错误计数）。否则重登一个 session 失效的号，它依旧躺在 12h plan 冷却里，
//     运维会以为导入没生效。
//   - 凭证没变（启动恢复时用真实凭证替换占位）→ 保留全部状态。
//
// 两种情况都保留 tokenUsage（账号历史事实）与 credits（余额由上游权威给出），
// 也不动 enabled/paused（那是人的意图）。
func (p *Pool) Add(a *auth.Auth) {
	if a == nil || a.UID == "" {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if e, ok := p.byUID[a.UID]; ok {
		if credentialChanged(e.a, a) {
			e.clearAutoLocksLocked()
		}
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
	// 同时回收该账号尚未归还的租约：否则 leases 表会永久残留已删账号的条目，
	// 长跑（反复增删账号）会持续吃内存。
	for t, u := range p.leases {
		if u == uid {
			delete(p.leases, t)
		}
	}
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

// AcquireLease 抢占一个在途名额，返回租约令牌；满/账号不存在返回 ok=false。
// MaxInFlight<=0 时仍然发令牌（计数关闭，但租约所有权语义保留）。
//
// 新代码请用 ReleaseToken 归还。之所以不直接把 Acquire 改成双返回值：全仓库有
// 6 处调用点在 server 包，签名一改就会让与本次改动无关的包编译失败。因此保留
// Acquire(uid) bool 作为兼容壳，逐步迁移到 AcquireLease/ReleaseToken。
func (p *Pool) AcquireLease(uid string) (string, bool) {
	e := p.entryOf(uid)
	if e == nil {
		return "", false
	}
	limit := int(p.maxInFlight.Load())
	if limit > 0 {
		for {
			cur := e.inFlight.Load()
			if cur >= int32(limit) {
				return "", false
			}
			if e.inFlight.CompareAndSwap(cur, cur+1) {
				goto granted
			}
		}
	} else {
		e.inFlight.Add(1)
	}
granted:
	p.mu.Lock()
	p.leaseSeq++
	token := uid + "#" + strconv.FormatUint(p.leaseSeq, 10)
	p.leases[token] = uid
	p.mu.Unlock()
	return token, true
}

// Acquire 兼容壳：抢占名额但丢弃令牌（用 Release(uid) 归还）。
//
// 它仍然会在 leases 表里登记一个真实租约，因此 A 请求的 Release 不会误释放
// 属于另一个 uid 的租约；但同一 uid 上有并发请求时无法区分彼此，这正是需要
// 迁移到 AcquireLease/ReleaseToken 的原因。
func (p *Pool) Acquire(uid string) bool {
	_, ok := p.AcquireLease(uid)
	return ok
}

// ReleaseToken 归还自己持有的租约；未知/重复的 token 一律忽略（幂等）。
func (p *Pool) ReleaseToken(token string) {
	if token == "" {
		return
	}
	p.mu.Lock()
	uid, ok := p.leases[token]
	if ok {
		delete(p.leases, token)
	}
	p.mu.Unlock()
	if !ok {
		return
	}
	p.decInFlight(uid)
}

// Release 兼容壳：释放该 UID 的任意一个在途租约。
//
// 语义上不如 ReleaseToken 精确（它无法判断这个名额属于哪个请求），保留它只是
// 为了让尚未改造的调用点继续工作。新代码必须用 ReleaseToken。
func (p *Pool) Release(uid string) {
	p.mu.Lock()
	token := ""
	for t, u := range p.leases {
		if u == uid {
			token = t
			break
		}
	}
	if token != "" {
		delete(p.leases, token)
	}
	p.mu.Unlock()
	if token == "" {
		// 没有该 UID 的租约 = 重复释放或根本没 Acquire 过。
		// **绝不能**在这里盲减计数：重复 Release 会把同一账号上另一个请求的
		// 名额减掉，实际并发就会突破上限（旧实现正是这么漏的）。
		return
	}
	p.decInFlight(uid)
}

// decInFlight 把一个账号的在途数减一并 clamp 到 0。
func (p *Pool) decInFlight(uid string) {
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

// LeaseCount 当前未归还的租约数（诊断/测试用）。
func (p *Pool) LeaseCount() int {
	p.mu.RLock()
	n := len(p.leases)
	p.mu.RUnlock()
	return n
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
