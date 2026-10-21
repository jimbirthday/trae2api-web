// Package session 提供纯内存的会话粘性路由：同一会话键尽量落到同一账号。
//
// 设计要点（见 docs/SCHEDULING.md §3）：
//   - 命中走 RLock 快查，未命中/失效走写锁 re-check 后分配（防同 key 并发重复分配）；
//   - 分配时优先选择"当前没有任何绑定"的账号，其次全池哈希；
//   - LastActive 惰性续期（按 TTL/10 节流）+ 后台 GC 清理过期绑定；
//   - 绑定数量有上限，超出后按最久未活跃批量淘汰，避免被任意客户端输入撑爆内存。
//
// 本包不依赖 Redis、不依赖任何第三方库，也不感知上游协议。
package session

import (
	"hash/fnv"
	"sync"
	"time"
)

const (
	// defaultMaxBindings 绑定上限默认值。
	defaultMaxBindings = 50000
	// maxTouchInterval 惰性续期的最大间隔（TTL 很长时每分钟一次即可）。
	maxTouchInterval = time.Minute
	// evictBatch 超出上限时一次淘汰的比例（1/8），摊平淘汰成本。
	evictDivisor = 8
)

// Config 路由依赖。
//
// Available 返回"当前可用账号"的有序 uid 列表（由 pool.AvailableUIDs 提供）。
// AvailableForModel 按模型返回可用账号；CanRoute 是对单个 (model, uid) 的快路径判定。
// 三个回调均可为 nil：为 nil 时视为"未接线"，只做已绑定命中、不做可用性校验
// （避免误伤全部绑定）；但两个候选来源都为 nil 时无法分配新绑定。
type Config struct {
	TTL        time.Duration
	GCInterval time.Duration
	Available  func() []string
	// AvailableForModel 按请求模型返回可用账号；nil 时回落 Available。
	AvailableForModel func(model string) []string
	// CanRoute 命中路径的按账号判定；nil 时回落 AvailableForModel 的线性包含。
	CanRoute func(model, uid string) bool
	// MaxBindings 覆盖默认绑定上限（<=0 用默认值）。
	MaxBindings int
}

// entry 单条会话绑定。
type entry struct {
	uid        string
	lastActive time.Time
}

// Router 会话粘性路由器，并发安全。
type Router struct {
	mu    sync.RWMutex
	items map[string]entry
	// boundUIDs 记录每个 uid 当前绑定的会话数，用于"空闲账号优先"分配。
	boundUIDs map[string]int

	cfg        Config
	touchEvery time.Duration
	maxBinds   int

	stopOnce sync.Mutex
	stop     chan struct{}
}

// New 构建路由器。TTL/GCInterval 非正时取默认（30m / 5m）。
func New(cfg Config) *Router {
	if cfg.TTL <= 0 {
		cfg.TTL = 30 * time.Minute
	}
	if cfg.GCInterval <= 0 {
		cfg.GCInterval = 5 * time.Minute
	}
	touch := cfg.TTL / 10
	if touch > maxTouchInterval {
		touch = maxTouchInterval
	}
	if touch < time.Millisecond {
		touch = time.Millisecond
	}
	maxBinds := cfg.MaxBindings
	if maxBinds <= 0 {
		maxBinds = defaultMaxBindings
	}
	return &Router{
		items:      make(map[string]entry),
		boundUIDs:  make(map[string]int),
		cfg:        cfg,
		touchEvery: touch,
		maxBinds:   maxBinds,
	}
}

// StartGC 启动后台 GC（幂等）。stop channel 用局部变量捕获，保证 StopGC 一定能让
// goroutine 退出（避免每轮重新求值字段造成的泄漏）。
func (r *Router) StartGC() {
	r.stopOnce.Lock()
	defer r.stopOnce.Unlock()
	if r.stop != nil {
		return
	}
	stop := make(chan struct{})
	r.stop = stop
	go func() {
		t := time.NewTicker(r.cfg.GCInterval)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				r.gcOnce()
			}
		}
	}()
}

// StopGC 停止后台 GC；未启动时无副作用。等待 goroutine 退出，测试可据此判定无泄漏。
func (r *Router) StopGC() {
	r.stopOnce.Lock()
	stop := r.stop
	r.stop = nil
	r.stopOnce.Unlock()
	if stop == nil {
		return
	}
	close(stop)
}

// gcOnce 清理过期绑定。
func (r *Router) gcOnce() {
	now := time.Now()
	ttl := r.cfg.TTL
	r.mu.Lock()
	defer r.mu.Unlock()
	for k, e := range r.items {
		if now.Sub(e.lastActive) > ttl {
			r.removeLocked(k, e.uid)
		}
	}
}

func (r *Router) removeLocked(key, uid string) {
	delete(r.items, key)
	r.decBoundLocked(uid)
}

func (r *Router) decBoundLocked(uid string) {
	if n := r.boundUIDs[uid] - 1; n > 0 {
		r.boundUIDs[uid] = n
	} else {
		delete(r.boundUIDs, uid)
	}
}

// Resolve 返回 key 当前绑定的 uid；未绑定或绑定已失效时尝试分配。
// key 为空或没有可用账号时返回 false。
func (r *Router) Resolve(key string) (string, bool) {
	return r.resolve(key, "")
}

// ResolveForModel 与 Resolve 相同，但命中后校验该账号在 model 上仍可路由；
// 不可用则解绑并返回 false。CanRoute 优先，其次 AvailableForModel 线性包含；
// 两者都为 nil 时不做可用性校验。
func (r *Router) ResolveForModel(key, model string) (string, bool) {
	return r.resolve(key, model)
}

// resolve 是热路径：先用读锁判断命中，只有需要写（续期/解绑/新分配）时才升级写锁。
func (r *Router) resolve(key, model string) (string, bool) {
	if key == "" {
		return "", false
	}
	now := time.Now()

	r.mu.RLock()
	e, hit := r.items[key]
	r.mu.RUnlock()

	if hit {
		if now.Sub(e.lastActive) > r.cfg.TTL {
			// 过期：交给写锁分支统一清理后重新分配。
			hit = false
		} else if r.routeOK(model, e.uid) {
			// 惰性续期：按 touchEvery 节流，避免每个请求都抢写锁。
			if now.Sub(e.lastActive) >= r.touchEvery {
				r.touch(key, e.uid, now)
			}
			return e.uid, true
		} else {
			// 绑定仍新鲜但该账号当前不可路由 → 解绑。不做二次选号：选号要带权重，
			// 属于 pool 的职责，调用方收到 false 后会重新选号并 Bind。
			r.Unbind(key)
			return "", false
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	// 写锁内 re-check：并发场景下同 key 可能已被别的 goroutine 分配/更新。
	if cur, found := r.items[key]; found {
		if now.Sub(cur.lastActive) > r.cfg.TTL {
			r.removeLocked(key, cur.uid)
		} else if r.routeOK(model, cur.uid) {
			cur.lastActive = now
			r.items[key] = cur
			return cur.uid, true
		} else {
			r.removeLocked(key, cur.uid)
			return "", false
		}
	}
	uid, ok := r.allocateLocked(key, model)
	if !ok {
		return "", false
	}
	if len(r.items) >= r.maxBinds {
		r.evictLocked()
	}
	r.items[key] = entry{uid: uid, lastActive: now}
	r.boundUIDs[uid]++
	return uid, true
}

// touch 续期 lastActive；仅在绑定未变时生效。
func (r *Router) touch(key, uid string, now time.Time) {
	r.mu.Lock()
	if cur, ok := r.items[key]; ok && cur.uid == uid {
		cur.lastActive = now
		r.items[key] = cur
	}
	r.mu.Unlock()
}

// routeOK 判定绑定号在当前请求模型下是否仍可路由。未接线（回调全 nil）视为可用。
func (r *Router) routeOK(model, uid string) bool {
	if r.cfg.CanRoute != nil {
		return r.cfg.CanRoute(model, uid)
	}
	if r.cfg.AvailableForModel != nil {
		return containsUID(r.cfg.AvailableForModel(model), uid)
	}
	if model == "" && r.cfg.Available != nil {
		return containsUID(r.cfg.Available(), uid)
	}
	return true
}

// candidates 返回分配候选：模型维度优先，其次账号级；两者都未接线时返回 nil。
func (r *Router) candidates(model string) []string {
	if r.cfg.AvailableForModel != nil {
		return r.cfg.AvailableForModel(model)
	}
	if r.cfg.Available != nil {
		return r.cfg.Available()
	}
	return nil
}

// allocateLocked 在新分配或失效重绑时选号：先试"完全空闲"的账号，再退回全池哈希。
func (r *Router) allocateLocked(key, model string) (string, bool) {
	cands := r.candidates(model)
	if len(cands) == 0 {
		return "", false
	}
	h := hashKey(key)
	free := make([]string, 0, len(cands))
	for _, uid := range cands {
		if uid != "" && r.boundUIDs[uid] == 0 {
			free = append(free, uid)
		}
	}
	if len(free) > 0 {
		return free[int(h%uint64(len(free)))], true
	}
	return cands[int(h%uint64(len(cands)))], true
}

// evictLocked 绑定数达上限时按最久未活跃批量淘汰。
func (r *Router) evictLocked() {
	n := len(r.items) / evictDivisor
	if n < 1 {
		n = 1
	}
	// 取最近活跃时间分位阈值，避免全量排序。
	times := make([]time.Time, 0, len(r.items))
	for _, e := range r.items {
		times = append(times, e.lastActive)
	}
	sortTimes(times)
	cutoff := times[len(times)-n] // 第 n 新之后的时间点：<= 它的都淘汰
	for k, e := range r.items {
		if !e.lastActive.After(cutoff) {
			r.removeLocked(k, e.uid)
		}
	}
}

// sortTimes 小规模插入排序（淘汰时刻 n 通常很小，且只需分位不需要全序）。
func sortTimes(ts []time.Time) {
	for i := 1; i < len(ts); i++ {
		v := ts[i]
		j := i - 1
		for j >= 0 && ts[j].After(v) {
			ts[j+1] = ts[j]
			j--
		}
		ts[j+1] = v
	}
}

// Lookup 只查已有绑定，不分配：命中且当前可路由则返回并惰性续期；
// 绑定失效（不可路由）则解绑并返回 false。
//
// 与 Resolve 的分工：Resolve 自带「按可用性哈希分配」的兜底，但那个兜底不看
// 余额/闲置/权重，会把加权选号架空。需要权重生效的调用方应使用
// Lookup（只查） + pool 加权选号 + Bind（成功后回写）。
func (r *Router) Lookup(key, model string) (string, bool) {
	if key == "" {
		return "", false
	}
	now := time.Now()
	r.mu.RLock()
	e, hit := r.items[key]
	r.mu.RUnlock()
	if !hit {
		return "", false
	}
	if now.Sub(e.lastActive) > r.cfg.TTL {
		r.Unbind(key)
		return "", false
	}
	if !r.routeOK(model, e.uid) {
		r.Unbind(key)
		return "", false
	}
	if now.Sub(e.lastActive) >= r.touchEvery {
		r.touch(key, e.uid, now)
	}
	return e.uid, true
}

// Bind 显式绑定 key -> uid（覆盖已有绑定）。
func (r *Router) Bind(key, uid string) {
	if key == "" || uid == "" {
		return
	}
	now := time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	if old, ok := r.items[key]; ok {
		if old.uid == uid {
			old.lastActive = now
			r.items[key] = old
			return
		}
		r.removeLocked(key, old.uid)
	}
	if len(r.items) >= r.maxBinds {
		r.evictLocked()
	}
	r.items[key] = entry{uid: uid, lastActive: now}
	r.boundUIDs[uid]++
}

// Unbind 解除 key 的绑定，返回是否存在。
func (r *Router) Unbind(key string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.items[key]
	if !ok {
		return false
	}
	r.removeLocked(key, e.uid)
	return true
}

// Count 当前绑定数量。
func (r *Router) Count() int {
	r.mu.RLock()
	n := len(r.items)
	r.mu.RUnlock()
	return n
}

// BoundCount 返回某 uid 当前绑定的会话数（调试/面板用）。
func (r *Router) BoundCount(uid string) int {
	r.mu.RLock()
	n := r.boundUIDs[uid]
	r.mu.RUnlock()
	return n
}

func containsUID(list []string, uid string) bool {
	for _, v := range list {
		if v == uid {
			return true
		}
	}
	return false
}

func hashKey(key string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	return h.Sum64()
}
