// 选号：锁内筛候选并复制标量快照 → 锁外算权重排序 → 短写锁复核提交。
package pool

import (
	"math"
	"math/rand/v2"
	"sort"
	"time"

	"trae2api-web/internal/auth"
)

// topK 加权随机的候选池上限。
const pickTopK = 5

// maxPickAttempts 提交前复核失败（候选状态突变）时的重拍上限。
const maxPickAttempts = 3

// pickCandidate 锁内复制出的标量快照（不含指针，安全锁外使用）。
type pickCandidate struct {
	uid          string
	credits      int64
	manualWeight int
	lastUsedNano int64
	// degrade 为降权系数（1 = 正常，0.2 = 降权中）。
	degrade float64
	weight  float64
}

// pickScratch 每次选号独立的候选缓冲。
type pickScratch struct {
	cands []pickCandidate
}

// Pick 选一个可用账号；无可用返回 nil。
func (p *Pool) Pick() *auth.Auth { return p.PickExcludingForModel(nil, "") }

// PickExcluding 跳过 tried 中的 uid。
func (p *Pool) PickExcluding(tried map[string]bool) *auth.Auth {
	return p.PickExcludingForModel(tried, "")
}

// PickExcludingForModel 选号主入口：reqModel 非空时叠加该模型的冷却过滤。
func (p *Pool) PickExcludingForModel(tried map[string]bool, reqModel string) *auth.Auth {
	now := nowFunc()
	var scratch pickScratch
	var cands []pickCandidate
	for attempt := 0; attempt < maxPickAttempts; attempt++ {
		cands = p.snapshotCandidates(&scratch, tried, reqModel, now)
		if len(cands) == 0 {
			return nil
		}
		// 锁外：算权重、排序取 TopK、加权随机。
		pick := chooseWeighted(cands, p.randInt64N)
		if a := p.commitPick(pick.uid, reqModel); a != nil {
			return a
		}
		// 提交被拒（状态已变），重拍一次快照；now 与参数沿用同一刻度。
	}
	return nil
}

// snapshotCandidates 在短读锁内筛出候选并复制标量快照。
func (p *Pool) snapshotCandidates(scratch *pickScratch, tried map[string]bool, reqModel string, now time.Time) []pickCandidate {
	p.mu.RLock()
	defer p.mu.RUnlock()

	out := scratch.cands[:0]
	for uid, e := range p.byUID {
		if tried != nil && tried[uid] {
			continue
		}
		// 与 HasRotatable/粘性/模型可用性共用同一口径（见 routableLocked）。
		if !p.routableLocked(e, now, reqModel) {
			continue
		}
		out = append(out, pickCandidate{
			uid:          uid,
			credits:      e.credits,
			manualWeight: e.manualWeight(),
			lastUsedNano: nanoOf(e.lastUsed),
			degrade:      degradeFactor(e, now),
		})
	}
	scratch.cands = out
	return out
}

// commitPick 在短写锁内复核候选仍可用，然后提交 lastUsed/usedSeq 并返回凭证。
func (p *Pool) commitPick(uid, reqModel string) *auth.Auth {
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.byUID[uid]
	if !ok {
		return nil
	}
	now := nowFunc()
	if !p.routableLocked(e, now, reqModel) {
		return nil
	}
	p.mutSeq++
	e.usedSeq = p.mutSeq
	e.lastUsed = now
	// lastUsed 参与持久化（重启后闲置补偿才不失真），因此要标记 dirty。
	p.dirtySeq.Store(p.mutSeq)
	return e.a
}

// ---------------------------------------------------------------------------
// 统一的可用性判定
// ---------------------------------------------------------------------------
//
// 为什么要抽出来：选号、粘性命中、可轮换探测、「该模型是否全池不可用」四处
// 判定的口径必须完全一致。历史上这四处各写一份，结果是 ModelBlocked 没排
// paused/余额地板、HasRotatable/PickByUIDForModel 没看 CreditFloor——面板说
// 「有可用账号」而实际选不出号，或反过来误报「所有账号都对该模型冷却」。

// floorBlocked 余额低于保底（CreditFloor<=0 = 关闭该规则）。
func (p *Pool) floorBlocked(e *entry) bool {
	floor := p.limits.CreditFloor
	return floor > 0 && e.credits < floor
}

// baseUsableLocked 账号本身是否具备接流量的资格（不含模型级冷却、不含在途）。
// paused/disabled/开关关闭/账号级冷却/熔断/余额地板 全部排除。
func (p *Pool) baseUsableLocked(e *entry, now time.Time) bool {
	return e.healthy(now) && !p.floorBlocked(e)
}

// availableLocked 账号在该模型上可用（静态口径，不看在途）。
func (p *Pool) availableLocked(e *entry, now time.Time, model string) bool {
	return p.baseUsableLocked(e, now) && !e.modelCooled(now, model)
}

// routableLocked 账号此刻能否真的接一个请求：可用 + 在途未满。
func (p *Pool) routableLocked(e *entry, now time.Time, model string) bool {
	return p.availableLocked(e, now, model) && !p.inFlightFull(e)
}

// chooseWeighted 对候选算权重，按权重降序取 TopK，再在 TopK 内加权随机。
// 权重 = 余额因子 × 闲置补偿 × 手动权重 × 降权系数。
func chooseWeighted(cands []pickCandidate, randN func(int64) int64) pickCandidate {
	now := nowFunc()
	for i := range cands {
		c := &cands[i]
		c.weight = routeWeight(c, now)
	}
	sort.SliceStable(cands, func(i, j int) bool {
		if cands[i].weight != cands[j].weight {
			return cands[i].weight > cands[j].weight
		}
		// 同权重：更久未用的优先（闲置补偿的确定性兜底），再按 uid 稳定。
		if cands[i].lastUsedNano != cands[j].lastUsedNano {
			return cands[i].lastUsedNano < cands[j].lastUsedNano
		}
		return cands[i].uid < cands[j].uid
	})
	n := len(cands)
	if n > pickTopK {
		n = pickTopK
	}
	top := cands[:n]

	var total float64
	for i := range top {
		if top[i].weight > 0 {
			total += top[i].weight
		}
	}
	if total <= 0 {
		return top[0]
	}
	r := float64(randNOf(randN)(scaleForRand)) / float64(scaleForRand) * total
	var acc float64
	for i := range top {
		w := top[i].weight
		if w <= 0 {
			continue
		}
		acc += w
		if r < acc {
			return top[i]
		}
	}
	return top[len(top)-1]
}

// scaleForRand 加权随机取整刻度（1e6 精度足够避免偏差）。
const scaleForRand = 1 << 20

func randNOf(fn func(int64) int64) func(int64) int64 {
	if fn != nil {
		return fn
	}
	return func(n int64) int64 {
		if n <= 0 {
			return 0
		}
		return rand.Int64N(n)
	}
}

// routeWeight 计算单候选权重。
func routeWeight(c *pickCandidate, now time.Time) float64 {
	w := balanceFactor(c.credits)
	w *= idleFactor(c.lastUsedNano, now)
	w *= float64(c.manualWeight)
	w *= c.degrade
	if math.IsNaN(w) || w <= 0 {
		return 1e-9
	}
	return w
}

// balanceFactor 余额因子：log1p(credits/100)；credits<=0 给小的正基数
// （让「余额未知/已耗尽」的号仍有被选中的机会，避免全池饿死）。
func balanceFactor(credits int64) float64 {
	if credits <= 0 {
		return 0.15
	}
	return math.Log1p(float64(credits) / 100.0)
}

// idleFactor 闲置补偿：1 + min(idleHours*0.5, 2)。从未使用过给满分。
func idleFactor(lastUsedNano int64, now time.Time) float64 {
	if lastUsedNano == 0 {
		return 1 + 2
	}
	last := time.Unix(0, lastUsedNano)
	idleHours := now.Sub(last).Hours()
	if idleHours < 0 {
		idleHours = 0
	}
	f := 1 + math.Min(idleHours*0.5, 2)
	return f
}

// degradeFactor 降权系数：降权中 0.2，否则 1。
func degradeFactor(e *entry, now time.Time) float64 {
	if !e.degradeUntil.IsZero() && now.Before(e.degradeUntil) {
		return degradeFactorActive
	}
	return 1
}

func nanoOf(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}

// ---------------------------------------------------------------------------
// 粘性/可用性查询
// ---------------------------------------------------------------------------

// PickByUIDForModel 粘性命中路径：指定账号在该模型上当前可用则返回凭证，否则 nil。
//
// 与 Pick 的区别：不参与权重挑选（会话粘性优先）。但**必须**同样提交
// lastUsed/usedSeq —— 粘性请求同样是真实流量，账号确实被使用过。若不更新，
// 「最久未使用」维度对长期跑粘性会话的号永远是旧值，闲置补偿会给它虚高的权重。
//
// 过滤口径与主路径完全一致（paused/disabled/余额地板/冷却/在途全排掉），
// 否则会出现「粘性命中了一个主路径根本不会选的号」。
func (p *Pool) PickByUIDForModel(uid, model string) *auth.Auth {
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.byUID[uid]
	if !ok {
		return nil
	}
	now := nowFunc()
	if !p.routableLocked(e, now, model) {
		return nil
	}
	p.mutSeq++
	e.usedSeq = p.mutSeq
	e.lastUsed = now
	p.dirtySeq.Store(p.mutSeq)
	return e.a
}

// HasRotatable 是否存在未 tried 且当前可路由的账号（与 Pick 候选口径一致）。
func (p *Pool) HasRotatable(tried map[string]bool, reqModel string) bool {
	now := nowFunc()
	p.mu.RLock()
	defer p.mu.RUnlock()
	for uid, e := range p.byUID {
		if tried != nil && tried[uid] {
			continue
		}
		if !p.routableLocked(e, now, reqModel) {
			continue
		}
		return true
	}
	return false
}

// AvailableUIDs 返回当前可路由的账号 UID（稳定排序）。
func (p *Pool) AvailableUIDs() []string { return p.AvailableForModel("") }

// AvailableForModel 返回该模型上当前可路由的账号 UID（稳定排序）。
func (p *Pool) AvailableForModel(model string) []string {
	now := nowFunc()
	p.mu.RLock()
	out := make([]string, 0, len(p.byUID))
	for uid, e := range p.byUID {
		if !p.availableLocked(e, now, model) {
			continue
		}
		out = append(out, uid)
	}
	p.mu.RUnlock()
	sort.Strings(out)
	return out
}

// UIDAvailableForModel 报告单账号在该模型上当前是否可路由。
func (p *Pool) UIDAvailableForModel(uid, model string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	e, ok := p.byUID[uid]
	if !ok {
		return false
	}
	return p.availableLocked(e, nowFunc(), model)
}

// ModelBlocked 聚合查询：该模型是否在全池范围内都被冷却（无任何账号可用）。
// 单账号都不会被此判定影响——它只是给上层「该模型是否整体不可用」的提示。
func (p *Pool) ModelBlocked(model string) ModelBlockStatus {
	now := nowFunc()
	p.mu.RLock()
	defer p.mu.RUnlock()
	if model == "" || len(p.byUID) == 0 {
		return ModelBlockStatus{Model: model}
	}
	// 分母只算「本来就能接流量的账号」：paused/禁用/余额地板/账号级冷却中的号
	// 对任何模型都不可用，把它们算进分母会让「其他号正在冷却」掩盖成
	// 「该模型在全池被封锁」的误报（反之也会把真封锁算成没封锁）。
	var total, cooled int
	var until time.Time
	for _, e := range p.byUID {
		if !p.baseUsableLocked(e, now) {
			continue
		}
		total++
		if c, ok := e.modelCooldowns[model]; ok && now.Before(c.Until) {
			cooled++
			if until.IsZero() || c.Until.Before(until) {
				until = c.Until
			}
		}
	}
	return ModelBlockStatus{Model: model, Blocked: total > 0 && cooled == total, Until: until}
}
