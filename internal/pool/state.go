// 状态迁移：开关/暂停/禁用/复活/额度/用量/签到/查询。
package pool

import (
	"sort"
	"time"

	"trae2api-web/internal/auth"
)

// SetEnabled 切换账号软开关；不影响 disabled（session 失效）状态。
func (p *Pool) SetEnabled(uid string, enabled bool, reason string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.byUID[uid]
	if !ok {
		return false
	}
	e.enabled = enabled
	if enabled {
		// 只清「手动关闭」写下的 reason；冷却/熔断的 reason 必须保留——历史实现在
		// 这里无条件清 reason，而 breakerUntil 还活着，于是面板显示「已启用」而
		// 选号依旧跳过该号，运维完全无法判断真实状态。
		if e.reasonIsManual {
			e.reason = ""
			e.reasonIsManual = false
		}
	} else if reason != "" && !e.hasActiveAutoLock(nowFunc()) {
		// 只在不处于冷却/熔断时才用「手动原因」覆盖 reason：否则会把「plan 权益
		// 不足」这类关键诊断信息抹掉，面板就再也看不出为什么不可用。
		e.reason = reason
		e.reasonIsManual = true
	}
	p.markDirtyLocked()
	return true
}

// Pause 暂停选号（不改变 enabled，不动冷却；面板「临时下线」）。
func (p *Pool) Pause(uid string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.byUID[uid]
	if !ok {
		return false
	}
	e.paused = true
	p.markDirtyLocked()
	return true
}

// Resume 解除暂停。
func (p *Pool) Resume(uid string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.byUID[uid]
	if !ok {
		return false
	}
	e.paused = false
	p.markDirtyLocked()
	return true
}

// Disable 硬禁用（session 失效终态），需重登或换文件后 Revive。
func (p *Pool) Disable(uid, reason string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.byUID[uid]
	if !ok {
		return
	}
	e.disabled = true
	e.disabledReason = reason
	// enabled 必须同步为 false：Status().Enabled 是对外契约的一部分，
	// 只翻 disabled 会出现「Enabled=true 且 Disabled=true」的自相矛盾状态。
	e.enabled = false
	// 不动 paused：那是运维显式下线的意图，session 失效不该顺手抹掉它。
	p.markDirtyLocked()
}

// Revive 人工复活：解除 session 终态禁用，并清掉全部「自动」锁
// （冷却/熔断/软退避/连败降权），因为运维点复活就是要它立刻能接流量。
//
// 但**不解除人工 Pause**：paused 是运维显式下线的意图，与 disabled（session
// 失效）和冷却（自动判定）语义不同。若复活顺手清 paused，一个被人工下线的号
// 会突然回到流量里——这正是「自动状态覆盖人工意图」的经典事故。
// 需要恢复流量的运维步骤仍是先 Resume（或直接调用 Resume 后再 Revive）。
func (p *Pool) Revive(uid string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.byUID[uid]
	if !ok {
		return false
	}
	e.disabled = false
	e.disabledReason = ""
	e.enabled = true
	e.clearAutoLocksLocked()
	p.markDirtyLocked()
	return true
}

// SetCredits 写回余额与总额度。
func (p *Pool) SetCredits(uid string, credits, total int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.byUID[uid]
	if !ok {
		return
	}
	e.credits = credits
	if total > 0 {
		e.creditsTotal = total
	}
	p.markDirtyLocked()
}

// SetCreditsDetailed 写回余额详情。expiring/earliest* 目前只用于未来路由扩展，
// 不参与权重计算；保留签名以兼容额度查询结果的完整透传。
func (p *Pool) SetCreditsDetailed(uid string, credits, total, expiring int64, earliestAt time.Time, earliestRemaining int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.byUID[uid]
	if !ok {
		return
	}
	e.credits = credits
	if total > 0 {
		e.creditsTotal = total
	}
	_ = expiring
	_ = earliestAt
	_ = earliestRemaining
	p.markDirtyLocked()
}

// ReenableIfCredits 写回余额，并在「额度回血」时解除**额度型**冷却，然后返回
// 是否真的解除了冷却。
//
// 名字有历史包袱：它曾经在 remain>0 时清掉一切自动锁（冷却/熔断/软退避/连败降权），
// 而这个方法被 GET /admin/api/credits（纯查询）调用——等于每查一次余额就把真实的
// 429 冷却与熔断白送掉，上游立刻被打穿。现在语义收窄为：
//
//   - 只有 coolKind == plan_limit（1005 权益不足 → 签到/重置回血）这类
//     「余额耗尽型」冷却，才允许因回血而解除；
//   - 失败率类信号（errCount/softStreak/breaker*/consecutiveFails/degradeUntil）
//     与余额无关，一律保留。
//
// 纯查询场景请改用 UpdateCreditsFromQuery（不做任何状态迁移）。
func (p *Pool) ReenableIfCredits(uid string, remain int64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.byUID[uid]
	if !ok {
		return false
	}
	e.credits = remain
	cleared := false
	if remain > 0 && !e.disabled {
		cleared = e.clearCreditCooldownLocked()
	}
	p.markDirtyLocked()
	return cleared
}

// UpdateCreditsFromQuery 用于「被动刷新余额」（面板/额度查询等 GET 路径）：
// 只更新余额数值，**不动**任何冷却/熔断/降级/开关状态。
//
// 为什么必须区分：GET 是幂等只读语义，不该有副作用。把「查询到的余额」当成
// 「账号已恢复」的直接证据，会让面板刷新变成绕过限流的后门。
func (p *Pool) UpdateCreditsFromQuery(uid string, credits, total int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.byUID[uid]
	if !ok {
		return
	}
	e.credits = credits
	if total > 0 {
		e.creditsTotal = total
	}
	p.markDirtyLocked()
}

// checkinDateLayout 签到日期格式（本地时区）。用日期而非 UTC 时间戳，
// 是因为「今天签到了吗」是运营视角的本地日历语义。
const checkinDateLayout = "2006-01-02"

// NoteCheckinDone 记录签到完成日期（本地时区 YYYY-MM-DD）。
func (p *Pool) NoteCheckinDone(uid string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if e, ok := p.byUID[uid]; ok {
		e.checkinDone = nowFunc().Format(checkinDateLayout)
		p.markDirtyLocked()
	}
}

// CheckinDoneToday 报告今天是否已签到（跨天后自动为 false）。
func (p *Pool) CheckinDoneToday(uid string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	e, ok := p.byUID[uid]
	if !ok {
		return false
	}
	return e.checkinDone == nowFunc().Format(checkinDateLayout)
}

// NoteUsage 只累计 TokenUsage 统计，不自行扣减余额（余额以上游查询为准）。
func (p *Pool) NoteUsage(uid string, delta TokenUsageDelta) {
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.byUID[uid]
	if !ok {
		return
	}
	u := &e.tokenUsage
	u.RequestCount++
	if delta.HasUsage {
		u.PromptTokens += delta.PromptTokens
		u.CompletionTokens += delta.CompletionTokens
		u.TotalTokens += delta.TotalTokens
		u.CacheReadTokens += delta.CacheReadTokens
		u.ReasoningTokens += delta.ReasoningTokens
	}
	if delta.LatencyMs > 0 {
		u.LastLatencyMs = delta.LatencyMs
	}
	u.LastUsedAt = nowFunc()
	if delta.Model != "" {
		u.LastModel = delta.Model
	}
	p.markDirtyLocked()
}

// Status 查询单账号状态。
func (p *Pool) Status(uid string) (Status, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	e, ok := p.byUID[uid]
	if !ok {
		return Status{}, false
	}
	return statusOf(uid, e, nowFunc()), true
}

// List 返回所有账号状态（按 UID 排序，稳定输出）。
func (p *Pool) List() []Status {
	now := nowFunc()
	p.mu.RLock()
	uids := make([]string, 0, len(p.byUID))
	for uid := range p.byUID {
		uids = append(uids, uid)
	}
	sort.Strings(uids)
	out := make([]Status, 0, len(uids))
	for _, uid := range uids {
		out = append(out, statusOf(uid, p.byUID[uid], now))
	}
	p.mu.RUnlock()
	return out
}

// AuthByUID 返回账号完整凭证（调度器/运维接口用）。
func (p *Pool) AuthByUID(uid string) *auth.Auth {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if e, ok := p.byUID[uid]; ok {
		return e.a
	}
	return nil
}

// Counts 池内账号分类计数。
func (p *Pool) Counts() Counts {
	now := nowFunc()
	p.mu.RLock()
	defer p.mu.RUnlock()
	var c Counts
	for _, e := range p.byUID {
		c.Total++
		switch {
		case e.disabled:
			c.Disabled++
		case !e.enabled || e.paused:
			c.Paused++
		case !e.healthy(now):
			// 账号级冷却/熔断中。
			c.Cooling++
		case p.floorBlocked(e):
			// 没坏也没被关，只是余额低于保底：单独计数，
			// 避免面板把「不参与选号」误报成「健康可用」。
			c.BelowFloor++
		default:
			c.Healthy++
		}
	}
	return c
}

// statusOf 组装单账号对外状态（调用方需持 p.mu 或已快照）。
func statusOf(uid string, e *entry, now time.Time) Status {
	weight, note, nick := e.profileSnapshot()
	st := Status{
		UID:              uid,
		Nickname:         nick,
		Weight:           weight,
		Note:             note,
		HasTokens:        e.hasTokens(),
		Credits:          e.credits,
		CreditsTotal:     e.creditsTotal,
		Reason:           e.reason,
		SoftStreak:       e.softStreak,
		Realm:            "cn",
		Enterprise:       e.a != nil && e.a.IsEnterprise(),
		EnterpriseID:     e.a.EnterpriseID,
		Disabled:         e.disabled,
		DisabledReason:   e.disabledReason,
		Paused:           e.paused,
		Enabled:          e.enabled,
		SuccessCount:     e.successCount,
		ErrTotal:         e.errTotal,
		LastSuccessTime:  e.lastSuccess,
		LastErrTime:      e.lastErr,
		InFlight:         int(e.inFlight.Load()),
		BreakerFails:     e.breakerFails,
		BreakerUntil:     e.breakerUntil,
		ConsecutiveFails: e.consecutiveFails,
		ErrCount:         e.errCount,
		DegradeUntil:     e.degradeUntil,
		TokenUsage:       e.tokenUsage,
		CheckinDone:      now.Format(checkinDateLayout) == e.checkinDone && e.checkinDone != "",
	}
	if e.a != nil {
		st.Enterprise = e.a.IsEnterprise()
	}
	// 生效截止取账号级冷却与熔断的最远者（与 healthy 的或门同源）。
	// 降权不参与 Cooling：账号仍可被选中，只是权重降到 0.2（DegradeUntil 单列）。
	until := e.until
	if e.breakerUntil.After(until) {
		until = e.breakerUntil
	}
	if now.Before(until) && !e.disabled && e.enabled && !e.paused {
		st.Cooling = true
		st.Until = until
		st.CoolRemaining = int64(until.Sub(now).Seconds())
	}
	if st.Cooling && !now.Before(e.until) {
		st.CoolKind = "breaker"
	} else {
		st.CoolKind = e.coolKind
	}
	st.RateLimitedModels = e.pendingModelCools(now)
	sort.Slice(st.RateLimitedModels, func(i, j int) bool {
		return st.RateLimitedModels[i].Model < st.RateLimitedModels[j].Model
	})
	return st
}
