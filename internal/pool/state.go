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
	if !enabled {
		if reason != "" {
			e.reason = reason
		}
	} else if !e.disabled && !nowFunc().Before(e.until) && e.reason != "" {
		// 重新启用时清掉「用户关闭」类 reason；冷却中的 reason 保留。
		e.reason = ""
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
	e.paused = false
	p.markDirtyLocked()
}

// Revive 解除 disabled（不清除人工暂停之外的其他状态）。
func (p *Pool) Revive(uid string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.byUID[uid]
	if !ok {
		return false
	}
	// 人工复活必须把账号彻底拉回可用态：只清 disabled 而保留冷却/熔断，
	// 面板点「复活」后账号仍旧选不中，运维会误以为功能失效。
	e.disabled = false
	e.disabledReason = ""
	e.enabled = true
	e.until = time.Time{}
	e.reason = ""
	e.coolKind = ""
	e.softStreak = 0
	e.errCount = 0
	e.breakerFails = 0
	e.breakerUntil = time.Time{}
	e.consecutiveFails = 0
	e.degradeUntil = time.Time{}
	e.paused = false
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

// ReenableIfCredits 签到回血后解冻：remain>0 且账号在冷却（非禁用）时恢复。
func (p *Pool) ReenableIfCredits(uid string, remain int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.byUID[uid]
	if !ok {
		return
	}
	e.credits = remain
	if remain > 0 && !e.disabled {
		e.until = time.Time{}
		e.reason = ""
		e.coolKind = ""
		e.errCount = 0
		e.softStreak = 0
		e.breakerUntil = time.Time{}
		e.breakerFails = 0
		e.degradeUntil = time.Time{}
		e.consecutiveFails = 0
	}
	p.markDirtyLocked()
}

// NoteCheckinDone 标记当日签到已完成（面板展示用，不参与选号）。
func (p *Pool) NoteCheckinDone(uid string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if e, ok := p.byUID[uid]; ok {
		e.checkinDone = true
		p.markDirtyLocked()
	}
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
		case e.healthy(now):
			c.Healthy++
		default:
			c.Cooling++
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
		CheckinDone:      e.checkinDone,
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
