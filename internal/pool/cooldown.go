// 冷却 / 熔断 / 模型级冷却 / 成功清零。
package pool

import "time"

// modelKindUnavailable CooldownModel 写入的默认 kind（上层用 upstream 的规则表
// 判定具体类别时可改写为 rate_limit / unknown_model）。
const modelKindUnavailable = "model_unavailable"

// Cooldown 冷却账号至 now+d（覆盖式：更短的冷却也会覆盖更长的？否——取更远截止）。
// 语义：同一次调用只延长不缩短，避免「软冷却覆盖 plan 长冷却」把号提前放出去。
func (p *Pool) Cooldown(uid string, kind CoolKind, d time.Duration, reason string) {
	if d <= 0 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.byUID[uid]
	if !ok {
		return
	}
	until := nowFunc().Add(d)
	// 已处于更远的冷却（例如 plan 12h）时不被短冷却缩短。
	if until.After(e.until) {
		e.until = until
		e.coolKind = kind.String()
		e.reason = reason
	} else if e.reason == "" {
		e.reason = reason
	}
	e.errCount = 0
	if kind != CoolSoft {
		// 非软冷却（plan/错误）不参与软退避计数。
		e.softStreak = 0
	}
	p.markDirtyLocked()
}

// CooldownSoftRate 软冷却，指数退避：base<<(streak-1)，封顶 SoftRateMax。
// resetAt 非零且更晚时取 resetAt（同样受封顶约束）。base<=0 时用 SoftRateBase。
func (p *Pool) CooldownSoftRate(uid string, base time.Duration, resetAt time.Time, reason string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.byUID[uid]
	if !ok {
		return
	}
	now := nowFunc()
	lim := p.limits
	if base <= 0 {
		base = lim.SoftRateBase
	}
	e.softStreak++
	shift := e.softStreak - 1
	if shift > maxSoftStreakShift {
		shift = maxSoftStreakShift
	}
	d := base << uint(shift)
	if d <= 0 || d > lim.SoftRateMax {
		d = lim.SoftRateMax
	}
	until := now.Add(d)
	if !resetAt.IsZero() && resetAt.After(until) {
		until = resetAt
	}
	// resetAt 同样受封顶约束。
	if maxUntil := now.Add(lim.SoftRateMax); until.After(maxUntil) {
		until = maxUntil
	}
	// 不缩短已有更远截止（例如同时处于 plan 长冷却）。
	if until.After(e.until) {
		e.until = until
		e.reason = reason
	}
	e.coolKind = CoolSoft.String()
	e.errCount = 0
	p.markDirtyLocked()
}

// CooldownModel 只冷却该模型，不影响账号级可用性；已有更长且未过期的条目不缩短。
func (p *Pool) CooldownModel(uid, model string, d time.Duration, reason string) {
	p.CooldownModelKind(uid, model, d, modelKindUnavailable, reason)
}

// CooldownModelKind 同 CooldownModel，额外指定 kind（rate_limit / model_unavailable /
// unknown_model）。上游错误规则表解析出具体类别时使用。
func (p *Pool) CooldownModelKind(uid, model string, d time.Duration, kind, reason string) {
	if model == "" || d <= 0 {
		return
	}
	if kind == "" {
		kind = modelKindUnavailable
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.byUID[uid]
	if !ok {
		return
	}
	if e.modelCooldowns == nil {
		e.modelCooldowns = map[string]modelCool{}
	}
	now := nowFunc()
	until := now.Add(d)
	if prev, ok := e.modelCooldowns[model]; ok && now.Before(prev.Until) && !until.After(prev.Until) {
		// 已有更长且未过期的条目：不缩短、不覆盖原因。
		return
	}
	e.modelCooldowns[model] = modelCool{Until: until, Kind: kind, Reason: reason}
	p.markDirtyLocked()
}

// NoteSuccess 成功请求：清错误计数、熔断、软退避与连败降权。
func (p *Pool) NoteSuccess(uid string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.byUID[uid]
	if !ok {
		return
	}
	e.errCount = 0
	e.breakerFails = 0
	e.breakerUntil = time.Time{}
	e.softStreak = 0
	e.consecutiveFails = 0
	e.degradeUntil = time.Time{}
	e.successCount++
	e.lastSuccess = nowFunc()
	p.markDirtyLocked()
}

// NoteError 累计 errCount，达 BreakerThresh 置熔断（连触指数升级，封顶 BreakerMax）。
func (p *Pool) NoteError(uid string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.byUID[uid]
	if !ok {
		return
	}
	now := nowFunc()
	e.errTotal++
	e.lastErr = now
	e.errCount++
	if e.errCount < p.limits.BreakerThresh {
		p.markDirtyLocked()
		return
	}
	e.errCount = 0
	e.breakerFails++
	d := p.limits.breakerBackoff(e.breakerFails)
	until := now.Add(d)
	// 熔断截止不缩短已有更远截止（例如同时处于 plan 长冷却）。
	if e.breakerUntil.Before(until) {
		e.breakerUntil = until
	}
	p.markDirtyLocked()
}

// NoteTransportError 记录一次「不罚号」的传输层失败（连接重置/超时）：
// 只累计 errTotal，不计入熔断与连败降权，因为失败原因可能不在账号侧。
func (p *Pool) NoteTransportError(uid string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.byUID[uid]
	if !ok {
		return
	}
	e.errTotal++
	e.lastErr = nowFunc()
	p.markDirtyLocked()
}
