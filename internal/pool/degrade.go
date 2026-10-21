// 连败降权：NoteFailures 计数，达阈把权重降到 0.2；NoteSuccess 清零。
// 降权不等于出池：账号仍在候选集中（entry.healthy 不看 degradeUntil）。
package pool

// degradeFactorActive 降权中的权重系数。
const degradeFactorActive = 0.2

// NoteFailures 记录一次「不罚号」的失败：consecutiveFails++，达 DegradeThresh
// 置 degradeUntil=now+DegradeWindow 并清零（到期后需重新连败满阈值才再降）。
// 已在降权期内再次达阈：不延长（防重试把降权越堆越厚）。
func (p *Pool) NoteFailures(uid string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.byUID[uid]
	if !ok {
		return
	}
	e.consecutiveFails++
	if e.consecutiveFails < p.limits.DegradeThresh {
		p.markDirtyLocked()
		return
	}
	e.consecutiveFails = 0
	now := nowFunc()
	if !e.degradeUntil.IsZero() && now.Before(e.degradeUntil) {
		p.markDirtyLocked()
		return
	}
	e.degradeUntil = now.Add(p.limits.DegradeWindow)
	p.markDirtyLocked()
}
