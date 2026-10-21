package pool

import "trae2api-web/internal/auth"

// Peek 只读选号：返回一个当前可路由的账号，但**不提交** lastUsed/usedSeq。
//
// 用途：调用方只需要「随便拿一个健康号去打个只读请求」（典型场景是拉模型表
// get_detail_param），不该因为这种辅助调用就扰动真正的调度状态：
//   - 不写 lastUsed → 闲置补偿不被污染
//   - 不写 usedSeq → 轮换次序不被插队
//   - 不占在途租约 → 不需要 Release，也不会把号算作满载
//
// 与 Pick 的区别就是副作用：Pick 会提交选号状态，Peek 完全不写。挑选口径
// （候选过滤 + 权重 + TopK 加权随机）与 Pick 完全一致。
func (p *Pool) Peek(reqModel string) *auth.Auth {
	now := nowFunc()
	var scratch pickScratch
	for attempt := 0; attempt < maxPickAttempts; attempt++ {
		cands := p.snapshotCandidates(&scratch, nil, reqModel, now)
		if len(cands) == 0 {
			return nil
		}
		c := chooseWeighted(cands, p.randInt64N)
		if a := p.peekCommit(c.uid, reqModel); a != nil {
			return a
		}
	}
	return nil
}

// peekCommit 与 commitPick 同口径复核，但只读：不写 lastUsed/usedSeq。
func (p *Pool) peekCommit(uid, reqModel string) *auth.Auth {
	p.mu.RLock()
	defer p.mu.RUnlock()
	e, ok := p.byUID[uid]
	if !ok {
		return nil
	}
	if !e.healthyForModel(nowFunc(), reqModel) {
		return nil
	}
	if int(p.maxInFlight.Load()) > 0 && int(e.inFlight.Load()) >= int(p.maxInFlight.Load()) {
		return nil
	}
	return e.a
}
