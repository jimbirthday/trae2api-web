// 积分采样器：用账号级累计计数器，估算每个模型「每百万 token 消耗多少积分」。
//
// 为什么不能按单请求取差值：TRAE 对话流（event:token_usage）只给 token，不给积分。
// 积分只能从 /trae/api/v2/pay/ide_user_ent_usage 的 usage_summary.consumed_amount
// 读到，而那是**账号级累计值**，且**异步结算**——实测常见两种失真：
//
//   - 结算滞后：某次请求前后差值恰好为 0（这笔还没入账）；
//   - 结算补齐：某次差值把之前几笔一起补进来，出现远高于真实值的离群
//     （实测 kimi-k3 真实 69~100 积分/百万，却出现过单次 562 的差值）。
//
// 因此单请求差值不可用。本采样器改成**窗口归集**：
//
//	窗口开始 → 读计数器
//	  期间每个完成的请求：记录 (model, tokens)
//	窗口结束 → 再读计数器，得到窗口内积分增量 Δcredit
//	          按 token 比例把 Δcredit 摊到各模型：credit_m = Δcredit × tokens_m / tokens_all
//
// 这样分子分母始终是同源、同窗口的量：异步结算只是把入账时点推后，
// 窗口内「积分总量」与「token 总量」仍然对应，比值因此无偏。
package usage

import (
	"sync"
	"time"
)

// CreditReader 读账号级累计已消耗积分。由 server 注入（对接 upstream.ConsumedCredits）。
type CreditReader func(uid string) (consumed float64, ok bool)

// Allocator 接收一个窗口的分摊结果。
type Allocator func(model string, credits float64, tokens int64)

// Sampler 窗口式积分采样器，并发安全。
type Sampler struct {
	mu     sync.Mutex
	state  map[string]*windowState
	window time.Duration

	Read CreditReader

	// MaxWindowCredits 单个窗口的合理积分上限；超过视为账号被别处使用
	// （例如 IDE 同时在跑）导致污染，丢弃该窗口。
	MaxWindowCredits float64
}

// window 单账号的一个采样窗口。
type windowState struct {
	started    time.Time
	consumedAt float64 // 窗口开始时的累计值
	valid      bool    // 是否成功读到起始值
	tokens     map[string]int64
	total      int64
}

// NewSampler 构建。window<=0 时用 30s。
func NewSampler(read CreditReader, win time.Duration) *Sampler {
	if win <= 0 {
		win = 30 * time.Second
	}
	return &Sampler{
		state:            map[string]*windowState{},
		window:           win,
		Read:             read,
		MaxWindowCredits: 2000, // 单窗口 2000 积分已是极大量（实测 30s 窗口约 0.1~0.5）
	}
}

// Window 返回窗口时长（供 API 展示）。
func (s *Sampler) Window() time.Duration { return s.window }

// NoteCompletion 记录一次完成的请求。到达窗口边界时结算并回调 alloc。
//
// tokens 必须是该请求上游返回的 total_tokens；model 为其映射后的 config_name。
func (s *Sampler) NoteCompletion(uid, model string, tokens int64, alloc Allocator) {
	if s == nil || s.Read == nil || uid == "" || model == "" || tokens <= 0 {
		return
	}
	now := time.Now()

	s.mu.Lock()
	w, ok := s.state[uid]
	if !ok {
		// 开新窗口：立刻读一次计数器作为基线。
		consumed, valid := s.Read(uid)
		w = &windowState{started: now, consumedAt: consumed, valid: valid, tokens: map[string]int64{}}
		s.state[uid] = w
		w.tokens[model] += tokens
		w.total += tokens
		s.mu.Unlock()
		return
	}
	// 窗口内累计。注意：即便本次不结算也要记 token，下一窗口结算时才有分子分母对应。
	w.tokens[model] += tokens
	w.total += tokens
	if now.Sub(w.started) < s.window {
		s.mu.Unlock()
		return
	}
	// 到点结算：把这段窗口整体换出，锁外读计数器（网络调用，不能持锁）。
	startConsumed, valid, byModel, total := w.consumedAt, w.valid, w.tokens, w.total
	delete(s.state, uid) // 下一笔会开新窗口
	s.mu.Unlock()

	if !valid || total <= 0 {
		return
	}
	after, ok := s.Read(uid)
	if !ok {
		return
	}
	delta := after - startConsumed
	// 差值为负（计数器重置/跨天）或异常大（账号被别处使用）→ 丢弃整窗。
	if delta <= 0 || delta > s.MaxWindowCredits {
		return
	}
	if alloc == nil {
		return
	}
	// 按 token 比例分摊到模型。窗口内通常只有一两个模型，误差很小；
	// 长期累计后各模型自身的比值会收敛。
	for m, tk := range byModel {
		if tk <= 0 {
			continue
		}
		alloc(m, delta*float64(tk)/float64(total), tk)
	}
}

// Flush 结算所有未到点的窗口（进程退出前调用，避免丢尾巴）。
func (s *Sampler) Flush(alloc Allocator) {
	if s == nil || s.Read == nil || alloc == nil {
		return
	}
	s.mu.Lock()
	wins := s.state
	s.state = map[string]*windowState{}
	s.mu.Unlock()

	for uid, w := range wins {
		if !w.valid || w.total <= 0 {
			continue
		}
		after, ok := s.Read(uid)
		if !ok {
			continue
		}
		delta := after - w.consumedAt
		if delta <= 0 || delta > s.MaxWindowCredits {
			continue
		}
		for m, tk := range w.tokens {
			if tk <= 0 {
				continue
			}
			alloc(m, delta*float64(tk)/float64(w.total), tk)
		}
	}
}
