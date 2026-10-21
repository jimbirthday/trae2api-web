// 调优参数：默认值与归一化（零值回落默认，避免误配导致 0 冷却/0 阈值）。
package pool

import "time"

// Limits 池级调优参数（SCHEDULING.md §2.1）。
type Limits struct {
	MaxInFlight   int           // 单账号在途上限，0=不限（默认 2）
	SoftRateBase  time.Duration // 软冷却基数（默认 60s）
	SoftRateMax   time.Duration // 软冷却退避封顶（默认 30m）
	BreakerThresh int           // 连续失败触发熔断（默认 3）
	BreakerBase   time.Duration // 熔断基数（默认 10m）
	BreakerMax    time.Duration // 熔断封顶（默认 1h）
	DegradeThresh int           // 连败降权阈值（默认 5）
	DegradeWindow time.Duration // 降权时长（默认 10m）
	// CreditFloor 余额保底：credits < floor 的账号不参与选号（0 = 关闭）。
	// 用途是把「快跑完的号」提前摘出流量，留给签到/月初重置回血，
	// 避免打到 4008 配额超限才被动冷却。
	CreditFloor int64
}

const (
	defaultMaxInFlight   = 2
	defaultSoftRateBase  = 60 * time.Second
	defaultSoftRateMax   = 30 * time.Minute
	defaultBreakerThresh = 3
	defaultBreakerBase   = 10 * time.Minute
	defaultBreakerMax    = time.Hour
	defaultDegradeThresh = 5
	defaultDegradeWindow = 10 * time.Minute
	maxSoftStreakShift   = 16 // 退避位移上限，防 1<<streak 溢出
)

func defaultLimits() Limits {
	return Limits{
		MaxInFlight:   defaultMaxInFlight,
		SoftRateBase:  defaultSoftRateBase,
		SoftRateMax:   defaultSoftRateMax,
		BreakerThresh: defaultBreakerThresh,
		BreakerBase:   defaultBreakerBase,
		BreakerMax:    defaultBreakerMax,
		DegradeThresh: defaultDegradeThresh,
		DegradeWindow: defaultDegradeWindow,
	}
}

// normalizeLimits 把零/负的时长与阈值替换为默认值。
//
// MaxInFlight 例外：0 与负数都表示「不限」（契约 §2.4：maxInFlight<=0 时 Acquire
// 恒 true），不回落默认——默认 2 由 New 的 defaultLimits 提供。这样调用方可以显式
// 传 0 关停租约，而不必用哨兵值。
func normalizeLimits(l Limits) Limits {
	d := defaultLimits()
	if l.SoftRateBase <= 0 {
		l.SoftRateBase = d.SoftRateBase
	}
	if l.SoftRateMax <= 0 {
		l.SoftRateMax = d.SoftRateMax
	}
	if l.SoftRateBase > l.SoftRateMax {
		l.SoftRateBase = l.SoftRateMax
	}
	if l.BreakerThresh <= 0 {
		l.BreakerThresh = d.BreakerThresh
	}
	if l.BreakerBase <= 0 {
		l.BreakerBase = d.BreakerBase
	}
	if l.BreakerMax <= 0 {
		l.BreakerMax = d.BreakerMax
	}
	if l.BreakerBase > l.BreakerMax {
		l.BreakerBase = l.BreakerMax
	}
	if l.DegradeThresh <= 0 {
		l.DegradeThresh = d.DegradeThresh
	}
	if l.DegradeWindow <= 0 {
		l.DegradeWindow = d.DegradeWindow
	}
	return l
}

// breakerBackoff 计算熔断时长：BreakerBase * 2^(fails-1)，封顶 BreakerMax。
func (l Limits) breakerBackoff(fails int) time.Duration {
	if fails < 1 {
		fails = 1
	}
	s := fails - 1
	if s > maxSoftStreakShift {
		s = maxSoftStreakShift
	}
	d := l.BreakerBase << uint(s)
	if d > l.BreakerMax || d <= 0 {
		d = l.BreakerMax
	}
	return d
}
