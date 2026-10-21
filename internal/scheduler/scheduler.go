// Package scheduler 定时任务：每日签到 + 余额回写 + token 预刷新。
//
// 三条设计原则（都是踩过的坑）：
//  1. 「今天跑过了吗」必须持久化。旧实现只看「今天还剩下的整点」，进程在计划
//     时间之后重启就整天不跑——而重启往往正是需要补跑签到的时刻。
//  2. 每个账号一次上游调用，串行跑几百个号会跑十几分钟，期间无法取消、也看不到
//     进度。改为带上限的并发 + context 取消。
//  3. 签到回血只解「额度型冷却」，不碰熔断/软退避/连败降权（见 pool 侧注释）。
package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"trae2api-web/internal/logfmt"
	"trae2api-web/internal/pool"
	"trae2api-web/internal/upstream"
)

// 默认并发上限。上游对同一 IP 的高并发不友好，4008/429 会被放大；
// 4~6 是「跑得完且不被限流」的经验区间。
const defaultConcurrency = 4

// maxConcurrencyCap 上限保护：配置写错（比如 1000）也不会把上游打崩。
const maxConcurrencyCap = 32

// Config 调度器依赖。
type Config struct {
	Pool         *pool.Pool
	Upstream     *upstream.Client
	CheckinHour  int           // 每日签到小时，默认 9
	RefreshHours []int         // token 预刷新小时，默认 [3]
	BalanceHours []int         // 余额回写小时，默认 [8,12,18,23]
	RefreshSkew  time.Duration // 预刷新窗口，默认 24h
	// Concurrency 并发账号数上限（默认 4，上限 32）。
	Concurrency int
	// StateFile 运行记录持久化路径（空 = 不持久化，重启后会重跑当天任务）。
	// 只记录「哪天跑过哪个任务」，不含任何凭证。
	StateFile string
}

// Scheduler 调度器。
type Scheduler struct {
	cfg Config

	// mu 保护 lastRun：Run 的定时分支与外部手动触发（HTTP 接口）会并发访问。
	mu      sync.Mutex
	lastRun map[string]string // taskKey → 本地日期 "2006-01-02"
}

// dateLayout 运行记录用的日期格式（本地时区）。
const dateLayout = "2006-01-02"

// New 构建。
func New(cfg Config) *Scheduler {
	if cfg.CheckinHour < 0 {
		cfg.CheckinHour = 9
	}
	if len(cfg.RefreshHours) == 0 {
		cfg.RefreshHours = []int{3}
	}
	if len(cfg.BalanceHours) == 0 {
		cfg.BalanceHours = []int{8, 12, 18, 23}
	}
	if cfg.RefreshSkew <= 0 {
		cfg.RefreshSkew = 24 * time.Hour
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = defaultConcurrency
	}
	if cfg.Concurrency > maxConcurrencyCap {
		cfg.Concurrency = maxConcurrencyCap
	}
	s := &Scheduler{cfg: cfg, lastRun: map[string]string{}}
	s.loadRunLog()
	return s
}

// nextFire 返回 now 之后最近的一个整点触发时间；hours 为本地小时（0-23）。
func nextFire(now time.Time, hours []int) time.Time {
	var earliest time.Time
	for _, h := range hours {
		t := time.Date(now.Year(), now.Month(), now.Day(), h, 0, 0, 0, now.Location())
		if !t.After(now) {
			t = t.Add(24 * time.Hour)
		}
		if earliest.IsZero() || t.Before(earliest) {
			earliest = t
		}
	}
	return earliest
}

// taskKind 任务类型。
type taskKind int

const (
	taskCheckin taskKind = iota
	taskRefresh
	taskBalance
)

// slot 一个计划触发点：某天某小时跑某任务。
type slot struct {
	key  string // 持久化键，例如 "balance:8"
	hour int
	kind taskKind
}

// slots 展开全部计划触发点（签到 + 各刷新小时 + 各余额小时）。
func (s *Scheduler) slots() []slot {
	out := make([]slot, 0, len(s.cfg.RefreshHours)+len(s.cfg.BalanceHours)+1)
	out = append(out, slot{key: "checkin", hour: s.cfg.CheckinHour, kind: taskCheckin})
	for _, h := range s.cfg.RefreshHours {
		out = append(out, slot{key: "refresh:" + itoa(h), hour: h, kind: taskRefresh})
	}
	for _, h := range s.cfg.BalanceHours {
		out = append(out, slot{key: "balance:" + itoa(h), hour: h, kind: taskBalance})
	}
	return out
}

func itoa(v int) string {
	if v == 0 {
		return "0"
	}
	var buf [8]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[i:])
}

// Run 主循环，阻塞直到 ctx 取消。
//
// 启动时先做一次 catch-up：今天已经过了计划时间但没跑过的任务立刻补跑。
// 否则「21:10 重启 → 09:00 的签到要等到明天」这类漏跑无人察觉。
func (s *Scheduler) Run(ctx context.Context) {
	s.catchUp(ctx)
	all := append([]int{}, s.cfg.RefreshHours...)
	all = append(all, s.cfg.BalanceHours...)
	all = append(all, s.cfg.CheckinHour)
	for {
		next := nextFire(time.Now(), all)
		timer := time.NewTimer(time.Until(next))
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
			now := time.Now()
			for _, sl := range s.slots() {
				if sl.hour != now.Hour() {
					continue
				}
				if s.ranToday(sl.key, now) {
					continue
				}
				s.runSlot(ctx, sl, now)
			}
		}
	}
}

// catchUp 补跑今天已经过期但未执行过的任务。
func (s *Scheduler) catchUp(ctx context.Context) {
	now := time.Now()
	for _, sl := range s.slots() {
		at := time.Date(now.Year(), now.Month(), now.Day(), sl.hour, 0, 0, 0, now.Location())
		// 只补「已过点」的；尚未到点的交给主循环。
		if at.After(now) || s.ranToday(sl.key, now) {
			continue
		}
		log.Printf("scheduler catch-up: %s (%s 未执行)", sl.key, at.Format("15:04"))
		s.runSlot(ctx, sl, now)
	}
}

// runSlot 按类型执行任务并记录运行日期。
//
// 只有「正常跑完」才记 ran：ctx 在跑的过程中被取消（进程要退出）时不能记账，
// 否则下次启动会认为今天已经跑过，任务就彻底漏了。
func (s *Scheduler) runSlot(ctx context.Context, sl slot, now time.Time) {
	switch sl.kind {
	case taskCheckin:
		s.runCheckin(ctx)
	case taskRefresh:
		s.runRefresh(ctx)
	case taskBalance:
		s.runBalance(ctx)
	}
	if ctx.Err() != nil {
		log.Printf("scheduler: %s 被取消，不记运行日期（下次启动会补跑）", sl.key)
		return
	}
	s.markRan(sl.key, now)
}

// ranToday 今天是否已执行过该任务。
func (s *Scheduler) ranToday(key string, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastRun[key] == now.Format(dateLayout)
}

// markRan 记录并落盘。
func (s *Scheduler) markRan(key string, now time.Time) {
	s.mu.Lock()
	if s.lastRun[key] == now.Format(dateLayout) {
		s.mu.Unlock()
		return
	}
	s.lastRun[key] = now.Format(dateLayout)
	snapshot := make(map[string]string, len(s.lastRun))
	for k, v := range s.lastRun {
		snapshot[k] = v
	}
	s.mu.Unlock()
	s.saveRunLog(snapshot)
}

// runLogFile 持久化结构。
type runLogFile struct {
	LastRun map[string]string `json:"last_run"`
}

// loadRunLog 读取运行记录（失败静默：宁可多跑一次，也不能因为读不到就完全不跑）。
func (s *Scheduler) loadRunLog() {
	if s.cfg.StateFile == "" {
		return
	}
	raw, err := os.ReadFile(s.cfg.StateFile)
	if err != nil {
		return
	}
	var f runLogFile
	if jerr := json.Unmarshal(raw, &f); jerr != nil {
		log.Printf("scheduler: 运行记录解析失败，忽略: %v", jerr)
		return
	}
	if f.LastRun != nil {
		s.lastRun = f.LastRun
	}
}

// saveRunLog 原子落盘（tmp + fsync + rename）。
func (s *Scheduler) saveRunLog(snapshot map[string]string) {
	if s.cfg.StateFile == "" {
		return
	}
	raw, err := json.MarshalIndent(runLogFile{LastRun: snapshot}, "", "  ")
	if err != nil {
		log.Printf("scheduler: 运行记录编码失败: %v", err)
		return
	}
	dir := filepath.Dir(s.cfg.StateFile)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			log.Printf("scheduler: 运行记录目录创建失败: %v", err)
			return
		}
	}
	tmp := s.cfg.StateFile + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		log.Printf("scheduler: 运行记录写入失败: %v", err)
		return
	}
	if _, err := f.Write(raw); err != nil {
		f.Close()
		os.Remove(tmp)
		log.Printf("scheduler: 运行记录写入失败: %v", err)
		return
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		log.Printf("scheduler: 运行记录落盘失败: %v", err)
		return
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return
	}
	if err := os.Rename(tmp, s.cfg.StateFile); err != nil {
		log.Printf("scheduler: 运行记录重命名失败: %v", err)
	}
}

// forEachAccount 以受限并发遍历账号；返回时所有任务都已结束（或 ctx 取消后提前返回）。
//
// 为什么要并发上限而不是全并发：签到/余额都是「同一上游、同一 IP」的批量调用，
// 一次放几百个请求必然触发限流，反而更慢，还会把正常请求的号打进冷却。
func (s *Scheduler) forEachAccount(ctx context.Context, fn func(ctx context.Context, st pool.Status)) {
	list := s.cfg.Pool.List()
	sem := make(chan struct{}, s.cfg.Concurrency)
	var wg sync.WaitGroup
	for _, st := range list {
		if ctx.Err() != nil {
			break
		}
		if st.Disabled {
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(st pool.Status) {
			defer wg.Done()
			defer func() { <-sem }()
			if ctx.Err() != nil {
				return
			}
			fn(ctx, st)
		}(st)
	}
	wg.Wait()
}

// RunCheckinNow 立即对所有账号执行签到 + 积分刷新 + 解冻（无取消，供 HTTP 接口
// 兼容调用）。新代码请用 RunCheckinContext。
func (s *Scheduler) RunCheckinNow() { s.runCheckin(context.Background()) }

// RunCheckinContext 带取消的签到；ctx 取消后尽快停止派发新账号。
func (s *Scheduler) RunCheckinContext(ctx context.Context) { s.runCheckin(ctx) }

// RunBalanceNow 立即刷新余额（无取消，兼容壳）。
func (s *Scheduler) RunBalanceNow() { s.runBalance(context.Background()) }

// RunBalanceContext 带取消的余额刷新。
func (s *Scheduler) RunBalanceContext(ctx context.Context) { s.runBalance(ctx) }

// RunRefreshNow 立即刷新 token（无取消，兼容壳）。
func (s *Scheduler) RunRefreshNow() { s.runRefresh(context.Background()) }

// RunRefreshContext 带取消的 token 预刷新。
func (s *Scheduler) RunRefreshContext(ctx context.Context) { s.runRefresh(ctx) }

// runCheckin 签到 + 积分刷新 + 额度型解冻。冷却中的账号也参与（签到就是为了解冻
// 它们）；硬禁用（session 失效）的跳过——它们连请求都发不出去。
func (s *Scheduler) runCheckin(ctx context.Context) {
	s.forEachAccount(ctx, func(ctx context.Context, st pool.Status) {
		a := s.cfg.Pool.AuthByUID(st.UID)
		if a == nil {
			return
		}
		label := logfmt.Label(st.UID, st.Nickname)
		checkedIn, _, enable, err := s.cfg.Upstream.CheckinStatus(a)
		switch {
		case err != nil:
			log.Printf("checkin status %s: %v", label, err)
		case checkedIn:
			log.Printf("checkin %s: already checked in", label)
			// 已签到也要记日期：否则面板永远显示「未签到」，且明天的判断没有依据。
			s.cfg.Pool.NoteCheckinDone(st.UID)
		case enable:
			if cerr := s.cfg.Upstream.CheckinClaim(a); cerr != nil {
				log.Printf("checkin claim %s: %v", label, cerr)
			} else {
				log.Printf("checkin %s: ok", label)
				s.cfg.Pool.NoteCheckinDone(st.UID)
			}
		}
		if ctx.Err() != nil {
			return
		}
		remain, limit, used, _, uerr := s.cfg.Upstream.EntUsage(a)
		if uerr != nil {
			log.Printf("ent-usage %s: %v", label, uerr)
			return
		}
		total := limit
		if total <= 0 {
			total = remain + used
		}
		s.cfg.Pool.SetCreditsDetailed(st.UID, remain, total, 0, time.Time{}, 0)
		// 只有「签到拿到额度」才允许解除额度型冷却（见 pool.ReenableIfCredits 注释）。
		s.cfg.Pool.ReenableIfCredits(st.UID, remain)
	})
}

// runBalance 只刷新余额（不签到），把最新积分回写 pool 供加权选号使用。
func (s *Scheduler) runBalance(ctx context.Context) {
	s.forEachAccount(ctx, func(ctx context.Context, st pool.Status) {
		a := s.cfg.Pool.AuthByUID(st.UID)
		if a == nil {
			return
		}
		remain, limit, used, _, err := s.cfg.Upstream.EntUsage(a)
		if err != nil {
			log.Printf("balance %s: %v", logfmt.Label(st.UID, st.Nickname), err)
			return
		}
		total := limit
		if total <= 0 {
			total = remain + used
		}
		// 纯余额刷新：不动冷却状态（与 GET 查询同口径）。
		s.cfg.Pool.UpdateCreditsFromQuery(st.UID, remain, total)
	})
}

// runRefresh 刷新 token；session 失效的自动禁用。
func (s *Scheduler) runRefresh(ctx context.Context) {
	s.forEachAccount(ctx, func(ctx context.Context, st pool.Status) {
		a := s.cfg.Pool.AuthByUID(st.UID)
		if a == nil || a.RefreshTokenValue() == "" {
			return
		}
		if !a.NeedsRefresh(s.cfg.RefreshSkew) {
			return
		}
		label := logfmt.Label(st.UID, st.Nickname)
		if err := s.cfg.Upstream.RefreshToken(a); err != nil {
			log.Printf("refresh %s: %v", label, err)
			var ue *upstream.Error
			if errors.As(err, &ue) && ue.Kind == upstream.ErrSessionDead {
				s.cfg.Pool.Disable(st.UID, "session dead")
			}
			return
		}
		// 同 chat 路径：refreshToken 已在上游轮换，落盘失败意味着重启后这个号
		// 就废了（旧 refreshToken 立即失效）。瞬时 IO 抖动很常见，先重试一次；
		// 仍失败必须打成显眼告警，否则这种「沉默的账号报废」没人会发现。
		if err := a.SaveAtomic(); err != nil {
			log.Printf("ERROR: refresh %s 落盘失败，重试: %v", label, err)
			if err2 := a.SaveAtomic(); err2 != nil {
				log.Printf("ERROR: !!! refresh %s 落盘仍失败: %v —— refreshToken 已轮换，"+
					"重启后需重新登录该账号", label, err2)
			}
		}
	})
}
