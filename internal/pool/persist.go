// state.json 持久化：兼容旧格式、原子写、dirty + 5s 后台 flusher。
//
// 并发纪律：写盘必须在 p.mu 之外完成。saveMu 只包住「采集快照之后的写文件」，
// 绝不与 p.mu 形成嵌套。
package pool

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"time"

	"trae2api-web/internal/auth"
)

// flushInterval 后台 flusher 周期（可变，便于测试缩短）。
var flushInterval = 5 * time.Second

// persistLogEvery 连续落盘失败每 N 次打一条提醒，避免刷屏。
const persistLogEvery = 12

// stateEntry state.json 单账号条目。指针字段用于区分「旧文件缺省」与「显式 false」。
type stateEntry struct {
	Credits      int64 `json:"credits"`
	CreditsTotal int64 `json:"credits_total,omitempty"`

	Disabled       bool   `json:"disabled"`
	DisabledReason string `json:"disabled_reason,omitempty"`
	Enabled        *bool  `json:"enabled,omitempty"`
	Paused         bool   `json:"paused,omitempty"`

	Reason   string    `json:"reason,omitempty"`
	Until    time.Time `json:"until,omitempty"`
	CoolKind string    `json:"cool_kind,omitempty"`

	SoftStreak   int       `json:"soft_streak,omitempty"`
	BreakerFails int       `json:"breaker_fails,omitempty"`
	BreakerUntil time.Time `json:"breaker_until,omitempty"`

	ConsecutiveFails int       `json:"consecutive_fails,omitempty"`
	DegradeUntil     time.Time `json:"degrade_until,omitempty"`

	SuccessCount int64 `json:"success_count,omitempty"`
	ErrTotal     int64 `json:"err_total,omitempty"`

	CheckinDone    bool                 `json:"checkin_done,omitempty"`
	TokenUsage     TokenUsage           `json:"token_usage,omitempty"`
	ModelCooldowns map[string]modelCool `json:"model_cooldowns,omitempty"`
}

// stateFile 持久化文件根。旧文件只有 {"accounts": {...}}，兼容。
type stateFile struct {
	Accounts map[string]stateEntry `json:"accounts"`
}

// load 读取 state.json（失败静默，池从空状态起步）。
func (p *Pool) load() {
	raw, err := os.ReadFile(p.stateFp)
	if err != nil {
		return
	}
	var sf stateFile
	if jerr := json.Unmarshal(raw, &sf); jerr != nil {
		// 损坏文件不覆盖、不删除：保持原样交给运维判断。
		log.Printf("pool: state.json 解析失败，忽略: %v", jerr)
		return
	}
	for uid, s := range sf.Accounts {
		enabled := true // 旧文件无 enabled 字段 → 默认启用
		if s.Enabled != nil {
			enabled = *s.Enabled
		}
		e := &entry{
			// placeholder：Add/SyncToDir 时换成完整凭证。
			a:                &auth.Auth{UID: uid},
			credits:          s.Credits,
			creditsTotal:     s.CreditsTotal,
			disabled:         s.Disabled,
			disabledReason:   s.DisabledReason,
			enabled:          enabled,
			paused:           s.Paused,
			reason:           s.Reason,
			until:            s.Until,
			coolKind:         s.CoolKind,
			softStreak:       s.SoftStreak,
			breakerFails:     s.BreakerFails,
			breakerUntil:     s.BreakerUntil,
			consecutiveFails: s.ConsecutiveFails,
			degradeUntil:     s.DegradeUntil,
			successCount:     s.SuccessCount,
			errTotal:         s.ErrTotal,
			checkinDone:      s.CheckinDone,
			tokenUsage:       s.TokenUsage,
			modelCooldowns:   map[string]modelCool{},
		}
		for m, c := range s.ModelCooldowns {
			e.modelCooldowns[m] = c
		}
		p.byUID[uid] = e
	}
}

// snapshotLocked 采集状态快照（调用方持 p.mu 读锁）。
func (p *Pool) snapshotLocked() stateFile {
	sf := stateFile{Accounts: make(map[string]stateEntry, len(p.byUID))}
	for uid, e := range p.byUID {
		se := stateEntry{
			Credits:          e.credits,
			CreditsTotal:     e.creditsTotal,
			Disabled:         e.disabled,
			DisabledReason:   e.disabledReason,
			Paused:           e.paused,
			Reason:           e.reason,
			Until:            e.until,
			CoolKind:         e.coolKind,
			SoftStreak:       e.softStreak,
			BreakerFails:     e.breakerFails,
			BreakerUntil:     e.breakerUntil,
			ConsecutiveFails: e.consecutiveFails,
			DegradeUntil:     e.degradeUntil,
			SuccessCount:     e.successCount,
			ErrTotal:         e.errTotal,
			CheckinDone:      e.checkinDone,
			TokenUsage:       e.tokenUsage,
		}
		// 仅软关闭时写 enabled=false；true 省略，旧版本读为 true。
		if !e.enabled {
			f := false
			se.Enabled = &f
		}
		if len(e.modelCooldowns) > 0 {
			se.ModelCooldowns = make(map[string]modelCool, len(e.modelCooldowns))
			for m, c := range e.modelCooldowns {
				se.ModelCooldowns[m] = c
			}
		}
		sf.Accounts[uid] = se
	}
	return sf
}

// Flush 立即落盘（进程退出/测试用）。无待写变更时直接返回。
func (p *Pool) Flush() {
	if p.stateFp == "" {
		return
	}
	p.flushOnce()
}

// flushOnce 采集快照并写盘；写文件全程在锁外。
func (p *Pool) flushOnce() {
	p.mu.RLock()
	seq := p.mutSeq
	snap := p.snapshotLocked()
	p.mu.RUnlock()

	raw, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		log.Printf("pool: state.json 编码失败: %v", err)
		return
	}

	p.saveMu.Lock()
	defer p.saveMu.Unlock()
	// 丢弃「更晚采集、更早落盘」的乱序覆盖：已写过更新的序号则跳过。
	if seq <= p.writtenSeq.Load() {
		return
	}
	if err := writeFileAtomic(p.stateFp, raw); err != nil {
		persistFailLog()
		return
	}
	persistFails = 0
	p.writtenSeq.Store(seq)
}

var persistFails int

func persistFailLog() {
	persistFails++
	if persistFails%persistLogEvery == 1 {
		log.Printf("pool: state.json 落盘失败（第 %d 次）", persistFails)
	}
}

// writeFileAtomic tmp + fsync + rename，权限 0600。
func writeFileAtomic(fp string, raw []byte) error {
	if dir := filepath.Dir(fp); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	tmp := fp + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(raw); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, fp)
}

// startFlusher 启动后台 flusher：有 dirty 时按 flushInterval 落盘。
func (p *Pool) startFlusher() {
	p.stopFlusher = make(chan struct{})
	p.flusherDone = make(chan struct{})
	go func() {
		defer close(p.flusherDone)
		t := time.NewTicker(flushInterval)
		defer t.Stop()
		for {
			select {
			case <-p.stopFlusher:
				p.flushOnce()
				return
			case <-t.C:
				// 用「未落盘的最高变更序号」判定，避免清除 dirty 时丢掉
				// 写盘期间发生的新变更。
				if p.dirtySeq.Load() > p.writtenSeq.Load() {
					p.flushOnce()
				}
			}
		}
	}()
}
