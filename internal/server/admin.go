// 管理面板（/admin）：只读查询，无鉴权（本地面板）；CLI 操作留待开发。
package server

import (
	_ "embed"
	"net/http"
	"sync"
	"time"

	"trae2api-web/internal/pool"
)

//go:embed admin.html
var adminPageHTML []byte

// adminPageGated 面板首页：未登录返回登录页，已登录返回控制台 HTML。
// 页面本身不含任何数据（数据全靠 /admin/api/* 拉取），所以未登录返回登录页
// 即可完全阻断「先看到页面」这条路。
func (h *Handler) adminPageGated(w http.ResponseWriter, r *http.Request) {
	if !h.adminAuthorized(r) {
		adminLoginHTML(w, http.StatusUnauthorized, "")
		return
	}
	h.adminPage(w, r)
}

// adminPage 返回内嵌 HTML 面板（深色简洁风，无外部依赖）。
func (h *Handler) adminPage(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(adminPageHTML)
}

// creditsCache 额度快照缓存：每次刷新要为每个账号打 2 个上游接口
// （EntUsage + CheckinStatus），N 个账号就是 2N 次上游调用。切 tab 就全量
// 重查会拖慢面板也浪费上游配额，所以这里缓存一小段时间。
type creditsCache struct {
	mu       sync.Mutex
	at       time.Time
	accounts []creditRow
	inflight *sync.WaitGroup // 同一时刻只允许一份刷新在跑
}

// creditsTTL 缓存有效期：面板切 tab 走缓存，用户点「刷新」强制穿透。
const creditsTTL = 60 * time.Second

// creditRow 面板额度卡片的一行。
type creditRow struct {
	UID            string `json:"uid"`
	Nickname       string `json:"nickname"`
	Remain         int64  `json:"remain"`
	Limit          int64  `json:"limit"`
	Used           int64  `json:"used"`
	Packs          int    `json:"packs"`
	CheckedIn      bool   `json:"checked_in"`
	CheckinCredits int64  `json:"checkin_credits"`
	CheckinEnable  bool   `json:"checkin_enable"`
	Cooling        bool   `json:"cooling"`
	Disabled       bool   `json:"disabled"`
	Error          string `json:"error,omitempty"`
}

// creditFetchConcurrency 同时打上游的账号数上限，避免一次性打出上百个请求。
const creditFetchConcurrency = 6

// adminCredits GET /admin/api/credits?page=&page_size=&refresh=1
//
// 命中缓存时直接返回（毫秒级）；?refresh=1 强制穿透重查。
// 分页只切返回结果，缓存始终是全量——这样翻页不会触发重新查上游。
func (h *Handler) adminCredits(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	page := clampInt(q.Get("page"), 1, 1, 1<<20)
	pageSize := clampInt(q.Get("page_size"), 30, 1, 200)
	force := q.Get("refresh") == "1" || q.Get("refresh") == "true"

	rows, at, cached := h.creditsSnapshot(force)
	total := len(rows)
	start := (page - 1) * pageSize
	if start > total {
		start = total
	}
	end := start + pageSize
	if end > total {
		end = total
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"fetched_at": at.Format("2006-01-02 15:04:05"),
		"cached":     cached,
		"total":      total,
		"page":       page,
		"page_size":  pageSize,
		"pages":      (total + pageSize - 1) / pageSize,
		"accounts":   rows[start:end],
	})
}

// creditsSnapshot 返回（可能是缓存的）全量额度快照，并报告是否为缓存。
//
// 关键点：同一时刻只允许一份刷新在跑。无论是切 tab 还是用户连点「实时查询」，
// 后来者都直接等前一份的结果——避免 N 个浏览器并发把上游打出 2N×M 次请求。
func (h *Handler) creditsSnapshot(force bool) ([]creditRow, time.Time, bool) {
	c := &h.credits
	c.mu.Lock()
	// 1) 有刷新在跑：等它，用它刚更新的结果。
	if wg := c.inflight; wg != nil {
		c.mu.Unlock()
		wg.Wait()
		c.mu.Lock()
		rows, at := c.accounts, c.at
		c.mu.Unlock()
		return rows, at, true
	}
	// 2) 缓存还新鲜且未强制刷新：直接用。
	if !force && c.accounts != nil && time.Since(c.at) < creditsTTL {
		rows, at := c.accounts, c.at
		c.mu.Unlock()
		return rows, at, true
	}
	// 3) 自己成为唯一的刷新者。
	wg := &sync.WaitGroup{}
	wg.Add(1)
	c.inflight = wg
	c.mu.Unlock()

	rows := h.fetchCredits()

	c.mu.Lock()
	c.accounts = rows
	c.at = time.Now()
	if c.inflight == wg { // 只清自己的标记，不要误清后来者
		c.inflight = nil
	}
	at := c.at
	c.mu.Unlock()
	wg.Done()
	return rows, at, false
}

// fetchCredits 并发查询全部账号的上游额度与签到状态（有并发上限）。
func (h *Handler) fetchCredits() []creditRow {
	st := h.cfg.Pool.List()
	out := make([]creditRow, len(st))
	sem := make(chan struct{}, creditFetchConcurrency)
	var wg sync.WaitGroup
	for i, s := range st {
		wg.Add(1)
		go func(i int, s pool.Status) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			a := h.cfg.Pool.AuthByUID(s.UID)
			if a == nil {
				out[i] = creditRow{UID: s.UID, Nickname: s.Nickname, Error: "no auth found"}
				return
			}
			ac := creditRow{UID: s.UID, Nickname: s.Nickname, Cooling: s.Cooling, Disabled: s.Disabled}
			if s.Disabled {
				// 已硬禁用的账号查上游只会拿到 4xx，白费配额且拖慢整页。
				ac.Error = "disabled"
				out[i] = ac
				return
			}
			remain, limit, used, packs, err := h.cfg.Upstream.EntUsage(a)
			if err != nil {
				ac.Error = "ent_usage: " + err.Error()
			} else {
				ac.Remain, ac.Limit, ac.Used, ac.Packs = remain, limit, used, packs
				// 回写 pool：查询额度是刷新调度状态的机会，不回写会导致面板显示
				// 4743 而选号权重仍按旧值（甚至 0）计算，且冷却号不会自动解冻。
				total := limit
				if total < remain {
					total = remain
				}
				h.cfg.Pool.SetCreditsDetailed(s.UID, remain, total, 0, time.Time{}, 0)
				h.cfg.Pool.ReenableIfCredits(s.UID, remain)
			}
			checkedIn, credits, enable, cerr := h.cfg.Upstream.CheckinStatus(a)
			if cerr != nil {
				if ac.Error != "" {
					ac.Error += "; "
				}
				ac.Error += "checkin: " + cerr.Error()
			} else {
				ac.CheckedIn, ac.CheckinCredits, ac.CheckinEnable = checkedIn, credits, enable
			}
			out[i] = ac
		}(i, s)
	}
	wg.Wait()
	return out
}
