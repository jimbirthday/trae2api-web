// 管理面板（/admin）：页面与全部 /admin/api/* 都要求持有 API Key（Bearer 或登录 cookie）。
//
// 详见 admingate.go：TW2A_API_KEY 为空时才退化为不鉴权（本地裸跑场景）。
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
	// 面板不允许被 iframe 嵌入（防点击劫持），并禁止 MIME 嗅探。
	w.Header().Set("X-Frame-Options", "DENY")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Referrer-Policy", "no-referrer")
	// 内联脚本/样式是面板的既定实现（无外部依赖），因此 CSP 只能放开 inline；
	// 但把 connect-src/frame-ancestors 收紧，仍能挡掉外部脚本注入与嵌套。
	w.Header().Set("Content-Security-Policy",
		"default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; "+
			"img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'")
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
				ac.Error = "ent_usage: " + redactUpstream(sanitizeErrMsg(err.Error()))
			} else {
				ac.Remain, ac.Limit, ac.Used, ac.Packs = remain, limit, used, packs
				// 回写 pool 的**余额**：不回写会让面板显示 4743 而选号权重仍按旧值计算。
				// 注意：这里是 GET 查询路径，绝不改冷却/熔断状态——否则「打开面板」
				// 就等于「给所有冷却号解冻」，一次误点就绕过了调度保护。
				total := limit
				if total < remain {
					total = remain
				}
				h.cfg.Pool.UpdateCreditsFromQuery(s.UID, remain, total)
			}
			checkedIn, credits, enable, cerr := h.cfg.Upstream.CheckinStatus(a)
			if cerr != nil {
				if ac.Error != "" {
					ac.Error += "; "
				}
				ac.Error += "checkin: " + redactUpstream(sanitizeErrMsg(cerr.Error()))
			} else {
				// 本地按天记录的签到状态一并纳入：上游偶尔不回 checked_in，
				// 本地日期戳能保证「今天签过」的号不会一直显示未签到。
				ac.CheckedIn = checkedIn || h.cfg.Pool.CheckinDoneToday(s.UID)
				ac.CheckinCredits, ac.CheckinEnable = credits, enable
			}
			out[i] = ac
		}(i, s)
	}
	wg.Wait()
	return out
}
