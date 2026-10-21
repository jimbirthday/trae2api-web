// admin_ext.go 面板运维扩展接口：暂停/恢复/复活、权重备注、余额刷新、批量操作、
// 请求流水与指标、运行配置查看与热改。
package server

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"trae2api-web/internal/logfmt"
	"trae2api-web/internal/reqlog"
	"trae2api-web/internal/upstream"
)

// adminPauseAccount POST /admin/api/accounts/{uid}/pause：暂停选号（仍参与保号任务）。
func (h *Handler) adminPauseAccount(w http.ResponseWriter, r *http.Request) {
	uid := r.PathValue("uid")
	if !h.cfg.Pool.Pause(uid) {
		writeOpenAIError(w, http.StatusNotFound, "not_found", "account not found")
		return
	}
	h.writeAccountStatus(w, uid)
}

// adminResumeAccount POST /admin/api/accounts/{uid}/resume：恢复选号。
func (h *Handler) adminResumeAccount(w http.ResponseWriter, r *http.Request) {
	uid := r.PathValue("uid")
	if !h.cfg.Pool.Resume(uid) {
		writeOpenAIError(w, http.StatusNotFound, "not_found", "account not found")
		return
	}
	h.writeAccountStatus(w, uid)
}

// adminReviveAccount POST /admin/api/accounts/{uid}/revive：解除 session 终态禁用。
func (h *Handler) adminReviveAccount(w http.ResponseWriter, r *http.Request) {
	uid := r.PathValue("uid")
	if !h.cfg.Pool.Revive(uid) {
		writeOpenAIError(w, http.StatusNotFound, "not_found", "account not found")
		return
	}
	h.writeAccountStatus(w, uid)
}

// profileRequest PATCH 体：权重 / 备注。
type profileRequest struct {
	Weight *int    `json:"weight,omitempty"`
	Note   *string `json:"note,omitempty"`
}

// adminAccountProfile POST /admin/api/accounts/{uid}/profile：改权重/备注并落盘。
func (h *Handler) adminAccountProfile(w http.ResponseWriter, r *http.Request) {
	uid := r.PathValue("uid")
	var req profileRequest
	if err := decodeBody(w, r, &req); err != nil {
		return
	}
	a := h.cfg.Pool.AuthByUID(uid)
	if a == nil {
		writeOpenAIError(w, http.StatusNotFound, "not_found", "no auth for uid")
		return
	}
	if err := a.SetProfile(req.Weight, req.Note); err != nil {
		writeOpenAIError(w, http.StatusInternalServerError, "save_failed", err.Error())
		return
	}
	h.writeAccountStatus(w, uid)
}

// adminAccountBalance POST /admin/api/accounts/{uid}/balance：实时查额度并回写 pool。
func (h *Handler) adminAccountBalance(w http.ResponseWriter, r *http.Request) {
	uid := r.PathValue("uid")
	a := h.cfg.Pool.AuthByUID(uid)
	if a == nil {
		writeOpenAIError(w, http.StatusNotFound, "not_found", "no auth for uid")
		return
	}
	remain, limit, used, packs, err := h.cfg.Upstream.EntUsage(a)
	if err != nil {
		writeOpenAIError(w, http.StatusBadGateway, "ent_usage_failed", err.Error())
		return
	}
	total := limit
	if total <= 0 {
		total = remain + used
	}
	h.cfg.Pool.SetCreditsDetailed(uid, remain, total, 0, time.Time{}, 0)
	writeJSON(w, http.StatusOK, map[string]any{
		"uid": uid, "remain": remain, "limit": limit, "used": used, "packs": packs,
	})
}

// setAllRequest 批量启停/暂停。
type setAllRequest struct {
	Action string   `json:"action"` // enable|disable|pause|resume
	UIDs   []string `json:"uids,omitempty"`
}

// adminAccountsSetAll POST /admin/api/accounts/set_all：批量状态变更。
func (h *Handler) adminAccountsSetAll(w http.ResponseWriter, r *http.Request) {
	var req setAllRequest
	if err := decodeBody(w, r, &req); err != nil {
		return
	}
	targets := req.UIDs
	if len(targets) == 0 {
		for _, st := range h.cfg.Pool.List() {
			targets = append(targets, st.UID)
		}
	}
	changed := 0
	for _, uid := range targets {
		var ok bool
		switch strings.ToLower(strings.TrimSpace(req.Action)) {
		case "enable":
			ok = h.cfg.Pool.SetEnabled(uid, true, "")
		case "disable":
			ok = h.cfg.Pool.SetEnabled(uid, false, "bulk disable")
		case "pause":
			ok = h.cfg.Pool.Pause(uid)
		case "resume":
			ok = h.cfg.Pool.Resume(uid)
		default:
			writeOpenAIError(w, http.StatusBadRequest, "invalid_request", "action must be enable|disable|pause|resume")
			return
		}
		if ok {
			changed++
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"action": req.Action, "changed": changed})
}

// adminCheckinAll POST /admin/api/checkin_all：并发签到 + 余额回写。
func (h *Handler) adminCheckinAll(w http.ResponseWriter, r *http.Request) {
	type result struct {
		UID      string `json:"uid"`
		Nickname string `json:"nickname,omitempty"`
		Checked  bool   `json:"checked_in"`
		Credits  int64  `json:"credits"`
		Error    string `json:"error,omitempty"`
	}
	st := h.cfg.Pool.List()
	out := make([]result, len(st))
	var wg sync.WaitGroup
	for i, s := range st {
		if s.Disabled {
			out[i] = result{UID: s.UID, Nickname: s.Nickname, Error: "disabled"}
			continue
		}
		wg.Add(1)
		go func(i int, sIdx struct {
			UID      string
			Nickname string
		}) {
			defer wg.Done()
			a := h.cfg.Pool.AuthByUID(sIdx.UID)
			if a == nil {
				out[i] = result{UID: sIdx.UID, Nickname: sIdx.Nickname, Error: "no auth"}
				return
			}
			res := result{UID: sIdx.UID, Nickname: sIdx.Nickname}
			checked, _, enable, err := h.cfg.Upstream.CheckinStatus(a)
			if err != nil {
				res.Error = "status: " + err.Error()
			} else if !checked && enable {
				if cerr := h.cfg.Upstream.CheckinClaim(a); cerr != nil {
					res.Error = "claim: " + cerr.Error()
				} else {
					res.Checked = true
					h.cfg.Pool.NoteCheckinDone(sIdx.UID)
				}
			} else {
				res.Checked = checked
			}
			if remain, limit, used, _, eerr := h.cfg.Upstream.EntUsage(a); eerr == nil {
				total := limit
				if total <= 0 {
					total = remain + used
				}
				h.cfg.Pool.SetCreditsDetailed(sIdx.UID, remain, total, 0, time.Time{}, 0)
				h.cfg.Pool.ReenableIfCredits(sIdx.UID, remain)
				res.Credits = remain
			}
			out[i] = res
		}(i, struct {
			UID      string
			Nickname string
		}{s.UID, s.Nickname})
	}
	wg.Wait()
	writeJSON(w, http.StatusOK, map[string]any{"accounts": out})
}

// adminRequests GET /admin/api/requests?limit=&offset=&uid=&model=&outcome=
//
// 分页语义：offset 为「跳过最新的多少条」，offset=0 即最新一页。
// 返回 total/精确性标记，前端据此渲染页码。
func (h *Handler) adminRequests(w http.ResponseWriter, r *http.Request) {
	if h.cfg.RequestLog == nil {
		writeJSON(w, http.StatusOK, map[string]any{"events": []any{}, "enabled": false})
		return
	}
	q := r.URL.Query()
	limit := clampInt(q.Get("limit"), 50, 1, 500)
	offset := clampInt(q.Get("offset"), 0, 0, 1<<20)
	f := reqlog.Filter{UID: q.Get("uid"), Model: q.Get("model"), Outcome: q.Get("outcome")}

	// 归档是异步落盘的（有 flush 间隔），最新请求可能还在内存环形缓冲里，
	// 因此两份合并去重后再排序：否则面板会「看不到刚发的请求」。
	// 只需取前 offset+limit 条——全局前 K 条必落在「内存 + 归档前 K 条」里。
	window := offset + limit
	archived, err := h.cfg.RequestLog.ReadArchivePage(0, window, f)
	if err != nil {
		archived = nil
	}
	merged := mergeEvents(filterRecent(h.cfg.RequestLog.Recent(0), f), archived)
	total, exact := h.requestsTotal(f, len(merged))
	events := slicePage(merged, offset, limit)

	writeJSON(w, http.StatusOK, map[string]any{
		"enabled":     true,
		"events":      events,
		"total":       total,
		"total_exact": exact,
		"offset":      offset,
		"limit":       limit,
		"metrics":     h.cfg.RequestLog.Metrics(),
		"archive":     h.cfg.RequestLog.ArchiveStats(),
	})
}

// requestsTotal 估算可翻页的总条数，并说明是否精确。
//
// 无过滤条件时用「归档行数（带缓存）」与「进程累计计数」取大，精确；
// 带过滤条件时不做全量扫描，返回 -1 表示未知（前端退化为相对翻页）。
func (h *Handler) requestsTotal(f reqlog.Filter, windowLen int) (int, bool) {
	m := h.cfg.RequestLog.Metrics()
	if f != (reqlog.Filter{}) {
		if windowLen == 0 {
			return 0, true // 首页就空，必然一条都没有
		}
		return -1, false
	}
	n := int(m.Total)
	if archived, ok := h.cfg.RequestLog.ArchiveTotal(f); ok && int(archived) > n {
		n = int(archived)
	}
	return n, true
}

// slicePage 取 [offset, offset+limit) 一段（超界返回空切片）。
func slicePage(evs []reqlog.Event, offset, limit int) []reqlog.Event {
	if offset >= len(evs) {
		return []reqlog.Event{}
	}
	evs = evs[offset:]
	if limit > 0 && len(evs) > limit {
		evs = evs[:limit]
	}
	return evs
}

// mergeEvents 合并「内存最近事件」与「归档事件」，按 request_id 去重并按时间倒序。
// 内存侧优先（字段更完整，且包含尚未 flush 的最新请求）。
func mergeEvents(recent, archived []reqlog.Event) []reqlog.Event {
	seen := make(map[string]bool, len(recent)+len(archived))
	out := make([]reqlog.Event, 0, len(recent)+len(archived))
	for _, e := range recent {
		if e.RequestID != "" {
			if seen[e.RequestID] {
				continue
			}
			seen[e.RequestID] = true
		}
		out = append(out, e)
	}
	for _, e := range archived {
		if e.RequestID != "" {
			if seen[e.RequestID] {
				continue
			}
			seen[e.RequestID] = true
		}
		out = append(out, e)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Time.After(out[j].Time) })
	return out
}

// filterRecent 在内存事件上套用与归档相同的过滤语义（子串包含）。
func filterRecent(events []reqlog.Event, f reqlog.Filter) []reqlog.Event {
	out := make([]reqlog.Event, 0, len(events))
	contain := func(s, sub string) bool {
		return sub == "" || strings.Contains(strings.ToLower(s), strings.ToLower(sub))
	}
	for _, e := range events {
		if !contain(e.UID, f.UID) || !contain(e.Model, f.Model) || !contain(e.Outcome, f.Outcome) {
			continue
		}
		out = append(out, e)
	}
	return out
}

// clampInt 解析查询参数为整数并夹到 [min,max]；空串/非法值取 def。
func clampInt(raw string, def, min, max int) int {
	v, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return def
	}
	if v < min {
		return min
	}
	if v > max {
		return max
	}
	return v
}

// adminMetrics GET /admin/api/metrics：进程级请求指标。
func (h *Handler) adminMetrics(w http.ResponseWriter, r *http.Request) {
	if h.cfg.RequestLog == nil {
		writeJSON(w, http.StatusOK, map[string]any{"enabled": false})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"enabled": true,
		"metrics": h.cfg.RequestLog.Metrics(),
		"archive": h.cfg.RequestLog.ArchiveStats(),
		"counts":  h.cfg.Pool.Counts(),
	})
}

// adminGetConfig GET /admin/api/config：运行配置（脱敏，不回显 key）。
func (h *Handler) adminGetConfig(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"default_model":     h.cfg.DefaultModel,
		"max_rotate":        h.cfg.MaxRotate,
		"plan_cooldown":     h.cfg.PlanCooldown.String(),
		"soft_cooldown":     h.cfg.SoftCooldown.String(),
		"err_threshold":     h.cfg.ErrThreshold,
		"err_cooldown":      h.cfg.ErrCooldown.String(),
		"refresh_skew":      h.cfg.RefreshSkew.String(),
		"session_enabled":   h.cfg.Session != nil,
		"reqlog_enabled":    h.cfg.RequestLog != nil,
		"model_error_rules": h.cfg.Upstream.ModelErrorRules(),
	})
}

// adminSaveConfig POST /admin/api/config：保存有限运行参数到 config.json。
// 只落「可安全热改」的白名单字段；env 覆盖时以 env 为准（重启生效）。
func (h *Handler) adminSaveConfig(w http.ResponseWriter, r *http.Request) {
	var patch map[string]any
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&patch); err != nil {
		writeOpenAIError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	path := h.cfg.ConfigPath
	if path == "" {
		writeOpenAIError(w, http.StatusBadRequest, "no_config_path", "config path unknown")
		return
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		writeOpenAIError(w, http.StatusInternalServerError, "read_failed", err.Error())
		return
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		writeOpenAIError(w, http.StatusInternalServerError, "parse_failed", err.Error())
		return
	}
	applied := []string{}
	for k, v := range patch {
		switch k {
		case "default_model", "cooldown", "pool", "session", "reqlog", "schedule", "upstream":
			doc[k] = v
			applied = append(applied, k)
		}
	}
	out, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		writeOpenAIError(w, http.StatusInternalServerError, "marshal_failed", err.Error())
		return
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, out, 0o600); err != nil {
		writeOpenAIError(w, http.StatusInternalServerError, "write_failed", err.Error())
		return
	}
	if err := os.Rename(tmp, path); err != nil {
		writeOpenAIError(w, http.StatusInternalServerError, "rename_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"applied": applied,
		"note":    "已写入 config.json；需重启生效",
	})
}

func (h *Handler) writeAccountStatus(w http.ResponseWriter, uid string) {
	if st, ok := h.cfg.Pool.Status(uid); ok {
		writeJSON(w, http.StatusOK, st)
		return
	}
	writeOpenAIError(w, http.StatusNotFound, "not_found", "account not found")
}

// parsePositiveInt 解析正整数（1..5000）。
func parsePositiveInt(s string) (int, error) {
	var n int
	if _, err := fmtSscan(s, &n); err != nil {
		return 0, err
	}
	if n <= 0 {
		return 0, errInvalidInt
	}
	if n > 5000 {
		n = 5000
	}
	return n, nil
}

var errInvalidInt = tinyErr("invalid int")

type tinyErr string

func (e tinyErr) Error() string { return string(e) }

// fmtSscan 仅用于整数解析，避免为一行引入 fmt 依赖歧义。
func fmtSscan(s string, n *int) (int, error) {
	v := 0
	if len(s) == 0 {
		return 0, errInvalidInt
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, errInvalidInt
		}
		v = v*10 + int(c-'0')
		if v > 1000000 {
			return 0, errInvalidInt
		}
	}
	*n = v
	return 1, nil
}

// adminExportAccounts GET /admin/api/accounts/export
// 导出全量凭证（**含明文 token**），供备份/迁移。必须带 Bearer；不要把结果贴到公开渠道。
func (h *Handler) adminExportAccounts(w http.ResponseWriter, r *http.Request) {
	accounts := make([]map[string]any, 0)
	for _, st := range h.cfg.Pool.List() {
		a := h.cfg.Pool.AuthByUID(st.UID)
		if a == nil {
			continue
		}
		weight, note, nickname, _ := a.ProfileSnapshot()
		accounts = append(accounts, map[string]any{
			"format":      "trae2api-web-accounts",
			"version":     1,
			"exported_at": time.Now().Format(time.RFC3339),
			"auth": map[string]any{
				"accessToken":  a.JWT(),
				"refreshToken": a.RefreshTokenValue(),
				"expiresAt":    a.ExpiresAt,
				"domain":       a.Domain,
				"apiHost":      a.ApiHost,
				"machineId":    a.MachineID,
				"deviceId":     a.DeviceID,
			},
			"account": map[string]any{
				"uid":          st.UID,
				"nickname":     nickname,
				"enterpriseId": a.EnterpriseID,
				"weight":       weight,
				"note":         note,
			},
		})
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Disposition", `attachment; filename="trae2api-web-accounts.json"`)
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, map[string]any{
		"format":      "trae2api-web-accounts",
		"version":     1,
		"exported_at": time.Now().Format(time.RFC3339),
		"accounts":    accounts,
	})
}

// adminAccountTest POST /admin/api/accounts/{uid}/test
// 用指定账号发一次最小对话，直接回答「这个号现在能不能接单」。
// 不复用 chat 主路径：不经选号、不计冷却，避免测试请求污染调度状态。
func (h *Handler) adminAccountTest(w http.ResponseWriter, r *http.Request) {
	uid := r.PathValue("uid")
	a := h.cfg.Pool.AuthByUID(uid)
	if a == nil {
		writeOpenAIError(w, http.StatusNotFound, "not_found", "no auth for uid")
		return
	}
	model := r.URL.Query().Get("model")
	if model == "" {
		model = h.cfg.DefaultModel
	}
	started := time.Now()
	body, _ := json.Marshal(map[string]any{
		"model": model,
		"messages": []map[string]any{
			{"role": "user", "content": "ping"},
		},
		"stream": false,
	})
	rc, status, respBody, terr := h.cfg.Upstream.ChatStream(a, body)
	elapsed := time.Since(started).Milliseconds()
	out := map[string]any{"uid": uid, "model": model, "latency_ms": elapsed}
	if terr != nil {
		out["ok"] = false
		out["error"] = terr.Error()
		writeJSON(w, http.StatusOK, out)
		return
	}
	if status >= 400 {
		out["ok"] = false
		out["status"] = status
		out["error"] = logfmt.Truncate(string(respBody), 300)
		out["kind"] = upstream.Classify(status, string(respBody)).String()
		writeJSON(w, http.StatusOK, out)
		return
	}
	_, aerr := upstream.Aggregate(rc)
	rc.Close()
	if aerr != nil {
		out["ok"] = false
		out["error"] = aerr.Error()
		writeJSON(w, http.StatusOK, out)
		return
	}
	out["ok"] = true
	out["status"] = status
	writeJSON(w, http.StatusOK, out)
}
