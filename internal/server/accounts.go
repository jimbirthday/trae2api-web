// accounts.go /admin/api/accounts 全套：列表 / 导入 / 删除 / PATCH 开关 / 刷新 / JSON 脱敏预览。
//
// 安全纪律（PLAN §4）：
//   - 列表与 JSON 预览绝不返回完整 token，只给前缀 + 长度
//   - 写操作经 withAdminAuth（Bearer 校验）
//   - 回调链接只在服务端解析，前端不经手敏感数据
package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"trae2api-web/internal/auth"
	"trae2api-web/internal/pool"
)

// accountSummary 列表/预览对外结构（脱敏）。
type accountSummary struct {
	UID          string `json:"uid"`
	Nickname     string `json:"nickname,omitempty"`
	EnterpriseID string `json:"enterprise_id,omitempty"`
	Weight       int    `json:"weight,omitempty"`
	Note         string `json:"note,omitempty"`
	HasTokens    bool   `json:"has_tokens"`
	Enabled      bool   `json:"enabled"`
	Disabled     bool   `json:"disabled"` // session dead 硬禁用
	Paused       bool   `json:"paused,omitempty"`
	Cooling      bool   `json:"cooling"`
	CoolKind     string `json:"cool_kind,omitempty"`
	CoolRemain   int64  `json:"cool_remaining_sec,omitempty"`
	Reason       string `json:"reason,omitempty"`
	Credits      int64  `json:"credits"`
	CreditsTotal int64  `json:"credits_total,omitempty"`
	ErrCount     int    `json:"err_count,omitempty"`
	ErrTotal     int64  `json:"err_total,omitempty"`
	SuccessCount int64  `json:"success_count,omitempty"`
	InFlight     int    `json:"in_flight"`
	MaxInFlight  int    `json:"max_in_flight,omitempty"`
	// RateLimited 模型级限额行（模型、类型、恢复时刻）。
	ModelBlocked []pool.RateLimitedModel `json:"model_blocked,omitempty"`
	// Token 有效期（Unix 秒）与是否临近过期
	ExpiresAt   int64  `json:"expires_at,omitempty"`
	ExpiredSoon bool   `json:"expired_soon,omitempty"`
	MachineID   string `json:"machine_id,omitempty"` // 前 8 位（脱敏）
	DeviceID    string `json:"device_id,omitempty"`  // 前 8 位（脱敏）
	Cookies     int    `json:"cookies,omitempty"`    // 存档的浏览器 cookie 数
	HasAuth     bool   `json:"has_auth"`
}

// adminAccounts GET /admin/api/accounts?page=&page_size=&q=&status=
//
// 分页在服务端做：账号多时前端一次性渲染上千行、每行 8 个按钮，DOM 与
// 事件字符串拼接会明显卡顿；这里只返回一页，并给出 total 供翻页。
// page 从 1 开始；page_size 默认 20，上限 200。
func (h *Handler) adminAccounts(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	pageSize := clampInt(q.Get("page_size"), 20, 1, 200)
	page := clampInt(q.Get("page"), 1, 1, 1<<20)
	keyword := strings.ToLower(strings.TrimSpace(q.Get("q")))
	wantStatus := strings.ToLower(strings.TrimSpace(q.Get("status")))

	statuses := h.cfg.Pool.List()
	out := make([]accountSummary, 0, len(statuses))
	for _, s := range statuses {
		sum := accountSummary{
			UID:          s.UID,
			Nickname:     s.Nickname,
			EnterpriseID: s.EnterpriseID,
			Weight:       s.Weight,
			Note:         s.Note,
			HasTokens:    s.HasTokens,
			Enabled:      s.Enabled,
			Disabled:     s.Disabled,
			Paused:       s.Paused,
			Cooling:      s.Cooling,
			CoolKind:     s.CoolKind,
			CoolRemain:   s.CoolRemaining,
			Reason:       s.Reason,
			Credits:      s.Credits,
			CreditsTotal: s.CreditsTotal,
			ErrCount:     s.ConsecutiveFails,
			ErrTotal:     s.ErrTotal,
			SuccessCount: s.SuccessCount,
			InFlight:     s.InFlight,
			MaxInFlight:  h.cfg.Pool.MaxInFlight(),
			ModelBlocked: s.RateLimitedModels,
		}
		if a := h.cfg.Pool.AuthByUID(s.UID); a != nil {
			sum.HasAuth = true
			sum.ExpiresAt = a.ExpiresAt
			if a.ExpiresAt > 0 && a.NeedsRefresh(24*time.Hour) { // 24h 内过期
				sum.ExpiredSoon = true
			}
			sum.MachineID = prefix(a.MachineID, 8)
			sum.DeviceID = prefix(a.DeviceID, 8)
			sum.Cookies = a.CookieCount()
		}
		out = append(out, sum)
	}

	filtered := out[:0]
	for _, sum := range out {
		if keyword != "" && !strings.Contains(strings.ToLower(sum.UID), keyword) &&
			!strings.Contains(strings.ToLower(sum.Nickname), keyword) &&
			!strings.Contains(strings.ToLower(sum.Note), keyword) {
			continue
		}
		if wantStatus != "" && accountStatusKey(sum) != wantStatus {
			continue
		}
		filtered = append(filtered, sum)
	}

	total := len(filtered)
	start := (page - 1) * pageSize
	if start > total {
		start = total
	}
	end := start + pageSize
	if end > total {
		end = total
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"accounts":  filtered[start:end],
		"total":     total,
		"page":      page,
		"page_size": pageSize,
		"pages":     (total + pageSize - 1) / pageSize,
	})
}

// accountStatusKey 归一化账号状态标签（与前端徽章口径一致），供 ?status= 过滤。
func accountStatusKey(s accountSummary) string {
	switch {
	case s.Disabled:
		return "disabled"
	case !s.Enabled:
		return "off"
	case s.Paused:
		return "paused"
	case s.Cooling:
		return "cooling"
	default:
		return "ok"
	}
}

// expectedImportFormats 导入失败时的提示文案：把支持的四种形态一次说清，
// 用户不用猜「为什么我贴的文件不被识别」。
const expectedImportFormats = "；支持：1) TRAE 回调链接字符串；" +
	"2) {\"json\":\"<账号JSON>\"}；3) {\"accounts\":[...]} 或顶层 [ ... ] 批量导出" +
	"（trae-cn-relay-accounts / 浏览器导出格式）；4) 单个账号对象"

// importRequest 导入请求体。
type importRequest struct {
	// 三选一：
	//   CallbackURL：TRAE 登录回调链接（http://127.0.0.1:.../authorize?refreshToken=...）
	//   JSON：嵌套形（{"account":..,"auth":..}）或扁平形（{"accessToken":..,"uid":..}）
	//   CallbackURL 与 JSON 都给时优先 CallbackURL。
	CallbackURL string `json:"callback_url,omitempty"`
	JSON        string `json:"json,omitempty"`
	// 可选覆盖字段（导入扁平 JSON 手建场景补 machine/device）
	MachineID string `json:"machine_id,omitempty"`
	DeviceID  string `json:"device_id,omitempty"`
}

// importResult 导入结果。
type importResult struct {
	UID        string `json:"uid"`
	Nickname   string `json:"nickname,omitempty"`
	Action     string `json:"action"`                // "created" | "updated"
	NeedsCheck bool   `json:"needs_check,omitempty"` // 建议用户确认额度
}

// adminImportAccount POST /admin/api/accounts/import：导入凭证。
// body 三选一：回调链接 / 嵌套 JSON / 扁平 JSON。
func (h *Handler) adminImportAccount(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeOpenAIError(w, http.StatusBadRequest, "invalid_request", "read body: "+err.Error())
		return
	}
	var req importRequest
	// 按首字符分发：`[...]` 是顶层数组导出（批量），`{...}` 是 JSON 对象，
	// 其余当 TRAE 回调链接字符串。
	// 历史实现只认 "{"，导致顶层数组导出会被误当成回调链接拼到上游。
	trim := strings.TrimSpace(string(body))
	switch {
	case strings.HasPrefix(trim, "[") || strings.HasPrefix(trim, "{"):
		if err := json.Unmarshal(body, &req); err != nil {
			// 顶层数组无法解进 importRequest（结构不匹配），走批量通道。
			if strings.HasPrefix(trim, "[") {
				h.importRelayBatch(w, body)
				return
			}
			writeOpenAIError(w, http.StatusBadRequest, "invalid_request",
				"parse json: "+err.Error()+expectedImportFormats)
			return
		}
	default:
		// 纯字符串 body → 当回调链接
		req.CallbackURL = trim
	}

	if h.cfg.AuthDir == "" {
		writeOpenAIError(w, http.StatusInternalServerError, "no_auth_dir", "server AuthDir not configured")
		return
	}

	// 批量通道：中转面板导出格式（trae-cn-relay-accounts）。
	// 两种入口都收：包在 {"json": "..."} 里，或整个 body 就是该 JSON。
	if raw := relayPayload(req, body); raw != nil {
		h.importRelayBatch(w, raw)
		return
	}

	var a *auth.Auth
	switch {
	case req.CallbackURL != "":
		a, err = h.importFromCallback(req)
	case req.JSON != "":
		a, err = h.importFromJSON(req)
	default:
		writeOpenAIError(w, http.StatusBadRequest, "invalid_request",
			"need callback_url or json"+expectedImportFormats)
		return
	}
	if err != nil {
		writeOpenAIError(w, http.StatusBadRequest, "import_failed", err.Error())
		return
	}
	if a.FilePath == "" {
		fp, ferr := auth.SafeFilePathFor(h.cfg.AuthDir, a.UID)
		if ferr != nil {
			writeOpenAIError(w, http.StatusBadRequest, "invalid_uid", ferr.Error())
			return
		}
		a.FilePath = fp
	}

	// 落盘（原子写，复用 auth.SaveAtomic）
	if err := os.MkdirAll(h.cfg.AuthDir, 0o755); err != nil {
		writeOpenAIError(w, http.StatusInternalServerError, "mkdir_failed", err.Error())
		return
	}
	_, statErr := os.Stat(a.FilePath)
	if err := a.SaveAtomic(); err != nil {
		writeOpenAIError(w, http.StatusInternalServerError, "save_failed", err.Error())
		return
	}
	action := "created"
	if statErr == nil {
		action = "updated"
	}
	h.cfg.Pool.Add(a)

	writeJSON(w, http.StatusOK, importResult{
		UID: a.UID, Nickname: a.Nickname, Action: action, NeedsCheck: true,
	})
}

// importFromCallback 解析回调链接 → ExchangeToken → GetUserInfo → 构造 Auth。
func (h *Handler) importFromCallback(req importRequest) (*auth.Auth, error) {
	info, err := ParseCallback(req.CallbackURL)
	if err != nil {
		return nil, err
	}
	// 无 refreshToken 但有 accessToken（userJwt 兜底）→ 直接用，不 ExchangeToken
	a := &auth.Auth{
		AccessToken:  info.AccessToken,
		RefreshToken: info.RefreshToken,
		UID:          info.UID,
		Nickname:     info.Nickname,
		EnterpriseID: info.EnterpriseID,
		Domain:       "trae.cn",
		ApiHost:      "https://api.trae.com.cn",
		MachineID:    req.MachineID,
		DeviceID:     req.DeviceID,
		ExpiresAt:    info.ExpiresAt,
	}
	// 有 refreshToken → ExchangeToken 换新 access token（轮换 refreshToken）
	if a.RefreshToken != "" {
		if err := h.cfg.Upstream.RefreshToken(a); err != nil {
			return nil, errors.New("exchange_token: " + err.Error())
		}
	}
	// GetUserInfo 补全 uid/nickname/enterpriseId（回调 userInfo 可能缺）
	uid, nick, ent, err := h.cfg.Upstream.GetUserInfo(a)
	if err == nil && uid != "" {
		a.UID = uid
		if nick != "" {
			a.Nickname = nick
		}
		if ent != "" {
			a.EnterpriseID = ent
		}
	}
	if a.UID == "" {
		return nil, errors.New("cannot determine uid from callback or GetUserInfo")
	}
	if a.AccessToken == "" {
		return nil, errors.New("no access token after exchange")
	}
	return a, nil
}

// importFromJSON 解析嵌套/扁平 JSON → 构造 Auth（凭证已完整，不 ExchangeToken）。
func (h *Handler) importFromJSON(req importRequest) (*auth.Auth, error) {
	a, err := auth.Parse([]byte(req.JSON))
	if err != nil {
		return nil, err
	}
	// 可选覆盖 machine/device（扁平 JSON 手建场景）
	if req.MachineID != "" {
		a.MachineID = req.MachineID
	}
	if req.DeviceID != "" {
		a.DeviceID = req.DeviceID
	}
	// 缺省 host/domain 补默认
	if a.Domain == "" {
		a.Domain = "trae.cn"
	}
	if a.ApiHost == "" {
		a.ApiHost = "https://api.trae.com.cn"
	}
	return a, nil
}

// adminDeleteAccount DELETE /admin/api/accounts/{uid}：删池条目 + 删 auths 文件。
func (h *Handler) adminDeleteAccount(w http.ResponseWriter, r *http.Request) {
	uid := r.PathValue("uid")
	if uid == "" {
		writeOpenAIError(w, http.StatusBadRequest, "invalid_request", "missing uid")
		return
	}
	// UID 会拼进文件名，先校验防路径穿越（否则可删 authDir 外的任意 *.json）。
	if err := auth.ValidateUID(uid); err != nil {
		writeOpenAIError(w, http.StatusBadRequest, "invalid_uid", err.Error())
		return
	}
	// 优先删该账号真实的落盘文件（可能来自导入目录而非 AuthDir 下的 trae-{uid}.json）。
	var fp string
	if a := h.cfg.Pool.AuthByUID(uid); a != nil {
		fp = a.FilePath
	}
	if fp == "" {
		if p, err := auth.SafeFilePathFor(h.cfg.AuthDir, uid); err == nil {
			fp = p
		}
	}
	removed := h.cfg.Pool.Remove(uid)
	if fp != "" {
		_ = os.Remove(fp) // 池条目已删，文件尽量删（不存在不报错）
	}
	if !removed {
		writeOpenAIError(w, http.StatusNotFound, "not_found", "account not in pool")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"uid": uid, "deleted": true, "file": fp})
}

// patchRequest PATCH 体：开关 / nickname。
type patchRequest struct {
	Enabled  *bool   `json:"enabled,omitempty"`
	Nickname *string `json:"nickname,omitempty"`
}

// adminPatchAccount PATCH /admin/api/accounts/{uid}：软开关 / nickname。
// token 字段一律拒绝修改（脱敏预览不返回真值，PATCH 不接受 token）。
func (h *Handler) adminPatchAccount(w http.ResponseWriter, r *http.Request) {
	uid := r.PathValue("uid")
	var req patchRequest
	if err := decodeBody(w, r, &req); err != nil {
		return
	}
	st, ok := h.cfg.Pool.Status(uid)
	if !ok {
		writeOpenAIError(w, http.StatusNotFound, "not_found", "account not found")
		return
	}
	if req.Enabled != nil {
		reason := "user enabled"
		if !*req.Enabled {
			reason = "user disabled"
		}
		if !h.cfg.Pool.SetEnabled(uid, *req.Enabled, reason) {
			writeOpenAIError(w, http.StatusNotFound, "not_found", "account vanished")
			return
		}
	}
	_ = st // nickname 更新需要改 auth 文件
	if req.Nickname != nil {
		a := h.cfg.Pool.AuthByUID(uid)
		if a == nil {
			writeOpenAIError(w, http.StatusNotFound, "not_found", "no auth for uid")
			return
		}
		// 允许空串（清空昵称）；落盘走 Auth 的写锁，绝不绕过锁直写字段，
		// 否则与 token 刷新的 SaveAtomic 并发时会互相覆盖。
		if err := a.SetNickname(*req.Nickname); err != nil {
			writeOpenAIError(w, http.StatusInternalServerError, "save_failed", err.Error())
			return
		}
	}
	st2, _ := h.cfg.Pool.Status(uid)
	writeJSON(w, http.StatusOK, st2)
}

// adminRefreshAccount POST /admin/api/accounts/{uid}/refresh：手动 ExchangeToken + 落盘。
func (h *Handler) adminRefreshAccount(w http.ResponseWriter, r *http.Request) {
	uid := r.PathValue("uid")
	a := h.cfg.Pool.AuthByUID(uid)
	if a == nil {
		writeOpenAIError(w, http.StatusNotFound, "not_found", "no auth for uid")
		return
	}
	if err := h.cfg.Upstream.RefreshToken(a); err != nil {
		// session 失效 → 硬禁用（与 chat 路径一致）
		if isSessionDeadErr(err) {
			h.cfg.Pool.Disable(uid, "manual refresh: session dead")
		}
		writeOpenAIError(w, http.StatusBadGateway, "refresh_failed", err.Error())
		return
	}
	if a.FilePath == "" {
		fp, ferr := auth.SafeFilePathFor(h.cfg.AuthDir, uid)
		if ferr != nil {
			writeOpenAIError(w, http.StatusBadRequest, "invalid_uid", ferr.Error())
			return
		}
		a.FilePath = fp
	}
	if err := a.SaveAtomic(); err != nil {
		writeOpenAIError(w, http.StatusInternalServerError, "save_failed", err.Error())
		return
	}
	st, _ := h.cfg.Pool.Status(uid)
	writeJSON(w, http.StatusOK, map[string]any{
		"uid":        uid,
		"expires_at": a.ExpiresAt,
		"status":     st,
	})
}

// adminAccountJSON GET /admin/api/accounts/{uid}/json：脱敏预览（只读，无鉴权但严格脱敏）。
func (h *Handler) adminAccountJSON(w http.ResponseWriter, r *http.Request) {
	uid := r.PathValue("uid")
	a := h.cfg.Pool.AuthByUID(uid)
	if a == nil {
		writeOpenAIError(w, http.StatusNotFound, "not_found", "no auth for uid")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"uid":           a.UID,
		"nickname":      a.Nickname,
		"enterprise_id": a.EnterpriseID,
		"domain":        a.Domain,
		"api_host":      a.ApiHost,
		"machine_id":    prefix(a.MachineID, 8),
		"device_id":     prefix(a.DeviceID, 8),
		"access_token":  auth.MaskToken(a.JWT(), 12),
		"refresh_token": auth.MaskToken(a.RefreshTokenValue(), 12),
		"expires_at":    a.ExpiresAt,
		"cookies":       a.CookieCount(),
		"file_path":     a.FilePath,
	})
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// decodeBody 限长读 + JSON 解码，失败写 400 并返回 err。
func decodeBody(w http.ResponseWriter, r *http.Request, v any) error {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeOpenAIError(w, http.StatusBadRequest, "invalid_request", "read body: "+err.Error())
		return err
	}
	if err := json.Unmarshal(body, v); err != nil {
		writeOpenAIError(w, http.StatusBadRequest, "invalid_request", "parse json: "+err.Error())
		return err
	}
	return nil
}

// prefix 返回 s 前 n 字符（脱敏 machine/device id 展示）。
func prefix(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// isSessionDeadErr 粗判 refresh 失败是否 session 失效（含 401/invalid token 标记）。
// 简化判断：错误信息含 session/unauthorized/401/invalid token。
func isSessionDeadErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	for _, m := range []string{"session", "unauthorized", "401", "invalid token", "token 失效"} {
		if strings.Contains(msg, m) {
			return true
		}
	}
	return false
}

// relayPayload 判断本次导入是否走批量通道，返回待解析的原始 JSON。
//
// 判定顺序：{"json":...} 里的内容 → 整个 body。
// 用结构化解析而非子串匹配：历史上用 bytes.Contains(`"auth"`) 判定，
// 只要某个 token/昵称里出现 "accounts" 字样就会误判整包格式。
func relayPayload(req importRequest, body []byte) []byte {
	if req.CallbackURL != "" {
		return nil // 回调链接优先，绝不按批量处理
	}
	cand := bytes.TrimSpace([]byte(req.JSON))
	if len(cand) == 0 {
		cand = bytes.TrimSpace(body)
	}
	if len(cand) == 0 || (cand[0] != '{' && cand[0] != '[') {
		return nil
	}
	if !auth.HasAccounts(cand) {
		return nil
	}
	// 本仓库嵌套形 {"auth":{...},"account":{...}} 交给单条通道（Parse 认识它）。
	var probe map[string]json.RawMessage
	if err := json.Unmarshal(cand, &probe); err == nil {
		if _, hasAuth := probe["auth"]; hasAuth {
			if _, hasAccounts := probe["accounts"]; !hasAccounts {
				return nil
			}
		}
	}
	return cand
}

// importRelayBatch 批量导入中转格式账号：逐条落盘 + 入池，返回逐条结果。
// 单条失败不影响其余；全部失败时返回 400。
func (h *Handler) importRelayBatch(w http.ResponseWriter, raw []byte) {
	list, err := auth.ParseAnyAccounts(raw)
	if err != nil {
		writeOpenAIError(w, http.StatusBadRequest, "import_failed", err.Error())
		return
	}
	if err := os.MkdirAll(h.cfg.AuthDir, 0o755); err != nil {
		writeOpenAIError(w, http.StatusInternalServerError, "mkdir_failed", err.Error())
		return
	}
	results := make([]map[string]any, 0, len(list))
	created, updated, failed := 0, 0, 0
	seen := make(map[string]bool, len(list))
	// 写前快照：判定 created/updated 用（避免 os.Stat → SaveAtomic 之间的 TOCTOU）。
	existed := make(map[string]bool, len(list))
	for _, st := range h.cfg.Pool.List() {
		existed[st.UID] = true
	}
	for _, a := range list {
		item := map[string]any{"uid": a.UID, "nickname": a.Nickname}
		if a.UID == "" {
			// 中转格式缺 uid 时向上游补一次（不阻塞整体导入）
			if uid, nick, ent, ierr := h.cfg.Upstream.GetUserInfo(a); ierr == nil && uid != "" {
				a.UID = uid
				if a.Nickname == "" {
					a.Nickname = nick
				}
				a.EnterpriseID = ent
				item["uid"] = a.UID
				item["nickname"] = a.Nickname
			}
		}
		if a.UID == "" {
			item["error"] = "cannot determine uid"
			results = append(results, item)
			failed++
			continue
		}
		if seen[a.UID] {
			item["error"] = "duplicate uid in payload"
			results = append(results, item)
			failed++
			continue
		}
		seen[a.UID] = true

		fp, fpErr := auth.SafeFilePathFor(h.cfg.AuthDir, a.UID)
		if fpErr != nil {
			item["error"] = "invalid_uid: " + fpErr.Error()
			results = append(results, item)
			failed++
			continue
		}
		a.FilePath = fp
		if err := a.SaveAtomic(); err != nil {
			item["error"] = "save_failed: " + err.Error()
			results = append(results, item)
			failed++
			continue
		}
		h.cfg.Pool.Add(a)
		if a.ImportDisabled {
			// 导出文件里 enabled=false → 导入后置为停用（仍落盘，便于后续手动启用）。
			h.cfg.Pool.SetEnabled(a.UID, false, "disabled in import file")
			item["enabled"] = false
		}
		if n := a.CookieCount(); n > 0 {
			item["cookies"] = n
		}
		// created/updated 以「写前快照是否已在池中」判定，避免先 Stat 再写的 TOCTOU。
		if existed[a.UID] {
			item["action"] = "updated"
			updated++
		} else {
			item["action"] = "created"
			created++
		}
		results = append(results, item)
	}
	out := map[string]any{
		"total":    len(list),
		"created":  created,
		"updated":  updated,
		"failed":   failed,
		"accounts": results,
	}
	if created+updated == 0 {
		writeJSON(w, http.StatusBadRequest, out)
		return
	}
	writeJSON(w, http.StatusOK, out)
}
