// login.go Web 登录闭环：生成登录 URL → pending 态 → /authorize 回调捕获 →
// ExchangeToken + GetUserInfo + 落盘 → 面板轮询 result。
//
// 回调端口策略（M0 结论：双端口 18080）：
//   - TRAE 登录页回调到 http://127.0.0.1:18080/authorize
//   - main.go 起第二个 http.Server 监听 18080，复用同一 Handler（/authorize 在主 mux 已注册）
//   - 回调捕获后直接在服务端完成导入（用户无需再粘赖）
//
// pending 态只在内存（重启丢失），符合「登录态瞬时」语义。
package server

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"time"

	"trae2api-web/internal/auth"
)

// loginPendingTTL 待登录会话有效期：超过即视为放弃（内存态，重启也清空）。
const loginPendingTTL = 10 * time.Minute

// pendingState pending 登录状态。
type pendingState string

const (
	pendingStateActive   pendingState = "pending"  // 等待用户登录回调
	pendingStateSuccess  pendingState = "success"  // 回调捕获 + 落盘成功
	pendingStateFailed   pendingState = "failed"   // ExchangeToken/落盘失败
	pendingStateCanceled pendingState = "canceled" // 用户取消
)

// pendingLogin 单次登录的临时上下文。
type pendingLogin struct {
	state       pendingState
	machineID   string
	deviceID    string
	callbackURL string // auth_callback_url（含端口）
	createdAt   time.Time
	// nonce 一次性回调凭据（32 hex）：写进 auth_callback_url 的 query，
	// TRAE 回跳时若原样带回就做严格匹配；不回传时退化为「必须存在未过期
	// pending 且未被占用」（见 authorizeCallback 的降级说明）。
	nonce  string
	used   bool // 已被回调消费（一次性，防重放）
	usedAt time.Time

	// success 时填
	uid      string
	nickname string
	// failed 时填
	errMsg string
}

// loginStartResponse POST /admin/api/login 返回。
type loginStartResponse struct {
	LoginURL    string `json:"login_url"`
	PendingID   string `json:"pending_id"`
	CallbackURL string `json:"callback_url"`
	// Warn 非空表示回调链路存在风险（未配置公网地址 / 端口可能不可达）。
	Warn string `json:"warn,omitempty"`
}

// adminLoginStart POST /admin/api/login：生成登录 URL + pending 态。
// body 可选 {callback_port: "18080"}；默认用 127.0.0.1:18080/authorize。
func (h *Handler) adminLoginStart(w http.ResponseWriter, r *http.Request) {
	var req struct {
		CallbackPort string `json:"callback_port,omitempty"`
	}
	_ = decodeBodyOptional(r, &req)
	if req.CallbackPort == "" {
		req.CallbackPort = "18080"
	}
	// 回调地址必须是「浏览器能访问到的地址」。Docker 部署时容器内
	// 127.0.0.1:18080 对宿主机浏览器不可达，必须用 TW2A_PUBLIC_BASE_URL
	// （如 https://trae.example.com）指向映射出来的端口。
	warn := ""
	base := strings.TrimSpace(h.cfg.PublicBaseURL)
	if base == "" {
		base = "http://127.0.0.1:" + req.CallbackPort
		warn = "未配置 TW2A_PUBLIC_BASE_URL，回调地址回落到本机 " + base +
			"/authorize；若服务运行在 Docker/远程主机，浏览器无法访问该地址，登录回调会失败"
	} else {
		base = strings.TrimRight(base, "/")
	}
	// nonce 写进回调 URL 的 query，便于 TRAE 原样回传时做严格校验。
	nonce, err := randomHex(16) // hex32
	if err != nil {
		writeOpenAIError(w, http.StatusInternalServerError, "rand_failed", err.Error())
		return
	}
	callbackURL := base + "/authorize?tw2a_state=" + url.QueryEscape(nonce)

	machineID, err := randomHex(16) // hex32
	if err != nil {
		writeOpenAIError(w, http.StatusInternalServerError, "rand_failed", err.Error())
		return
	}
	deviceID, err := randomHex(16)
	if err != nil {
		writeOpenAIError(w, http.StatusInternalServerError, "rand_failed", err.Error())
		return
	}
	loginURL := BuildLoginURL(machineID, deviceID, callbackURL)

	pendingID, err := randomHex(8) // hex16
	if err != nil {
		writeOpenAIError(w, http.StatusInternalServerError, "rand_failed", err.Error())
		return
	}
	pl := &pendingLogin{
		state:       pendingStateActive,
		machineID:   machineID,
		deviceID:    deviceID,
		callbackURL: callbackURL,
		createdAt:   time.Now(),
		nonce:       nonce,
	}
	h.loginMu.Lock()
	h.logins[pendingID] = pl
	h.loginMu.Unlock()

	writeJSON(w, http.StatusOK, loginStartResponse{
		LoginURL: loginURL, PendingID: pendingID, CallbackURL: callbackURL, Warn: warn,
	})
}

// adminLoginResult GET /admin/api/login/result?pending_id=...
func (h *Handler) adminLoginResult(w http.ResponseWriter, r *http.Request) {
	pendingID := r.URL.Query().Get("pending_id")
	pl, ok := h.getPending(pendingID)
	if !ok {
		writeOpenAIError(w, http.StatusNotFound, "not_found", "pending login not found (expired or invalid)")
		return
	}
	resp := map[string]any{
		"pending_id": pendingID,
		"state":      string(pl.state),
	}
	if pl.state == pendingStateSuccess {
		resp["uid"] = pl.uid
		resp["nickname"] = pl.nickname
	} else if pl.state == pendingStateFailed {
		resp["error"] = pl.errMsg
	}
	writeJSON(w, http.StatusOK, resp)
}

// adminLoginCancel POST /admin/api/login/cancel（body {pending_id} 或 query ?pending_id=）
func (h *Handler) adminLoginCancel(w http.ResponseWriter, r *http.Request) {
	pendingID := r.URL.Query().Get("pending_id")
	if pendingID == "" {
		var req struct {
			PendingID string `json:"pending_id"`
		}
		_ = decodeBodyOptional(r, &req)
		pendingID = req.PendingID
	}
	h.loginMu.Lock()
	pl, ok := h.logins[pendingID]
	if ok && pl.state == pendingStateActive {
		pl.state = pendingStateCanceled
	}
	// 直接删 pending（取消后不再保留）
	delete(h.logins, pendingID)
	h.loginMu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"pending_id": pendingID, "canceled": ok})
}

// authorizeCallback GET /authorize：TRAE 回调落点。
// 捕获 query → 校验登录会话 → ExchangeToken → GetUserInfo → 落盘 → 标记 pending 成功。
//
// 安全门禁（关键）：这个端点是浏览器跳转落点、无法要求 Bearer，因此必须自证
// 「本次回调对应一个由管理员刚发起的登录」：
//   - 首选严格匹配：回调 query 里的 tw2a_state 与 pending.nonce 一致（一次性，用完即废）；
//   - 降级匹配：TRAE 若不回传 tw2a_state，则用 loginTraceID / machine_id 反查 pending，
//     或「当前恰好只有一个未过期的待登录会话」——此时任何外部访问都无法凭空塞号。
//
// 校验不通过一律 400 拒绝，绝不落盘、绝不调用上游。
func (h *Handler) authorizeCallback(w http.ResponseWriter, r *http.Request) {
	pl, perr := h.claimLoginPending(r)
	if perr != nil {
		authorizeRender(w, http.StatusBadRequest, "登录回调被拒绝", perr.Error())
		return
	}
	defer h.finishPending(pl, pendingStateFailed, "", "", "callback aborted")

	rawURL := r.URL.String()
	if !strings.HasPrefix(rawURL, "http") {
		// /authorize?... → 补全成完整 URL 供 ParseCallback
		rawURL = "http://127.0.0.1" + rawURL
	}
	info, err := ParseCallback(rawURL)
	if err != nil {
		// 回调解析失败：展示一个友好错误页（非 JSON，因为是浏览器跳转）
		h.finishPending(pl, pendingStateFailed, "", "", err.Error())
		authorizeRender(w, http.StatusBadRequest, "登录回调解析失败", err.Error())
		return
	}

	machineID := r.URL.Query().Get("machine_id")
	deviceID := r.URL.Query().Get("device_id")
	if machineID == "" || deviceID == "" {
		// 登录时生成的那一对 id 才是凭证的正确身份，优先用 pending 里的。
		if pl.machineID != "" {
			machineID = pl.machineID
		}
		if pl.deviceID != "" {
			deviceID = pl.deviceID
		}
	}
	// 降级路径下回传的 uid 必须与 pending 期望一致（防「拿别人的回调塞进我的会话」）。
	if pl.uid != "" && info.UID != "" && pl.uid != info.UID {
		h.finishPending(pl, pendingStateFailed, info.UID, "", "callback uid mismatch")
		authorizeRender(w, http.StatusBadRequest, "登录失败", "回调账号与本次登录会话不一致")
		return
	}

	a := &auth.Auth{
		AccessToken:  info.AccessToken,
		RefreshToken: info.RefreshToken,
		UID:          info.UID,
		Nickname:     info.Nickname,
		EnterpriseID: info.EnterpriseID,
		Domain:       "trae.cn",
		ApiHost:      "https://api.trae.com.cn",
		MachineID:    machineID,
		DeviceID:     deviceID,
		ExpiresAt:    info.ExpiresAt,
	}
	// 有 refreshToken → ExchangeToken 换新 access + 轮换 refreshToken
	if a.RefreshToken != "" {
		if exErr := h.cfg.Upstream.RefreshToken(a); exErr != nil {
			h.finishPending(pl, pendingStateFailed, "", "", exErr.Error())
			authorizeRender(w, http.StatusBadGateway, "ExchangeToken 失败", exErr.Error())
			return
		}
	}
	// GetUserInfo 补全 uid/nickname
	uid, nick, ent, gErr := h.cfg.Upstream.GetUserInfo(a)
	if gErr == nil && uid != "" {
		a.UID = uid
		if nick != "" {
			a.Nickname = nick
		}
		if ent != "" {
			a.EnterpriseID = ent
		}
	}
	if a.UID == "" {
		err := errors.New("cannot determine uid from callback or GetUserInfo")
		h.finishPending(pl, pendingStateFailed, "", "", err.Error())
		authorizeRender(w, http.StatusBadRequest, "登录失败", err.Error())
		return
	}
	if a.AccessToken == "" {
		err := errors.New("no access token after exchange")
		h.finishPending(pl, pendingStateFailed, a.UID, "", err.Error())
		authorizeRender(w, http.StatusBadRequest, "登录失败", err.Error())
		return
	}
	// UID 会拼进文件名：不合法直接拒绝（防路径穿越写盘）。
	if err := auth.ValidateUID(a.UID); err != nil {
		h.finishPending(pl, pendingStateFailed, "", "", err.Error())
		authorizeRender(w, http.StatusBadRequest, "登录失败", "上游返回的账号 ID 不合法")
		return
	}

	// 落盘
	if a.FilePath == "" {
		fp, fpErr := auth.SafeFilePathFor(h.cfg.AuthDir, a.UID)
		if fpErr != nil {
			h.finishPending(pl, pendingStateFailed, "", "", fpErr.Error())
			authorizeRender(w, http.StatusInternalServerError, "落盘失败", fpErr.Error())
			return
		}
		a.FilePath = fp
	}
	if mkErr := mkdirAll(h.cfg.AuthDir); mkErr != nil {
		h.finishPending(pl, pendingStateFailed, a.UID, a.Nickname, mkErr.Error())
		authorizeRender(w, http.StatusInternalServerError, "落盘失败", mkErr.Error())
		return
	}
	if err := a.SaveAtomic(); err != nil {
		h.finishPending(pl, pendingStateFailed, a.UID, a.Nickname, err.Error())
		authorizeRender(w, http.StatusInternalServerError, "落盘失败", err.Error())
		return
	}
	h.cfg.Pool.Add(a)
	h.finishPending(pl, pendingStateSuccess, a.UID, a.Nickname, "")

	authorizeRender(w, http.StatusOK, "登录成功",
		"账号 "+a.UID+"（"+a.Nickname+"）已添加，可关闭此窗口返回面板。")
}

// claimLoginPending 校验并「占用」一个待完成的登录会话（一次性）。
//
// 严格匹配 tw2a_state；TRAE 不回传时降级为 loginTraceID/machine_id 反查，
// 再降级为「当前只有一个未过期的待登录会话」。
func (h *Handler) claimLoginPending(r *http.Request) (*pendingLogin, error) {
	q := r.URL.Query()
	state := strings.TrimSpace(q.Get("tw2a_state"))
	traceID := strings.TrimSpace(q.Get("loginTraceID"))
	machineID := strings.TrimSpace(q.Get("machine_id"))

	h.loginMu.Lock()
	defer h.loginMu.Unlock()

	// 顺手清掉过期会话（内存里不攒垃圾）。
	for id, pl := range h.logins {
		if time.Since(pl.createdAt) > loginPendingTTL {
			delete(h.logins, id)
		}
	}

	usable := func(pl *pendingLogin) bool {
		return pl != nil && pl.state == pendingStateActive && !pl.used &&
			time.Since(pl.createdAt) <= loginPendingTTL
	}

	if state != "" {
		for _, pl := range h.logins {
			if pl.nonce != "" && subtle.ConstantTimeCompare([]byte(pl.nonce), []byte(state)) == 1 {
				if !usable(pl) {
					return nil, errors.New("登录会话已使用或已过期，请回到面板重新发起登录")
				}
				pl.used, pl.usedAt = true, time.Now()
				return pl, nil
			}
		}
		return nil, errors.New("回调 state 无法匹配任何待登录会话（可能伪造或已过期）")
	}

	// 降级：TRAE 未回传 state。用 loginTraceID / machine_id 反查。
	var matched []*pendingLogin
	for _, pl := range h.logins {
		if !usable(pl) {
			continue
		}
		if traceID != "" && machineTraceID(pl.machineID, pl.deviceID) == traceID {
			matched = append([]*pendingLogin{pl}, matched...)
			continue
		}
		if machineID != "" && pl.machineID == machineID {
			matched = append([]*pendingLogin{pl}, matched...)
		}
	}
	if len(matched) == 0 {
		// 再降级：全局只有一个待登录会话时接受（管理员刚点的登录）。
		for _, pl := range h.logins {
			if usable(pl) {
				matched = append(matched, pl)
			}
		}
	}
	if len(matched) == 0 {
		return nil, errors.New("没有待完成的登录会话，请回到面板点「添加账号」发起登录")
	}
	if len(matched) > 1 {
		return nil, errors.New("存在多个待登录会话且回调未携带 state，无法确认归属（请重新发起登录）")
	}
	pl := matched[0]
	pl.used, pl.usedAt = true, time.Now()
	return pl, nil
}

// finishPending 把 pending 会话置为终态（幂等：只有 active 状态会被改写）。
func (h *Handler) finishPending(pl *pendingLogin, state pendingState, uid, nick, errMsg string) {
	if pl == nil {
		return
	}
	h.loginMu.Lock()
	defer h.loginMu.Unlock()
	if pl.state != pendingStateActive {
		return
	}
	pl.state = state
	if uid != "" {
		pl.uid = uid
	}
	if nick != "" {
		pl.nickname = nick
	}
	if errMsg != "" {
		pl.errMsg = errMsg
	}
}

// getPending 取 pending（不存在或已过期 false）。pending TTL 10 分钟。
func (h *Handler) getPending(id string) (*pendingLogin, bool) {
	if id == "" {
		return nil, false
	}
	h.loginMu.Lock()
	defer h.loginMu.Unlock()
	pl, ok := h.logins[id]
	if !ok {
		return nil, false
	}
	if time.Since(pl.createdAt) > loginPendingTTL {
		delete(h.logins, id)
		return nil, false
	}
	return pl, true
}

// randomHex 生成 n 字节随机 hex（2n hex 字符）。
func randomHex(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
