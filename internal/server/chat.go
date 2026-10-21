// chat.go /v1/chat/completions 主路径：选号（粘性优先）→ 在途租约 → 退避换号 →
// 冷却分类 → 流式/非流式输出 → usage 记账 → 请求流水。
package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"trae2api-web/internal/auth"
	"trae2api-web/internal/logfmt"
	"trae2api-web/internal/pool"
	"trae2api-web/internal/reqlog"
	"trae2api-web/internal/session"
	"trae2api-web/internal/upstream"
	"trae2api-web/internal/usage"
)

// rotateBackoff 换号之间的短暂退避（指数，封顶 400ms），并响应 ctx 取消。
// 每次换号立刻重打上游在限流场景下会放大压力，也会让 429 更快扩散。
func rotateBackoff(rotates int, ctx context.Context) bool {
	d := time.Duration(1<<uint(minInt(rotates, 4))) * 50 * time.Millisecond
	if d > 400*time.Millisecond {
		d = 400 * time.Millisecond
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (h *Handler) chatCompletions(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	// 复用 ServeHTTP 中间件已经定好的请求 ID（客户端传了就用客户端的）。
	// 这里再生成一个会覆盖响应头，导致「客户端拿到的 ID」与「归档里记的 ID」
	// 不是同一个——报障时按 ID 查不到任何东西。
	reqID := strings.TrimSpace(w.Header().Get("X-Request-ID"))
	if reqID == "" {
		reqID = reqlog.NewRequestID()
		w.Header().Set("X-Request-ID", reqID)
	}
	rec := &reqlog.Event{
		RequestID: reqID, Time: started, Path: r.URL.Path,
		// 来源信息只用于排障（谁在打、哪个客户端 SDK 的 bug），不参与鉴权。
		ClientIP:  chatClientIP(r),
		UserAgent: logfmt.Truncate(r.UserAgent(), 200),
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes+1))
	if err != nil {
		rec.Outcome = reqlog.OutcomeHTTPError
		rec.Status = http.StatusBadRequest
		rec.Error = sanitizeErrMsg("read body: " + err.Error())
		writeChatError(w, rec, http.StatusBadRequest, "invalid_request", "read body: "+sanitizeErrMsg(err.Error()))
		return
	}
	if len(body) > maxBodyBytes {
		rec.Outcome = reqlog.OutcomeHTTPError
		rec.Status = http.StatusRequestEntityTooLarge
		writeChatError(w, rec, http.StatusRequestEntityTooLarge, "request_too_large", "request body exceeds 8MB limit")
		return
	}
	var peek struct {
		Stream bool   `json:"stream"`
		Model  string `json:"model"`
		// Messages 只做「非空」校验：空 messages 上游会直接 4xx，
		// 我们提前拦下能给出更准确的 OpenAI 语义错误码。
		Messages []json.RawMessage `json:"messages"`
		// StreamOptions 只需读 include_usage：OpenAI 规定该标志为 true 时，
		// 才在 [DONE] 前追加一个只带 usage 的 chunk。
		StreamOptions struct {
			IncludeUsage bool `json:"include_usage"`
		} `json:"stream_options"`
	}
	if err := json.Unmarshal(body, &peek); err != nil {
		rec.Outcome = reqlog.OutcomeHTTPError
		rec.Status = http.StatusBadRequest
		rec.Error = "body is not valid JSON"
		writeChatError(w, rec, http.StatusBadRequest, "invalid_request", "request body is not valid JSON: "+sanitizeErrMsg(err.Error()))
		return
	}
	rec.Stream = peek.Stream
	rec.Model = peek.Model
	defer func() {
		rec.LatencyMs = time.Since(started).Milliseconds()
		h.recordRequest(rec)
	}()

	if len(peek.Messages) == 0 {
		// OpenAI 语义：messages 必填且非空。发到上游只会换回一个含义模糊的 4xx，
		// 还会白白消耗一次账号调用与额度。
		rec.Outcome = reqlog.OutcomeHTTPError
		rec.Status = http.StatusBadRequest
		rec.Error = "messages must be a non-empty array"
		writeChatError(w, rec, http.StatusBadRequest, "invalid_request", "messages must be a non-empty array")
		return
	}

	configName, err := h.mapModel(peek.Model)
	if err != nil {
		rec.Outcome = reqlog.OutcomeHTTPError
		rec.Status = http.StatusBadRequest
		rec.Error = err.Error()
		writeChatError(w, rec, http.StatusBadRequest, "model_not_found", err.Error())
		return
	}
	body = setModelInBody(body, configName)
	rec.Model = configName

	// 会话粘性：提取会话键 → 只查已有绑定（不分配）。
	//
	// 关键：这里刻意不用 Resolve（它会在未命中时按 hash 直接分配账号），否则
	// 新会话的选号会绕过 pool 的加权算法，余额/闲置/手动权重/降权全部失效。
	// 正确分工是 Lookup 只查、选号交给 pool、成功后再 Bind 回写。
	sessKey := ""
	stickyUID := ""
	if h.cfg.Session != nil {
		sessKey = session.ExtractKey(body)
		if sessKey != "" {
			if uid, ok := h.cfg.Session.Lookup(sessKey, configName); ok {
				stickyUID = uid
			}
		}
	}
	unbindSticky := func() {
		if sessKey != "" && stickyUID != "" && h.cfg.Session != nil {
			h.cfg.Session.Unbind(sessKey)
			stickyUID = ""
		}
	}

	tried := map[string]bool{}
	var lastErr error
	lastRateLimited := false
	rotates := 0

	// 流式响应的 SSE 写入器：**惰性创建**，但在连上游之前就创建好。
	//
	// 为什么要提前开流：从拿到账号到上游吐出第一个字节，中间有 token 预刷新 +
	// 建连 + 上游排队 + 首包生成，实测 0.8~10s。若等上游握手完成才写响应头，
	// 客户端这段时间收不到任何字节，看起来就是「点了没反应」。提前开流后
	// 客户端几毫秒内就能拿到 200 + SSE 头 + role 帧，等待期由心跳保活。
	sseModel := providerName(peek.Model, configName)
	var sse *upstream.SSEWriter
	openStream := func() {
		if sse != nil {
			return
		}
		sse = upstream.NewSSEWriter(w, sseModel)
		sse.StartHeartbeat(h.cfg.StreamHeartbeat)
		// 响应头已经 flush 出去了，真实状态码就是 200；否则流水里会留下
		// status=0 的「离线错误」，跟 HTTP 语义对不上，面板也会误判。
		rec.Status = http.StatusOK
	}
	defer func() {
		if sse != nil {
			sse.Close()
		}
	}()

	for rotates < h.cfg.MaxRotate {
		var acct *auth.Auth
		if stickyUID != "" {
			if a := h.cfg.Pool.PickByUIDForModel(stickyUID, configName); a != nil {
				acct = a
			} else {
				unbindSticky()
			}
		}
		if acct == nil {
			// 已经吃过 429：若没有别的未试过且现在就能用的号，直接停，避免必然再 429。
			if lastRateLimited && !h.cfg.Pool.HasRotatable(tried, configName) {
				break
			}
			if a := h.cfg.Pool.PickExcludingForModel(tried, configName); a != nil {
				acct = a
			}
		}
		if acct == nil {
			break
		}
		tried[acct.UID] = true
		rec.UID = acct.UID
		rec.Nickname = acct.Nickname

		// 在途租约：Pick 已过滤满额账号，此处 CAS 兜底并发抢名额的竞态。
		// 用带令牌的版本：同一账号并发多个请求时，A 请求的归还绝不能减掉
		// B 请求占的名额（旧的无令牌 Release 会这么做，实际并发上限会失效）。
		leaseToken, leaseOK := h.cfg.Pool.AcquireLease(acct.UID)
		if !leaseOK {
			if stickyUID == acct.UID {
				unbindSticky()
			}
			if !rotateBackoff(rotates, r.Context()) {
				break
			}
			rotates++
			rec.Rotates = rotates
			continue
		}

		// token 预刷新可能耗时数百毫秒，上游首包更久 —— 先把流开出去。
		if peek.Stream {
			openStream()
		}

		// token 临近过期 → 先刷新（失败则冷却换号）。
		if refreshed, rerr := h.cfg.Upstream.RefreshTokenIfNeeded(acct, h.cfg.RefreshSkew); rerr != nil {
			h.cfg.Pool.ReleaseToken(leaseToken)
			lastErr = rerr
			var ue *upstream.Error
			if errors.As(rerr, &ue) && ue.Kind == upstream.ErrSessionDead {
				h.cfg.Pool.Disable(acct.UID, "refresh session dead")
			} else {
				h.cfg.Pool.Cooldown(acct.UID, pool.CoolErr, h.cfg.ErrCooldown, "refresh: "+rerr.Error())
			}
			if !rotateBackoff(rotates, r.Context()) {
				break
			}
			rotates++
			rec.Rotates = rotates
			continue
		} else if refreshed {
			_ = acct.SaveAtomic()
		}

		// 带 ctx 发请求：客户端断开（用户关闭页面/取消）时立刻中止上游连接，
		// 不再空跑一整个推理；同时拿到上游 Retry-After 建议时长。
		rc, status, respBody, uerr, terr := h.cfg.Upstream.ChatStreamDetailed(r.Context(), acct, body)
		retryAfter := time.Duration(0)
		if uerr != nil {
			retryAfter = uerr.RetryAfter
		}
		if terr != nil {
			// 超时 / 客户端断连：不是账号的问题，换号只会拖长请求并误罚健康号。
			if upstream.IsUpstreamTimeout(terr) || upstream.IsClientGone(terr) || r.Context().Err() != nil {
				h.cfg.Pool.ReleaseToken(leaseToken)
				// 只计数、不罚号：不喂 NoteError（不计熔断额度）、不喂 NoteFailures（不降权）。
				h.cfg.Pool.NoteTransportError(acct.UID)
				lastErr = terr
				rec.Error = sanitizeErrMsg(terr.Error())
				log.Printf("WARN: [server] upstream timeout/client-gone acct=%s: %v (account not penalized)",
					logfmt.Label(acct.UID, acct.Nickname), terr)
				break
			}
			h.cfg.Pool.ReleaseToken(leaseToken)
			h.cfg.Pool.NoteFailures(acct.UID)
			h.cfg.Pool.NoteError(acct.UID)
			lastErr = terr
			if !rotateBackoff(rotates, r.Context()) {
				break
			}
			rotates++
			rec.Rotates = rotates
			continue
		}

		if status >= 400 {
			// 上游已返回错误响应体（ChatStream 已关闭 body），立即归还租约，
			// 否则该号会一直被算作「在途占满」而再也选不中（严重泄漏）。
			h.cfg.Pool.ReleaseToken(leaseToken)
			lastErr = h.penalizeHTTP(acct, status, respBody, configName, retryAfter)
			if lastErr != nil {
				var ue *upstream.Error
				if errors.As(lastErr, &ue) && ue.Kind == upstream.ErrSoftRate {
					lastRateLimited = true
				}
			}
			if !rotateBackoff(rotates, r.Context()) {
				break
			}
			rotates++
			rec.Rotates = rotates
			continue
		}

		// 新会话首次选中 → 回写绑定，后续请求走粘性命中路径。
		// 只绑定「上游确实接受了这次请求」的账号，避免把失败号钉死给该会话。
		bindSticky := func() {
			if h.cfg.Session != nil && sessKey != "" && stickyUID != acct.UID {
				h.cfg.Session.Bind(sessKey, acct.UID)
				stickyUID = acct.UID
			}
		}

		// 以下进入「上游已接受请求」阶段：租约保持占用，直到响应体消费完再释放。
		if peek.Stream {
			openStream() // 理论上已开；防御性兜底
			// usage 先暂存、等 Run 返回后按最终结果记账。
			// 为什么不在 OnUsage 里直接落库：上游可能先发 token_usage 再发
			// event:error，这个失败的尝试会被换号重试；若当场记账，同一次用户
			// 请求就会被计两遍 token（面板/计费都虚高）。
			var pendUsage upstream.UsageDetail
			hasPendUsage := false
			runErr := sse.Run(rc, upstream.StreamOptions{
				// 把请求的模型名写进每个 chunk：上游 metadata 的 model 常为空，
				// 留空会让客户端无法按模型统计/显示。
				Model: sseModel,
				// 思考期给客户端看什么（上游思考可达数千字/数十秒）。
				Thinking: h.cfg.StreamThinking,
				// usage chunk 只在客户端显式要求时下发（OpenAI stream_options 语义）。
				IncludeUsage: peek.StreamOptions.IncludeUsage,
				// 关键：错误帧由本函数在「确认不能换号」之后再写。
				// 上游空流/截断/event:error 时我们还有机会换号重试，
				// 若 Run 抢先写 error 帧+[DONE]，客户端已经结束解析，
				// 重试出来的正文就变成孤儿帧。
				SuppressErrorFrame: true,
				OnError: func(se *upstream.SOLOStreamError) {
					h.handleStreamError(acct.UID, configName, se)
				},
				OnUsage: func(d upstream.UsageDetail) {
					pendUsage, hasPendUsage = d, true
				},
			})
			rc.Close()
			h.cfg.Pool.ReleaseToken(leaseToken)
			// 流式耗时拆解落进请求流水：面板据此区分「上游慢」与「我们慢」。
			// 必须在 Run 返回后取快照（它已停止更新）；此时 rec 的 usage 字段
			// 也已由 OnUsage 回填，可以算 TPS。
			st := sse.Stats()
			// 有内容或整条流正常收尾，这次用量才算数（失败的尝试已经换号重试）。
			if hasPendUsage && (runErr == nil || st.HasOutput()) {
				h.recordUsageDetail(acct.UID, configName, pendUsage, time.Since(started), rec)
				h.noteCreditCompletion(acct.UID, configName, pendUsage.TotalTokens)
			}
			applyStreamStats(rec, st)
			// 按模型聚合吞吐：与请求流水用同一个快照，口径完全一致。
			// 放在 Run 返回后（此时 OnUsage 已回填 completion_tokens），
			// 因此分子分母来自同一个请求。
			// 只统计真正下发过内容的流：空流/截断流的 gen_ms 是「等了两秒然后
			// 断了」，把它当成一次吞吐样本会把按模型聚合的 TPS 拉偏（分母虚高、
			// 分子为 0）。
			if h.cfg.Usage != nil && st.HasOutput() {
				h.cfg.Usage.AddTiming(configName, rec.CompletionTokens, st.GenMs,
					st.FirstTextMs, st.FirstFrameMs, int64(st.ContentChars+st.ThinkChars))
			}

			if runErr == nil {
				// 只有「真的收到了内容且正常收尾」才算成功：
				// 先记成功会让坏号被粘性钉死，后续同一会话的请求全部失败。
				bindSticky()
				h.cfg.Pool.NoteSuccess(acct.UID)
				rec.Status = http.StatusOK
				rec.Outcome = reqlog.OutcomeSuccess
				return
			}

			if r.Context().Err() != nil {
				// 客户端主动断连（用户关闭/取消）：不算上游失败，也不罚号。
				rec.Outcome = reqlog.OutcomeInterrupted
				rec.Error = sanitizeErrMsg(runErr.Error())
				return
			}

			// 上游失败。若客户端还没收到任何正文/思考/工具调用，
			// 说明这次尝试对外是不可见的 —— 可以安全换号重试。
			if !st.HasOutput() {
				if stickyUID == acct.UID {
					unbindSticky()
				}
				lastErr = runErr
				h.penalizeStreamFailure(acct, runErr)
				rec.Error = sanitizeErrMsg(runErr.Error())
				log.Printf("WARN: [server] stream failed before any output acct=%s model=%s: %v",
					logfmt.Label(acct.UID, acct.Nickname), configName, runErr)
				if !rotateBackoff(rotates, r.Context()) {
					break
				}
				rotates++
				rec.Rotates = rotates
				continue
			}

			// 已经下发过内容：换号会让客户端看到「两段拼接」的回复，
			// 只能就地收尾报错（error 帧 + [DONE]）。
			rec.Outcome = reqlog.OutcomeStreamError
			rec.Error = sanitizeErrMsg(runErr.Error())
			sse.WriteError(publicStreamErrMsg(runErr))
			return
		}

		resp, aerr := upstream.AggregateWithModel(rc, sseModel)
		rc.Close()
		// 同流式：usage 记账与积分采样都要在 Release 前完成。
		var usageDetail upstream.UsageDetail
		var hasUsage bool
		if aerr == nil {
			if raw, ok := resp["usage"].(map[string]any); ok {
				usageDetail = upstream.UsageDetailFromMap(raw)
				hasUsage = true
			}
		}
		if hasUsage {
			h.recordUsageDetail(acct.UID, configName, usageDetail, time.Since(started), rec)
			h.noteCreditCompletion(acct.UID, configName, usageDetail.TotalTokens)
		}
		h.cfg.Pool.ReleaseToken(leaseToken)
		if aerr != nil {
			var se *upstream.SOLOStreamError
			if errors.As(aerr, &se) {
				lastErr = aerr
				h.handleStreamError(acct.UID, configName, se)
				// 与非流式的流内错误判定保持一致：一律走 KindFor（模型级规则优先），
				// 两处用不同口径会让同一个错误在流式/非流式下罚到不同的状态机。
				if se.KindFor(h.cfg.Upstream) == upstream.ErrSoftRate {
					lastRateLimited = true
				}
				rec.Error = sanitizeErrMsg(aerr.Error())
				if !rotateBackoff(rotates, r.Context()) {
					break
				}
				rotates++
				rec.Rotates = rotates
				continue
			}
			// 空流 / 截断流 / 上游 5xx 之外的协议异常：都不是「请求本身有问题」，
			// 换个号往往就能成，所以这里继续轮转而不是直接 502。
			if errors.Is(aerr, upstream.ErrEmptyCompletion) || errors.Is(aerr, upstream.ErrTruncatedStream) {
				lastErr = aerr
				h.penalizeStreamFailure(acct, aerr)
				rec.Error = sanitizeErrMsg(aerr.Error())
				if !rotateBackoff(rotates, r.Context()) {
					break
				}
				rotates++
				rec.Rotates = rotates
				continue
			}
			rec.Outcome = reqlog.OutcomeStreamError
			rec.Error = sanitizeErrMsg(aerr.Error())
			writeChatError(w, rec, http.StatusBadGateway, "upstream_parse", aerr.Error())
			return
		}
		bindSticky()
		h.cfg.Pool.NoteSuccess(acct.UID)
		rec.Status = http.StatusOK
		rec.Outcome = reqlog.OutcomeSuccess
		writeJSON(w, http.StatusOK, resp)
		return
	}

	msg := "all accounts unavailable (cooling/disabled)"
	if mb := h.cfg.Pool.ModelBlocked(configName); mb.Blocked {
		// 账号都在，但每个号都对该模型处于模型级冷却：语义不同，需明确区分。
		msg = fmt.Sprintf("model %q is blocked on all accounts until %s",
			configName, mb.Until.Format(time.RFC3339))
		writeChatError(w, rec, http.StatusServiceUnavailable, "model_blocked", msg)
		rec.Outcome = reqlog.OutcomeHTTPError
		rec.Status = http.StatusServiceUnavailable
		rec.Error = sanitizeErrMsg(msg)
		return
	}
	if lastErr != nil {
		// 上游原始错误里可能带内部 URL / biz code / 响应体片段：只进日志与流水，
		// 不直接回给客户端。
		rec.Error = sanitizeErrMsg(msg + ": " + lastErr.Error())
		msg += ": " + publicStreamErrMsg(lastErr)
	}
	rec.Outcome = reqlog.OutcomeHTTPError
	rec.Status = http.StatusServiceUnavailable
	if rec.Error == "" {
		rec.Error = msg
	}
	// 流已经开出去（响应头 200 已发），状态码改不动了：用流内 error 事件
	// 收尾，客户端才会立刻报错而不是一直挂着等 token。
	if sse != nil {
		rec.Outcome = reqlog.OutcomeStreamError
		sse.WriteError(msg)
		return
	}
	writeChatError(w, rec, http.StatusServiceUnavailable, "no_healthy_account", msg)
}

// chatClientIP 取真实客户端 IP：X-Forwarded-For 首段 → X-Real-IP → RemoteAddr。
//
// 自包含实现（不复用 admin 侧的 clientIP）：这条路径是所有外部请求都要走的，
// 不能因为后台模块重构而连带编译/语义风险；nginx 默认只设 X-Real-IP，
// 只认 XFF 会让日志里的来源永远是反代 IP。
func chatClientIP(r *http.Request) string {
	if v := r.Header.Get("X-Forwarded-For"); v != "" {
		if i := strings.IndexByte(v, ','); i >= 0 {
			v = v[:i]
		}
		if v = strings.TrimSpace(v); v != "" {
			return logfmt.Truncate(v, 64)
		}
	}
	if v := strings.TrimSpace(r.Header.Get("X-Real-IP")); v != "" {
		return logfmt.Truncate(v, 64)
	}
	host := r.RemoteAddr
	if i := strings.LastIndexByte(host, ':'); i > 0 {
		host = host[:i]
	}
	return logfmt.Truncate(strings.Trim(host, "[]"), 64)
}

// openAIErrType 把 HTTP 状态码映射成 OpenAI 规范的 error.type。
//
// 为什么不能一律 api_error：客户端（尤其官方 SDK 与带重试逻辑的网关）会按
// type 决定行为 —— rate_limit_error 才退避重试，invalid_request_error 重试
// 无意义，authentication_error 要提示用户换 key。全给 api_error 会让他们
// 对「400 参数错」做无意义的指数退避，对「429」反而不退避。
func openAIErrType(status int) string {
	switch {
	case status == http.StatusUnauthorized:
		return "authentication_error"
	case status == http.StatusForbidden:
		return "permission_error"
	case status == http.StatusNotFound:
		return "not_found_error"
	case status == http.StatusTooManyRequests:
		return "rate_limit_error"
	case status == http.StatusBadRequest || status == http.StatusUnprocessableEntity ||
		status == http.StatusRequestEntityTooLarge:
		return "invalid_request_error"
	case status >= 500:
		return "api_error"
	default:
		return "api_error"
	}
}

// writeChatError 写 OpenAI 风格错误响应，并带上请求 ID 便于与流水对齐。
func writeChatError(w http.ResponseWriter, rec *reqlog.Event, status int, code, msg string) {
	msg = sanitizeErrMsg(msg)
	if rec != nil && rec.RequestID != "" {
		w.Header().Set("X-Request-ID", rec.RequestID)
	}
	// 只用 OpenAI schema 定义的四个键：严格校验的 SDK 会拒绝未知字段。
	// 请求 ID 放在 X-Request-ID 头里（上面已设），不塞进 error 对象。
	writeJSON(w, status, map[string]any{
		"error": map[string]any{
			"message": msg,
			"type":    openAIErrType(status),
			"param":   nil,
			"code":    code,
		},
	})
}

// publicStreamErrMsg 决定「已经下发过内容」之后写给客户端的错误文案。
//
// 上游原始错误可能带内部 URL、biz code、甚至响应体片段，直接透给客户端既
// 泄露实现细节，也无助于用户。这里只按类别给一句话，细节留在日志与流水里。
func publicStreamErrMsg(err error) string {
	switch {
	case errors.Is(err, upstream.ErrStreamIdleTimeout):
		return "upstream stalled (no data within idle timeout); please retry"
	case errors.Is(err, upstream.ErrTruncatedStream):
		return "upstream closed the stream before completion; please retry"
	case errors.Is(err, upstream.ErrEmptyCompletion):
		return "upstream returned an empty completion; please retry"
	}
	var se *upstream.SOLOStreamError
	if errors.As(err, &se) {
		// 只回业务码：上游 message 里可能带内部路由/账号标识，留在日志与流水里。
		return fmt.Sprintf("upstream solo error (code=%d); please retry", se.Code)
	}
	return "upstream stream failed; please retry"
}

// penalizeStreamFailure 处置「还没向客户端下发任何内容」的流内失败。
//
// 与 penalizeHTTP 的分工：这里没有 HTTP 状态码，只有错误对象。口径：
//   - 空闲超时：上游自己挂住了，不罚号（只计数），换号试下一个；
//   - 空流/截断：上游收下请求却没给结果，按可重试错误轻度计数；
//   - event:error：已由 OnError 回调按模型级规则/plan/rate 处置过，不重复罚。
func (h *Handler) penalizeStreamFailure(acct *auth.Auth, err error) {
	uid := acct.UID
	if errors.Is(err, upstream.ErrStreamIdleTimeout) || errors.Is(err, upstream.ErrEmptyCompletion) ||
		errors.Is(err, upstream.ErrTruncatedStream) {
		// 这三类都可能是上游侧抖动：计失败数用于降权，但不进熔断额度，
		// 避免一次上游抽风把整池账号打进熔断。
		h.cfg.Pool.NoteTransportError(uid)
		return
	}
	var se *upstream.SOLOStreamError
	if errors.As(err, &se) {
		return // OnError 已经按分类处置（模型级冷却/plan/rate）
	}
	h.cfg.Pool.NoteError(uid)
}

// penalizeHTTP 按 HTTP 错误分类处置账号，返回带分类的 error（供外层决定是否继续轮转）。
func (h *Handler) penalizeHTTP(acct *auth.Auth, status int, respBody []byte, model string, retryAfter time.Duration) error {
	uid := acct.UID
	bodyStr := string(respBody)
	// 模型级错误规则优先（默认空规则 = 永不命中）。
	if kind, ok := h.cfg.Upstream.MatchModelError(status, bodyStr); ok {
		h.cfg.Pool.CooldownModelKind(uid, model, h.cfg.ErrCooldown, kind, "模型级: "+kind)
		return &upstream.Error{Kind: upstream.ErrModelBlocked, Status: status, Msg: bodyStr, Model: model}
	}
	switch upstream.Classify(status, bodyStr) {
	case upstream.ErrPlanLimit:
		h.cfg.Pool.Cooldown(uid, pool.CoolPlan, h.cfg.PlanCooldown, "plan 权益不足")
	case upstream.ErrSoftRate:
		// 上游给了 Retry-After 就按它算（更贴近真实限流窗口），否则走指数退避。
		resetAt := time.Time{}
		if d := retryAfter; d > 0 {
			resetAt = time.Now().Add(d)
		}
		h.cfg.Pool.CooldownSoftRate(uid, h.cfg.SoftCooldown, resetAt, "429 rate limit")
	case upstream.ErrSessionDead:
		h.cfg.Pool.Disable(uid, "session dead")
	case upstream.ErrNotFound:
		// 404 短冷却但不累计 errCount（防雪崩）
		h.cfg.Pool.Cooldown(uid, pool.CoolSoft, h.cfg.SoftCooldown, "upstream 404")
	default:
		h.cfg.Pool.NoteError(uid)
	}
	return &upstream.Error{Kind: upstream.Classify(status, bodyStr), Status: status, Msg: bodyStr}
}

// handleStreamError 流内业务错误 → pool 状态机。
func (h *Handler) handleStreamError(uid, model string, se *upstream.SOLOStreamError) {
	switch se.KindFor(h.cfg.Upstream) {
	case upstream.ErrModelBlocked:
		h.cfg.Pool.CooldownModelKind(uid, model, h.cfg.ErrCooldown, "model_unavailable", "流内模型级错误")
	case upstream.ErrPlanLimit:
		h.cfg.Pool.Cooldown(uid, pool.CoolPlan, h.cfg.PlanCooldown, "plan 权益不足")
	case upstream.ErrSoftRate:
		h.cfg.Pool.CooldownSoftRate(uid, h.cfg.SoftCooldown, time.Time{}, "stream rate limit")
	default:
		h.cfg.Pool.NoteError(uid)
	}
}

// recordUsage 把本次上游 usage 记到 pool（累计统计 + 请求计数）。
// recordUsageDetail 把一次请求的完整用量落到 pool（单账号维度）与 usage.Tracker
// （按模型维度）。流式/非流式共用，避免两边口径不一致。
func (h *Handler) recordUsageDetail(uid, model string, d upstream.UsageDetail, latency time.Duration, rec *reqlog.Event) {
	h.cfg.Pool.NoteUsage(uid, pool.TokenUsageDelta{
		Model: model, PromptTokens: d.PromptTokens, CompletionTokens: d.CompletionTokens,
		TotalTokens: d.TotalTokens, CacheReadTokens: d.CacheReadTokens,
		ReasoningTokens: d.ReasoningTokens,
		LatencyMs:       latency.Milliseconds(), HasUsage: true,
	})
	if h.cfg.Usage != nil {
		h.cfg.Usage.Add(usage.Delta{
			Model: model, PromptTokens: d.PromptTokens, CompletionTokens: d.CompletionTokens,
			TotalTokens: d.TotalTokens, CacheReadTokens: d.CacheReadTokens,
			ReasoningTokens: d.ReasoningTokens,
		})
	}
	if rec != nil {
		rec.PromptTokens, rec.CompletionTokens, rec.TotalTokens = d.PromptTokens, d.CompletionTokens, d.TotalTokens
		rec.CacheReadTokens = d.CacheReadTokens
	}
}

// noteCreditCompletion 把一个完成的请求交给积分采样器（窗口归集）。
// 采样是尽力而为：任何异常都静默丢弃，绝不影响对话结果。
func (h *Handler) noteCreditCompletion(uid, model string, tokens int64) {
	if h.cfg.CreditSampler == nil || h.cfg.Usage == nil || tokens <= 0 {
		return
	}
	h.cfg.CreditSampler.NoteCompletion(uid, model, tokens, func(m string, credits float64, tk int64) {
		h.cfg.Usage.AddCreditSample(m, credits, tk)
	})
}

// applyStreamStats 把流式计时快照折算成请求流水字段（含 TPS）。
//
// TPS 的分母是 GenMs（首帧→末帧），不是总延迟：
// 总延迟里含 token 预刷新、建连、上游排队、首包生成，拿它除 token
// 会系统性低估吞吐，而且这几段的耗时已经由 upstream_ttf_ms 单独体现。
func applyStreamStats(rec *reqlog.Event, st upstream.StreamStats) {
	if rec == nil || !st.HasTiming {
		return
	}
	rec.TTFTMs = st.FirstTextMs
	rec.UpstreamTTFMs = st.FirstFrameMs
	rec.GenMs = st.GenMs
	rec.MaxGapMs = st.MaxGapMs
	rec.TPS = st.TPS(rec.CompletionTokens)
	// 字符速率用于交叉验证：token 由上游记账，字符是本端实测，
	// 两者比值异常（如 token 正常但字符速率极低）说明瓶颈在下游写入。
	if rec.GenMs > 0 {
		chars := st.ContentChars + st.ThinkChars
		rec.CharPS = float64(chars) / (float64(rec.GenMs) / 1000.0)
	}
}

// recordRequest 落一条请求流水（内存环形 + 可选归档）。
func (h *Handler) recordRequest(e *reqlog.Event) {
	if h.cfg.RequestLog == nil {
		return
	}
	h.cfg.RequestLog.Record(*e)
}

// sanitizeErrMsg 去掉可能带出的凭证/长 body，只留短消息。
func sanitizeErrMsg(s string) string {
	s = strings.TrimSpace(s)
	if len(s) > 300 {
		s = logfmt.Truncate(s, 300)
	}
	return s
}

// providerName 决定写进 SSE chunk 的 model 字段：
// 优先回显客户端请求的模型名（客户端按它做路由/统计），
// 客户端没传（"auto"/空）时回落到实际使用过的 config_name。
func providerName(requested, resolved string) string {
	requested = strings.TrimSpace(requested)
	if requested == "" || strings.EqualFold(requested, "auto") {
		return resolved
	}
	return requested
}
