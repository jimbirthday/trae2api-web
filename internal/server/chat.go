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
	reqID := reqlog.NewRequestID()
	rec := &reqlog.Event{RequestID: reqID, Time: started, Path: r.URL.Path}

	body, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes+1))
	if err != nil {
		writeOpenAIError(w, http.StatusBadRequest, "invalid_request", "read body: "+err.Error())
		return
	}
	if len(body) > maxBodyBytes {
		writeOpenAIError(w, http.StatusRequestEntityTooLarge, "request_too_large", "request body exceeds 8MB limit")
		return
	}
	var peek struct {
		Stream bool   `json:"stream"`
		Model  string `json:"model"`
		// StreamOptions 只需读 include_usage：OpenAI 规定该标志为 true 时，
		// 才在 [DONE] 前追加一个只带 usage 的 chunk。
		StreamOptions struct {
			IncludeUsage bool `json:"include_usage"`
		} `json:"stream_options"`
	}
	_ = json.Unmarshal(body, &peek)
	rec.Stream = peek.Stream
	rec.Model = peek.Model
	defer func() {
		rec.LatencyMs = time.Since(started).Milliseconds()
		h.recordRequest(rec)
	}()

	configName, err := h.mapModel(peek.Model)
	if err != nil {
		rec.Outcome = reqlog.OutcomeHTTPError
		rec.Error = err.Error()
		writeOpenAIError(w, http.StatusBadRequest, "invalid_request", err.Error())
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
		if !h.cfg.Pool.Acquire(acct.UID) {
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
			h.cfg.Pool.Release(acct.UID)
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
				h.cfg.Pool.Release(acct.UID)
				// 只计数、不罚号：不喂 NoteError（不计熔断额度）、不喂 NoteFailures（不降权）。
				h.cfg.Pool.NoteTransportError(acct.UID)
				lastErr = terr
				rec.Error = terr.Error()
				log.Printf("WARN: [server] upstream timeout/client-gone acct=%s: %v (account not penalized)",
					logfmt.Label(acct.UID, acct.Nickname), terr)
				break
			}
			h.cfg.Pool.Release(acct.UID)
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
			h.cfg.Pool.Release(acct.UID)
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

		// 成功：先占住租约，直到响应体完全消费/流结束再释放。
		if peek.Stream {
			bindSticky()
			h.cfg.Pool.NoteSuccess(acct.UID)
			rec.Status = http.StatusOK
			openStream() // 理论上已开；防御性兜底
			err := sse.Run(rc, upstream.StreamOptions{
				// 把请求的模型名写进每个 chunk：上游 metadata 的 model 常为空，
				// 留空会让客户端无法按模型统计/显示。
				Model: sseModel,
				// 思考期给客户端看什么（上游思考可达数千字/数十秒）。
				Thinking: h.cfg.StreamThinking,
				// usage chunk 只在客户端显式要求时下发（OpenAI stream_options 语义）。
				IncludeUsage: peek.StreamOptions.IncludeUsage,
				OnError: func(se *upstream.SOLOStreamError) {
					h.handleStreamError(acct.UID, configName, se)
				},
				OnUsage: func(d upstream.UsageDetail) {
					// 流式 usage 记账：与非流式保持对称（含缓存/思考链/延迟）。
					h.recordUsageDetail(acct.UID, configName, d, time.Since(started), rec)
					h.noteCreditCompletion(acct.UID, configName, d.TotalTokens)
				},
			})
			rc.Close()
			h.cfg.Pool.Release(acct.UID)
			switch {
			case err != nil && r.Context().Err() != nil:
				// 客户端主动断连（用户关闭/取消）：不算上游失败，也不罚号。
				rec.Outcome = reqlog.OutcomeInterrupted
			case err != nil:
				rec.Outcome = reqlog.OutcomeStreamError
				rec.Error = err.Error()
			default:
				rec.Outcome = reqlog.OutcomeSuccess
			}
			return
		}

		resp, aerr := upstream.Aggregate(rc)
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
		h.cfg.Pool.Release(acct.UID)
		if aerr != nil {
			var se *upstream.SOLOStreamError
			if errors.As(aerr, &se) {
				lastErr = aerr
				h.handleStreamError(acct.UID, configName, se)
				if se.Kind() == upstream.ErrSoftRate {
					lastRateLimited = true
				}
				rec.Error = aerr.Error()
				if !rotateBackoff(rotates, r.Context()) {
					break
				}
				rotates++
				rec.Rotates = rotates
				continue
			}
			rec.Outcome = reqlog.OutcomeStreamError
			rec.Error = aerr.Error()
			writeOpenAIError(w, http.StatusBadGateway, "upstream_parse", aerr.Error())
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
		writeOpenAIError(w, http.StatusServiceUnavailable, "model_blocked", msg)
		rec.Outcome = reqlog.OutcomeHTTPError
		rec.Status = http.StatusServiceUnavailable
		rec.Error = msg
		return
	}
	if lastErr != nil {
		msg += ": " + lastErr.Error()
	}
	rec.Outcome = reqlog.OutcomeHTTPError
	rec.Status = http.StatusServiceUnavailable
	rec.Error = msg
	// 流已经开出去（响应头 200 已发），状态码改不动了：用流内 error 事件
	// 收尾，客户端才会立刻报错而不是一直挂着等 token。
	if sse != nil {
		rec.Outcome = reqlog.OutcomeStreamError
		sse.WriteError(msg)
		return
	}
	writeOpenAIError(w, http.StatusServiceUnavailable, "no_healthy_account", msg)
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
