package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"trae2api-web/internal/auth"
	"trae2api-web/internal/reqlog"
	"trae2api-web/internal/upstream"
)

// messages 缺失/为空必须在本地拦成 400 invalid_request_error，
// 不要发到上游换回一个含义模糊的 4xx（还白耗一次账号调用）。
func TestChatRejectsEmptyMessagesLocally(t *testing.T) {
	called := false
	up := newFakeUpstream(t, func(string) (int, string, bool) {
		called = true
		return 200, soloSSE, true
	})
	h := NewHandler(Config{
		Pool:     testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at", ExpiresAt: 9999999999}),
		Upstream: up,
	})
	for _, body := range []string{`{"model":"glm-5.2"}`, `{"model":"glm-5.2","messages":[]}`} {
		req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(body))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("body=%s code=%d want 400", body, rec.Code)
		}
		var e struct {
			Error struct {
				Type string `json:"type"`
				Code string `json:"code"`
			} `json:"error"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &e); err != nil {
			t.Fatalf("bad envelope: %s", rec.Body)
		}
		if e.Error.Type != "invalid_request_error" {
			t.Errorf("type=%q want invalid_request_error", e.Error.Type)
		}
		if e.Error.Code != "invalid_request" {
			t.Errorf("code=%q", e.Error.Code)
		}
	}
	if called {
		t.Error("upstream must not be called for empty messages")
	}
}

// 请求 ID 要回给客户端，便于按 ID 在面板里定位这一条流水。
func TestChatSetsRequestIDHeader(t *testing.T) {
	up := newFakeUpstream(t, func(string) (int, string, bool) { return 200, soloSSE, true })
	h := NewHandler(Config{
		Pool:     testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at", ExpiresAt: 9999999999}),
		Upstream: up,
	})
	req := httptest.NewRequest("POST", "/v1/chat/completions",
		strings.NewReader(`{"model":"glm-5.2","messages":[{"role":"user","content":"hi"}]}`))
	req.Header.Set("X-Forwarded-For", "203.0.113.9, 10.0.0.1")
	req.Header.Set("User-Agent", "audit-test/1.0")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body)
	}
	if rec.Header().Get("X-Request-ID") == "" {
		t.Error("missing X-Request-ID header")
	}
}

// 流式：上游一个内容帧都没给 → 不能记成功、不能粘死账号，且要换号。
func TestChatStreamEmptyRotatesAndDoesNotStick(t *testing.T) {
	var calls []string
	up := newFakeUpstream(t, func(authz string) (int, string, bool) {
		calls = append(calls, authz)
		if strings.Contains(authz, "at-bad") {
			// 只有 done、没有任何内容帧：典型的「上游静默吞请求」。
			return 200, "event:done\ndata:{\"finish_reason\":\"stop\"}\n\n", true
		}
		return 200, soloSSE, true
	})
	p := testPoolWith(
		&auth.Auth{UID: "bad", AccessToken: "at-bad", ExpiresAt: 9999999999},
		&auth.Auth{UID: "good", AccessToken: "at-good", ExpiresAt: 9999999999},
	)
	h := NewHandler(Config{Pool: p, Upstream: up, MaxRotate: 3})
	req := httptest.NewRequest("POST", "/v1/chat/completions",
		strings.NewReader(`{"model":"glm-5.2","stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	body := rec.Body.String()
	if !strings.Contains(body, "你好") {
		t.Fatalf("expected failover to healthy account, body=%q calls=%v", body, calls)
	}
	st, _ := p.Status("bad")
	if st.SuccessCount != 0 {
		t.Errorf("empty stream must not count as success: %+v", st)
	}
}

// 非流式的空流同样要报错（而不是回一个 content:"" 的 200）。
func TestChatNonStreamEmptyIsError(t *testing.T) {
	up := newFakeUpstream(t, func(string) (int, string, bool) {
		return 200, "event:metadata\ndata:{}\n\nevent:done\ndata:{\"finish_reason\":\"stop\"}\n\n", true
	})
	h := NewHandler(Config{
		Pool:     testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at", ExpiresAt: 9999999999}),
		Upstream: up,
	})
	req := httptest.NewRequest("POST", "/v1/chat/completions",
		strings.NewReader(`{"model":"glm-5.2","messages":[{"role":"user","content":"hi"}]}`))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code == http.StatusOK {
		t.Fatalf("empty upstream stream must not be a 200: %s", rec.Body)
	}
	var e map[string]any
	json.Unmarshal(rec.Body.Bytes(), &e)
	if e["error"] == nil {
		t.Errorf("want OpenAI error envelope, got %s", rec.Body)
	}
}

// 上游 401 但没有会话失效标记、body 有内容 → 可换号（不硬禁用账号）。
func TestChat401WithBodyRotatesWithoutDisabling(t *testing.T) {
	up := newFakeUpstream(t, func(authz string) (int, string, bool) {
		if strings.Contains(authz, "at-bad") {
			return 401, `{"code":4001,"message":"invalid param"}`, false
		}
		return 200, soloSSE, true
	})
	p := testPoolWith(
		&auth.Auth{UID: "bad", AccessToken: "at-bad", ExpiresAt: 9999999999},
		&auth.Auth{UID: "good", AccessToken: "at-good", ExpiresAt: 9999999999},
	)
	h := NewHandler(Config{Pool: p, Upstream: up})
	var ok bool
	for i := 0; i < 20; i++ {
		req := httptest.NewRequest("POST", "/v1/chat/completions",
			strings.NewReader(`{"model":"glm-5.2","messages":[{"role":"user","content":"hi"}]}`))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code == http.StatusOK {
			ok = true
			break
		}
	}
	if !ok {
		t.Fatal("should fail over on retryable 401")
	}
	st, _ := p.Status("bad")
	if st.Disabled {
		t.Errorf("401 with non-session body must not hard-disable the account: %+v", st)
	}
}

var _ = upstream.ErrEmptyCompletion

// 请求流水必须带来源信息：否则线上无法判断「谁在打」「哪个 SDK 在重试」。
func TestChatRecordsClientIPAndUserAgent(t *testing.T) {
	up := newFakeUpstream(t, func(string) (int, string, bool) { return 200, soloSSE, true })
	rl := reqlog.New(reqlog.Config{})
	defer rl.Close()
	h := NewHandler(Config{
		Pool:       testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at", ExpiresAt: 9999999999}),
		Upstream:   up,
		RequestLog: rl,
	})
	req := httptest.NewRequest("POST", "/v1/chat/completions",
		strings.NewReader(`{"model":"glm-5.2","messages":[{"role":"user","content":"hi"}]}`))
	req.Header.Set("X-Forwarded-For", "203.0.113.9, 10.0.0.1")
	req.Header.Set("User-Agent", "curl/8.7.1")
	h.ServeHTTP(httptest.NewRecorder(), req)

	recs := rl.Recent(1)
	if len(recs) != 1 {
		t.Fatalf("want 1 record, got %d", len(recs))
	}
	got := recs[0]
	if got.ClientIP != "203.0.113.9" {
		t.Errorf("client_ip=%q want 203.0.113.9 (first XFF hop)", got.ClientIP)
	}
	if got.UserAgent != "curl/8.7.1" {
		t.Errorf("user_agent=%q", got.UserAgent)
	}
	if got.Outcome != reqlog.OutcomeSuccess {
		t.Errorf("outcome=%q", got.Outcome)
	}
}

// error.type 必须按状态码映射，客户端据此决定是否重试。
func TestOpenAIErrTypeMapping(t *testing.T) {
	cases := map[int]string{
		400: "invalid_request_error",
		401: "authentication_error",
		403: "permission_error",
		404: "not_found_error",
		413: "invalid_request_error",
		429: "rate_limit_error",
		500: "api_error",
		502: "api_error",
		503: "api_error",
	}
	for status, want := range cases {
		if got := openAIErrType(status); got != want {
			t.Errorf("openAIErrType(%d)=%q want %q", status, got, want)
		}
	}
}

// 上游 429 在 /v1/chat/completions 上必须回 rate_limit_error + 429/503 语义，
// 且错误体里不能出现上游原始 body。
func TestChatErrorBodyIsSanitized(t *testing.T) {
	up := newFakeUpstream(t, func(string) (int, string, bool) {
		return 429, `{"code":429,"token":"eyJhbGciOiJIUzI1NiJ9.LEAKED_SECRET","message":"slow down"}`, false
	})
	h := NewHandler(Config{
		Pool:     testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at", ExpiresAt: 9999999999}),
		Upstream: up,
	})
	req := httptest.NewRequest("POST", "/v1/chat/completions",
		strings.NewReader(`{"model":"glm-5.2","messages":[{"role":"user","content":"hi"}]}`))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if strings.Contains(rec.Body.String(), "LEAKED_SECRET") {
		t.Errorf("upstream credential leaked into client response: %s", rec.Body)
	}
}

// 客户端自带 X-Request-ID 时必须原样沿用（端到端串联的前提）。
// 回归点：chatCompletions 曾自己 NewRequestID 并覆盖响应头，导致客户端拿到的
// ID 与归档里记的 ID 不是同一个，按 ID 查流水查不到。
func TestChatReusesClientRequestID(t *testing.T) {
	up := newFakeUpstream(t, func(string) (int, string, bool) { return 200, soloSSE, true })
	h := NewHandler(Config{
		Pool:     testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at", ExpiresAt: 9999999999}),
		Upstream: up,
		APIKey:   "k-test",
	})
	body := `{"model":"glm-5.2","messages":[{"role":"user","content":"hi"}]}`
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+h.cfg.APIKey)
	req.Header.Set("X-Request-ID", "client-trace-42")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if got := w.Header().Get("X-Request-ID"); got != "client-trace-42" {
		t.Fatalf("X-Request-ID=%q want client-trace-42", got)
	}
}
