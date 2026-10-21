package server

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"trae2api-web/internal/auth"
)

// 真实的超时错误长这样：*url.Error 的 Error() 里带完整 URL。
func fakeTimeoutErr() error {
	return &url.Error{
		Op:  "Post",
		URL: "https://trae-api-cn.mchost.guru/api/agent/v3/llm_utils_chat",
		Err: &net.OpError{Op: "read", Net: "tcp", Err: errFakeTimeout{}},
	}
}

type errFakeTimeout struct{}

func (errFakeTimeout) Error() string   { return "timeout awaiting response headers" }
func (errFakeTimeout) Timeout() bool   { return true }
func (errFakeTimeout) Temporary() bool { return true }

func TestRedactUpstream(t *testing.T) {
	cases := []struct{ in, want string }{
		{
			`Post "https://trae-api-cn.mchost.guru/api/agent/v3/llm_utils_chat": net/http: timeout awaiting response headers`,
			"net/http: timeout awaiting response headers",
		},
		// 裸内网 IP（没有 scheme / 域名）：dial 报错的典型形态。
		{
			`Post "https://x.example/y": dial tcp 10.255.255.1:443: connect: connection refused`,
			"dial tcp <upstream>: connect: connection refused",
		},
		{"dial tcp 127.0.0.1:8000 failed", "dial tcp <upstream> failed"},
		{"upstream https://api.trae.cn/foo?token=abc failed", "upstream <upstream> failed"},
		{"dial trae-api-cn.mchost.guru:443: connect: refused", "dial <upstream>: connect: refused"},
		{"plain message without host", "plain message without host"},
		{"", ""},
	}
	for _, c := range cases {
		if got := redactUpstream(c.in); got != c.want {
			t.Errorf("redactUpstream(%q)=%q want %q", c.in, got, c.want)
		}
	}
}

// 超时类错误必须落到固定文案，且不含 URL。
func TestPublicStreamErrMsgTimeoutHasNoURL(t *testing.T) {
	msg := publicStreamErrMsg(fakeTimeoutErr())
	if strings.Contains(msg, "http") || strings.Contains(msg, "mchost") {
		t.Fatalf("timeout message leaks upstream: %q", msg)
	}
	if !strings.Contains(msg, "timed out") {
		t.Errorf("want timeout wording, got %q", msg)
	}
}

// 端到端：所有账号都超时 → 503/SSE error 里都不能出现上游地址。
func TestNoAccountAvailableErrorHidesUpstream(t *testing.T) {
	up := newFakeUpstream(t, func(string) (int, string, bool) { return 200, soloSSE, true })
	// 用「连不上」的 transport 模拟超时（*url.Error 包装）。
	up.StreamHTTP = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, fakeTimeoutErr()
	})}
	up.StreamIdleTimeout = -1
	h := NewHandler(Config{
		Pool:      testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at", ExpiresAt: 9999999999}),
		Upstream:  up,
		MaxRotate: 1,
	})
	req := httptest.NewRequest("POST", "/v1/chat/completions",
		strings.NewReader(`{"model":"glm-5.2","messages":[{"role":"user","content":"hi"}]}`))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	body := w.Body.String()
	for _, bad := range []string{"mchost", "https://", "trae-api", "api.trae"} {
		if strings.Contains(body, bad) {
			t.Fatalf("response leaks %q: %s", bad, body)
		}
	}
	if w.Code == 0 {
		t.Fatalf("no response")
	}
}

// 流式场景（响应头已 200）：错误帧同样不能带上游地址。
func TestStreamErrorFrameHidesUpstream(t *testing.T) {
	up := newFakeUpstream(t, func(string) (int, string, bool) { return 200, soloSSE, true })
	up.StreamHTTP = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, fakeTimeoutErr()
	})}
	h := NewHandler(Config{
		Pool:      testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at", ExpiresAt: 9999999999}),
		Upstream:  up,
		MaxRotate: 1,
	})
	req := httptest.NewRequest("POST", "/v1/chat/completions",
		strings.NewReader(`{"model":"glm-5.2","stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	body := w.Body.String()
	for _, bad := range []string{"mchost", "https://", "trae-api"} {
		if strings.Contains(body, bad) {
			t.Fatalf("stream leaks %q: %s", bad, body)
		}
	}
}

// 管理端「测活」接口同样不能回显上游地址。
func TestAccountTestErrorHidesUpstream(t *testing.T) {
	up := newFakeUpstream(t, func(string) (int, string, bool) { return 200, soloSSE, true })
	up.StreamHTTP = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, fakeTimeoutErr()
	})}
	h := NewHandler(Config{
		Pool:     testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at", ExpiresAt: 9999999999}),
		Upstream: up,
	})
	req := httptest.NewRequest("POST", "/admin/api/accounts/u1/test", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if strings.Contains(w.Body.String(), "mchost") || strings.Contains(w.Body.String(), "https://") {
		t.Fatalf("admin test leaks upstream: %s", w.Body.String())
	}
}

// 写错误体的公共函数必须兜住：即使调用点直接透传 err.Error()。
func TestWriteOpenAIErrorRedacts(t *testing.T) {
	w := httptest.NewRecorder()
	writeOpenAIError(w, 502, "upstream_parse", fmt.Errorf("wrap: %w", fakeTimeoutErr()).Error())
	if strings.Contains(w.Body.String(), "mchost") {
		t.Fatalf("leak: %s", w.Body.String())
	}
}
