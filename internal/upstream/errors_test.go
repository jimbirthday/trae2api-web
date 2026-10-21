package upstream

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"trae2api-web/internal/auth"
)

func TestMatchModelErrorEmptyRulesNeverFires(t *testing.T) {
	c := &Client{}
	if kind, ok := c.MatchModelError(403, `{"code":6004,"msg":"no such model"}`); ok || kind != "" {
		t.Fatalf("empty rules must not match, got %q %v", kind, ok)
	}
	// 显式注入空表同理
	c.SetModelErrorRules(nil)
	if _, ok := c.MatchModelError(403, `{"code":6004}`); ok {
		t.Fatal("nil rules must not match")
	}
	// 无任何条件的规则被丢弃
	c.SetModelErrorRules([]ModelErrorRule{{Kind: "model_unavailable"}})
	if _, ok := c.MatchModelError(403, `{"code":6004}`); ok {
		t.Fatal("condition-less rule must be dropped")
	}
}

func TestMatchModelErrorCombinations(t *testing.T) {
	cases := []struct {
		name  string
		rules []ModelErrorRule
		stat  int
		body  string
		want  string
		ok    bool
	}{
		{"code only hit", []ModelErrorRule{{Code: 6004, Kind: ModelKindUnknownModel}}, 403, `{"code":6004,"msg":"x"}`, ModelKindUnknownModel, true},
		{"code only with space", []ModelErrorRule{{Code: 6004, Kind: ModelKindUnknownModel}}, 403, `{"code": 6004}`, ModelKindUnknownModel, true},
		{"code only bare number", []ModelErrorRule{{Code: 6004}}, 500, `err 6004 happened`, ModelKindModelUnavailable, true},
		{"code overflow token mismatch", []ModelErrorRule{{Code: 6004}}, 500, `err 16004 happened`, "", false},
		{"code miss", []ModelErrorRule{{Code: 6004}}, 403, `{"code":7000}`, "", false},
		{"status only", []ModelErrorRule{{Status: 429, Kind: ModelKindRateLimit}}, 429, ``, ModelKindRateLimit, true},
		{"status mismatch", []ModelErrorRule{{Status: 429}}, 403, ``, "", false},
		{"contains all must hit", []ModelErrorRule{{Contains: []string{"model", "not exist"}, Kind: ModelKindUnknownModel}}, 400, `Model Not Exist`, ModelKindUnknownModel, true},
		{"contains partial miss", []ModelErrorRule{{Contains: []string{"model", "not exist"}}}, 400, `model is busy`, "", false},
		{"code+status+contains", []ModelErrorRule{{Code: 6004, Status: 403, Contains: []string{"model"}}}, 403, `{"code":6004,"msg":"model unavailable"}`, ModelKindModelUnavailable, true},
		{"code+status mismatch", []ModelErrorRule{{Code: 6004, Status: 429}}, 403, `{"code":6004}`, "", false},
		{"first rule wins", []ModelErrorRule{{Status: 429, Kind: ModelKindRateLimit}, {Status: 429, Kind: ModelKindUnknownModel}}, 429, ``, ModelKindRateLimit, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := &Client{}
			c.SetModelErrorRules(tc.rules)
			kind, ok := c.MatchModelError(tc.stat, tc.body)
			if ok != tc.ok || kind != tc.want {
				t.Fatalf("got (%q,%v) want (%q,%v)", kind, ok, tc.want, tc.ok)
			}
		})
	}
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name string
		hdr  http.Header
		want time.Duration
	}{
		{"seconds", http.Header{"Retry-After": {"7"}}, 7 * time.Second},
		{"http-date", http.Header{"Retry-After": {now.Add(30 * time.Second).Format(http.TimeFormat)}}, 30 * time.Second},
		{"past date", http.Header{"Retry-After": {now.Add(-time.Minute).Format(http.TimeFormat)}}, 0},
		{"garbage", http.Header{"Retry-After": {"soon"}}, 0},
		{"retry-after-ms", http.Header{"Retry-After-Ms": {"1500"}}, 1500 * time.Millisecond},
		{"x-ratelimit-reset unix sec", http.Header{"X-Ratelimit-Reset": {fmt.Sprint(now.Unix() + 12)}}, 12 * time.Second},
		{"x-ratelimit-reset unix ms", http.Header{"X-Ratelimit-Reset": {fmt.Sprint(now.UnixMilli() + 2500)}}, 2500 * time.Millisecond},
		{"x-ratelimit-reset past", http.Header{"X-Ratelimit-Reset": {fmt.Sprint(now.Unix() - 5)}}, 0},
		{"empty", http.Header{}, 0},
		{"nil header", nil, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ParseRetryAfter(tc.hdr, now); got != tc.want {
				t.Fatalf("got %v want %v", got, tc.want)
			}
		})
	}
}

type timeoutErr struct{}

func (timeoutErr) Error() string   { return "timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

func TestIsUpstreamTimeoutAndClientGone(t *testing.T) {
	if !IsUpstreamTimeout(fmt.Errorf("dial: %w", timeoutErr{})) {
		t.Error("wrapped net.Timeout not detected")
	}
	if !IsUpstreamTimeout(&net.OpError{Op: "dial", Err: timeoutErr{}}) {
		t.Error("net.OpError timeout not detected")
	}
	if !IsUpstreamTimeout(fmt.Errorf("a: %w", fmt.Errorf("b: %w", context.DeadlineExceeded))) {
		t.Error("wrapped DeadlineExceeded not detected")
	}
	if !IsUpstreamTimeout(os.ErrDeadlineExceeded) {
		t.Error("os.ErrDeadlineExceeded not detected")
	}
	if IsUpstreamTimeout(errors.New("broken pipe")) {
		t.Error("plain error must not be timeout")
	}
	if IsUpstreamTimeout(nil) || IsClientGone(nil) {
		t.Error("nil must be false")
	}
	if !IsClientGone(fmt.Errorf("stream: %w", context.Canceled)) {
		t.Error("wrapped context.Canceled not detected")
	}
	if IsClientGone(context.DeadlineExceeded) {
		t.Error("deadline is not client gone")
	}
}

func TestUsageFromEvent(t *testing.T) {
	p, c, tot, ok := UsageFromEvent(&SOLOEvent{Usage: map[string]any{
		"prompt_tokens": float64(21), "completion_tokens": float64(142),
		"total_tokens": float64(163), "reasoning_tokens": float64(135),
	}})
	if !ok || p != 21 || c != 142 || tot != 163 {
		t.Fatalf("got %d/%d/%d ok=%v", p, c, tot, ok)
	}
	// total 缺失 → prompt+completion
	p, c, tot, ok = UsageFromEvent(&SOLOEvent{Usage: map[string]any{
		"prompt_tokens": float64(5), "completion_tokens": float64(6),
	}})
	if !ok || p != 5 || c != 6 || tot != 11 {
		t.Fatalf("fallback got %d/%d/%d", p, c, tot)
	}
	if _, _, _, ok := UsageFromEvent(nil); ok {
		t.Error("nil event must be !ok")
	}
	if _, _, _, ok := UsageFromEvent(&SOLOEvent{}); ok {
		t.Error("empty usage must be !ok")
	}
}

func TestSOLOStreamErrorKind(t *testing.T) {
	SetStreamErrorClassifier(nil)
	if got := (&SOLOStreamError{Code: 1005}).Kind(); got != ErrPlanLimit {
		t.Errorf("1005 = %v want ErrPlanLimit", got)
	}
	if got := (&SOLOStreamError{Code: 4003}).Kind(); got != ErrClient {
		t.Errorf("other = %v want ErrClient", got)
	}
	SetStreamErrorClassifier(func(code int64, msg string) ErrKind {
		if strings.Contains(msg, "model") {
			return ErrModelBlocked
		}
		return ErrClient
	})
	defer SetStreamErrorClassifier(nil)
	if got := (&SOLOStreamError{Code: 9, Msg: "model gone"}).Kind(); got != ErrModelBlocked {
		t.Errorf("hook = %v want ErrModelBlocked", got)
	}
}

func TestChatStreamDetailedModelRule(t *testing.T) {
	c := testClient(func(r *http.Request) (*http.Response, error) {
		resp := jsonResp(403, `{"code":6004,"msg":"model not found"}`)
		resp.Header.Set("Retry-After", "3")
		return resp, nil
	})
	c.SetModelErrorRules([]ModelErrorRule{{Code: 6004, Status: 403, Contains: []string{"model"}, Kind: ModelKindUnknownModel}})
	_, status, body, uerr, err := c.ChatStreamDetailed(context.Background(), &auth.Auth{AccessToken: "at", UID: "u"}, []byte(`{"model":"glm-5.2"}`))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if status != 403 || len(body) == 0 || uerr == nil {
		t.Fatalf("status=%d body=%q uerr=%v", status, body, uerr)
	}
	if uerr.Kind != ErrModelBlocked || uerr.Model != "glm-5.2" || uerr.RetryAfter != 3*time.Second {
		t.Fatalf("uerr=%+v", uerr)
	}
	// 旧 API 行为不变：status/body 返回，err=nil
	rc, st2, b2, err2 := c.ChatStream(&auth.Auth{AccessToken: "at", UID: "u"}, []byte(`{"model":"glm-5.2"}`))
	if rc != nil || st2 != 403 || len(b2) == 0 || err2 != nil {
		t.Fatalf("legacy ChatStream rc=%v st=%d body=%q err=%v", rc, st2, b2, err2)
	}
}

func TestChatStreamDetailedNoRulesClassifiesLegacy(t *testing.T) {
	c := testClient(func(r *http.Request) (*http.Response, error) {
		return jsonResp(429, `{"code":1234,"msg":"slow down"}`), nil
	})
	_, _, _, uerr, err := c.ChatStreamDetailed(context.Background(), &auth.Auth{AccessToken: "at"}, []byte(`{"config_name":"cfg-x"}`))
	if err != nil || uerr == nil {
		t.Fatalf("uerr=%v err=%v", uerr, err)
	}
	if uerr.Kind != ErrSoftRate || uerr.Model != "cfg-x" {
		t.Fatalf("uerr=%+v", uerr)
	}
}

func TestModelFromBody(t *testing.T) {
	if got := ModelFromBody([]byte(`{"config_name":"c1","model":"m1"}`)); got != "c1" {
		t.Errorf("got %q", got)
	}
	if got := ModelFromBody([]byte(`{"model":"m1"}`)); got != "m1" {
		t.Errorf("got %q", got)
	}
	if got := ModelFromBody([]byte(`not json`)); got != "" {
		t.Errorf("got %q", got)
	}
}

// 流内错误（event:error）只有 code+msg，没有原始 JSON body，
// 因此必须能按 code 直接命中规则（3003 = 该账号跑不了这个模型）。
func TestStreamErrKindMatchesRuleByCode(t *testing.T) {
	c := New()
	c.SetModelErrorRules([]ModelErrorRule{{Code: 3003, Kind: ModelKindModelUnavailable}})
	if k := c.streamErrKind(3003, "all models failed"); k != ErrModelBlocked {
		t.Fatalf("code=3003 should be ErrModelBlocked, got %v", k)
	}
	// 未命中规则时保持默认口径（非 1005 → Client）。
	if k := c.streamErrKind(5001, "boom"); k != ErrClient {
		t.Fatalf("code=5001 should stay ErrClient, got %v", k)
	}
	// 1005 仍为 PlanLimit（回归）。
	if k := c.streamErrKind(1005, "plan limit"); k != ErrPlanLimit {
		t.Fatalf("code=1005 should be ErrPlanLimit, got %v", k)
	}
	// 规则为空：3003 不被当模型级错误。
	empty := New()
	if k := empty.streamErrKind(3003, "all models failed"); k == ErrModelBlocked {
		t.Fatal("empty rules must never classify as model blocked")
	}
	// KindFor 与 streamErrKind 结果一致。
	e := &SOLOStreamError{Code: 3003, Msg: "all models failed"}
	if e.KindFor(c) != ErrModelBlocked {
		t.Fatalf("KindFor=%v", e.KindFor(c))
	}
}
