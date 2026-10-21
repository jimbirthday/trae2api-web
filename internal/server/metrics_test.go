package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"trae2api-web/internal/auth"
	"trae2api-web/internal/pool"
	"trae2api-web/internal/reqlog"
	"trae2api-web/internal/usage"
)

// TestMetricsExposesCoreSeries 校验 /metrics 的基本契约：
// 200 + Prometheus 文本 Content-Type + 关键指标名存在。
// 指标名是告警规则的契约面，改名必须同步改 dashboard，所以要用测试钉住。
func TestMetricsExposesCoreSeries(t *testing.T) {
	rl := reqlog.New(reqlog.Config{})
	rl.Record(reqlog.Event{Outcome: reqlog.OutcomeSuccess, LatencyMs: 120,
		Model: "glm-5.2", PromptTokens: 10, CompletionTokens: 20, TotalTokens: 30,
		TPS: 50, TTFTMs: 800})
	rl.Record(reqlog.Event{Outcome: reqlog.OutcomeHTTPError, LatencyMs: 200, Model: "glm-5.2"})
	ut := usage.New("", 0)
	ut.Add(usage.Delta{Model: "glm-5.2", PromptTokens: 10, CompletionTokens: 20, TotalTokens: 30})
	ut.AddTiming("glm-5.2", 20, 400, 800, 700, 80)

	h := NewHandler(Config{
		Pool:       testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999}),
		RequestLog: rl,
		Usage:      ut,
	})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("code=%d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain; version=0.0.4") {
		t.Errorf("content-type=%q", ct)
	}
	body := rec.Body.String()
	for _, want := range []string{
		"# HELP trae2web_up ", "# TYPE trae2web_up gauge",
		"trae2web_uptime_seconds", `trae2web_accounts{state="total"} 1`,
		`trae2web_requests_total{outcome="success"} 1`,
		`trae2web_requests_total{outcome="failure"} 1`,
		`trae2web_tokens_total{kind="completion"} 20`,
		`trae2web_model_requests_total{model="glm-5.2"} 1`,
		`trae2web_model_tokens_total{model="glm-5.2",kind="cache_read"} 0`,
		`trae2web_model_tps{model="glm-5.2"} 50`,
		"trae2web_in_flight 0",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("缺少 %q\n---\n%s", want, body)
		}
	}
}

// TestMetricsEscapesLabels 校验标签值转义：模型名可能含引号/反斜杠/换行，
// 不转义会让整个 exposition 解析失败，Prometheus 侧表现为 target down。
func TestMetricsEscapesLabels(t *testing.T) {
	ut := usage.New("", 0)
	ut.Add(usage.Delta{Model: "we\"ird\\name\n2", PromptTokens: 1})
	h := NewHandler(Config{Usage: ut})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body := rec.Body.String()
	want := `model="we\"ird\\name\n2"`
	if !strings.Contains(body, want) {
		t.Fatalf("转义不符，期望含 %s\n---\n%s", want, body)
	}
}

func TestPromEscape(t *testing.T) {
	cases := map[string]string{
		"plain": "plain",
		`a"b`:   `a\"b`,
		`a\b`:   `a\\b`,
		"a\nb":  `a\nb`,
		"中文-模型": "中文-模型",
	}
	for in, want := range cases {
		if got := promEscape(in); got != want {
			t.Errorf("promEscape(%q)=%q want %q", in, got, want)
		}
	}
}

// TestMetricsNoAuth 记录一个有意为之的决定：/metrics 不鉴权。
// 若有人日后改成鉴权，Prometheus 默认抓取会 401，这个测试会立刻失败，
// 强制改动者去更新 README 的部署说明（IP 白名单）。
func TestMetricsNoAuth(t *testing.T) {
	h := NewHandler(Config{APIKey: "secret", Pool: testPoolWith()})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("metrics 应匿名可读, code=%d", rec.Code)
	}
	// 对照：业务端点仍然要 key。
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, httptest.NewRequest("GET", "/v1/models", nil))
	if rec2.Code != http.StatusUnauthorized {
		t.Fatalf("/v1/models 应要求 key, code=%d", rec2.Code)
	}
}

// TestMetricsFormatStable 校验浮点格式化：整数不带小数点（否则 Grafana 里
// 常见的 counter 会显示成 1.000000e+00 这种难读形式）。
func TestFormatPromFloat(t *testing.T) {
	if got := formatPromFloat(1); got != "1" {
		t.Errorf("1 → %q", got)
	}
	if got := formatPromFloat(0.5); got != "0.5" {
		t.Errorf("0.5 → %q", got)
	}
	if got := formatPromFloat(58.7); got != "58.7" {
		t.Errorf("58.7 → %q", got)
	}
}

// TestMetricsNoPoolNoPanic 边界：Pool/Usage/RequestLog 全 nil 时不能 panic
// （最小配置启动 + 立刻抓取，是运维最常见的组合）。
func TestMetricsNilDeps(t *testing.T) {
	h := NewHandler(Config{})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("code=%d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "trae2web_up 1") {
		t.Fatalf("body=%s", rec.Body)
	}
	_ = time.Now
}

// readiness 与 liveness 必须语义分离：
// 没有可用账号时 /readyz 要 503（让 LB 摘流），但 /healthz 仍要 200
// （账号全冷却等自愈即可，重启进程毫无帮助还会丢冷却计时）。
func TestReadyzSeparatesFromHealthz(t *testing.T) {
	empty := pool.New("")
	h := NewHandler(Config{Pool: empty})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("empty pool readyz=%d want 503", rec.Code)
	}
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("empty pool healthz=%d want 200 (liveness must not depend on accounts)", rec.Code)
	}

	withAcct := testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at", ExpiresAt: 9999999999})
	h2 := NewHandler(Config{Pool: withAcct})
	rec = httptest.NewRecorder()
	h2.ServeHTTP(rec, httptest.NewRequest("GET", "/readyz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("readyz with account=%d want 200", rec.Code)
	}
}
