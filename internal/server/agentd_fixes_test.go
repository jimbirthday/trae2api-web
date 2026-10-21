package server

// 本轮安全/协议修复的回归测试（Agent D 负责的文件范围）。
// 每个用例对应审计报告里的一条问题，避免改动被后续重构悄悄回退。

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"trae2api-web/internal/auth"
	"trae2api-web/internal/pool"
	"trae2api-web/internal/upstream"
)

func newTestHandler(t *testing.T, cfg Config) *Handler {
	t.Helper()
	if cfg.Upstream == nil {
		cfg.Upstream = newFakeUpstream(t, func(string) (int, string, bool) { return 200, soloSSE, true })
	}
	if cfg.Pool == nil {
		cfg.Pool = testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999})
	}
	if cfg.AuthDir == "" {
		cfg.AuthDir = t.TempDir()
	}
	return NewHandler(cfg)
}

// CORS：OPTIONS 预检 204，且回显允许来源；普通请求也带 X-Request-ID。
func TestCORSAndRequestID(t *testing.T) {
	h := newTestHandler(t, Config{})
	req := httptest.NewRequest("OPTIONS", "/v1/chat/completions", nil)
	req.Header.Set("Origin", "https://app.example.com")
	req.Header.Set("Access-Control-Request-Method", "POST")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("preflight code=%d", rec.Code)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Fatalf("allow-origin=%q", got)
	}
	if !strings.Contains(rec.Header().Get("Access-Control-Allow-Headers"), "Authorization") {
		t.Fatalf("allow-headers=%q", rec.Header().Get("Access-Control-Allow-Headers"))
	}
	if rec.Header().Get("Access-Control-Allow-Credentials") != "" {
		t.Fatal("Bearer 鉴权不应设置 Allow-Credentials（否则 * 非法）")
	}

	req = httptest.NewRequest("GET", "/v1/models", nil)
	req.Header.Set("X-Request-ID", "req-abc")
	req.Header.Set("Origin", "https://app.example.com")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if got := rec.Header().Get("X-Request-ID"); got != "req-abc" {
		t.Fatalf("request id 未沿用请求头：%q", got)
	}
	if got := rec.Header().Get("Access-Control-Expose-Headers"); !strings.Contains(got, "X-Request-ID") {
		t.Fatalf("expose-headers=%q", got)
	}
}

// /authorize 不做跨域（回调 query 里带 token，绝不能允许脚本读取）。
func TestAuthorizeHasNoCORS(t *testing.T) {
	h := newTestHandler(t, Config{})
	req := httptest.NewRequest("GET", "/authorize?tw2a_state=x", nil)
	req.Header.Set("Origin", "https://evil.example.com")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Fatalf("/authorize 不应返回 CORS 头，got %q", got)
	}
}

// /v1/models/{id}：命中返回单模型对象，未命返回 404 not_found_error。
func TestModelByID(t *testing.T) {
	h := newTestHandler(t, Config{})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/v1/models/glm-5.2", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body)
	}
	var m map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &m); err != nil {
		t.Fatalf("json: %v", err)
	}
	if m["id"] != "glm-5.2" || m["object"] != "model" {
		t.Fatalf("model=%v", m)
	}

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/v1/models/does-not-exist", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("code=%d", rec.Code)
	}
	var e struct {
		Error struct {
			Type string `json:"type"`
			Code string `json:"code"`
		} `json:"error"`
	}
	json.Unmarshal(rec.Body.Bytes(), &e)
	if e.Error.Type != "not_found_error" {
		t.Fatalf("error type=%q", e.Error.Type)
	}
}

// OpenAI 错误 type 映射：不能一律 api_error。
func TestOpenAIErrorTypeMapping(t *testing.T) {
	cases := map[string]string{
		"invalid_api_key":   "authentication_error",
		"permission_denied": "permission_error",
		"not_found":         "not_found_error",
		"rate_limit_hit":    "rate_limit_error",
		"invalid_request":   "invalid_request_error",
		"import_failed":     "invalid_request_error",
		"upstream_error":    "api_error",
	}
	for code, want := range cases {
		if got := openAIErrorType(code); got != want {
			t.Errorf("code %q → %q，want %q", code, got, want)
		}
	}
}

// /authorize 在没有任何待登录会话时必须拒绝，且绝不落盘。
func TestAuthorizeRejectsWithoutPendingLogin(t *testing.T) {
	dir := t.TempDir()
	h := newTestHandler(t, Config{AuthDir: dir})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET",
		"/authorize?refreshToken=abc&userInfo=%7B%22UserID%22%3A%22evil%22%7D", nil))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body)
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 0 {
		t.Fatalf("未授权的回调不应写盘，实际写入 %d 个文件", len(entries))
	}
	if !strings.Contains(rec.Body.String(), "登录回调被拒绝") {
		t.Fatalf("body=%s", rec.Body)
	}
}

// 回调页必须转义注入的标题/详情，避免反射型 XSS。
func TestAuthorizeRenderEscapes(t *testing.T) {
	rec := httptest.NewRecorder()
	authorizeRender(rec, http.StatusBadRequest, "<img src=x onerror=alert(1)>", "<script>x</script>")
	body := rec.Body.String()
	if strings.Contains(body, "<script>x</script>") || strings.Contains(body, "<img src=x") {
		t.Fatalf("未转义：%s", body)
	}
}

// UID 路径穿越：导入含非法 uid 的账号必须 400，且不能在 authDir 之外建文件。
func TestImportRejectsPathTraversalUID(t *testing.T) {
	dir := t.TempDir()
	authDir := filepath.Join(dir, "auths")
	if err := os.MkdirAll(authDir, 0o755); err != nil {
		t.Fatal(err)
	}
	h := newTestHandler(t, Config{AuthDir: authDir})
	payload := `{"json":"{\"accounts\":[{\"user_id\":\"../../pwn\",\"label\":\"x\",\"token\":\"eyJhbGciOiJub25lIn0.eyJkYXRhIjp7ImlkIjoiMTIzIn19.\",\"refresh_token\":\"r\"}]}"}`
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/admin/api/accounts/import", strings.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	h.ServeHTTP(rec, req)
	if rec.Code == http.StatusOK {
		t.Fatalf("非法 uid 应被拒绝，body=%s", rec.Body)
	}
	if _, err := os.Stat(filepath.Join(dir, "pwn.json")); err == nil {
		t.Fatal("路径穿越成功写到了 authDir 之外")
	}
}

// 删除接口：非法 uid 直接 400（历史实现会拼出 authDir 之外的路径并 os.Remove）。
func TestDeleteRejectsInvalidUID(t *testing.T) {
	h := newTestHandler(t, Config{APIKey: ""})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("DELETE", "/admin/api/accounts/bad..uid", nil))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body)
	}
}

// 导出默认脱敏；?raw=1 才给明文。
func TestExportMaskedByDefault(t *testing.T) {
	p := testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at-0123456789abcdef", ExpiresAt: 9999999999})
	h := newTestHandler(t, Config{Pool: p})
	var out struct {
		Masked   bool `json:"masked"`
		Accounts []struct {
			Auth struct {
				AccessToken string `json:"accessToken"`
			} `json:"auth"`
		} `json:"accounts"`
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/admin/api/accounts/export", nil))
	json.Unmarshal(rec.Body.Bytes(), &out)
	if !out.Masked {
		t.Fatal("默认应脱敏")
	}
	if strings.Contains(out.Accounts[0].Auth.AccessToken, "0123456789abcdef") {
		t.Fatalf("默认导出泄漏明文：%q", out.Accounts[0].Auth.AccessToken)
	}

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/admin/api/accounts/export?raw=1", nil))
	out.Accounts = nil
	json.Unmarshal(rec.Body.Bytes(), &out)
	if out.Masked || out.Accounts[0].Auth.AccessToken != "at-0123456789abcdef" {
		t.Fatalf("raw 导出应为明文：%+v", out)
	}
}

// 配置保存：非法形状必须 400，且原文件内容保持不变（不能写坏配置导致下次起不来）。
func TestSaveConfigRejectsInvalidPatch(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	orig := []byte("{\n  \"default_model\": \"glm-5.2\"\n}\n")
	if err := os.WriteFile(path, orig, 0o600); err != nil {
		t.Fatal(err)
	}
	h := newTestHandler(t, Config{ConfigPath: path})

	bad := []string{
		`{"cooldown":{"plan_credit":"-5m"}}`,
		`{"pool":{"max_in_flight":9999}}`,
		`{"schedule":{"checkin_hour":99}}`,
		`{"schedule":{"refresh_hours":[]}}`,
		`{"cooldown":"not-an-object"}`,
		`{"unknown_section":{"a":1}}`,
	}
	for _, body := range bad {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("POST", "/admin/api/config", strings.NewReader(body))
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("body %s → code=%d（应 400）resp=%s", body, rec.Code, rec.Body)
		}
		got, _ := os.ReadFile(path)
		if string(got) != string(orig) {
			t.Fatalf("非法配置不应落盘，文件已被改写：%s", got)
		}
	}

	// 合法 patch 应写入并留备份
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/admin/api/config",
		strings.NewReader(`{"cooldown":{"plan_credit":"12h"},"schedule":{"checkin_hour":9}}`))
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body)
	}
	if _, err := os.Stat(path + ".bak"); err != nil {
		t.Fatal("应保留 config.json.bak 备份")
	}
}

// 面板登录爆破限流：连续 5 次失败后返回 429。
func TestAdminSessionRateLimit(t *testing.T) {
	h := newTestHandler(t, Config{APIKey: "secret"})
	for i := 0; i < 5; i++ {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("POST", "/admin/api/session", strings.NewReader(`{"key":"wrong"}`))
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("第 %d 次 code=%d", i+1, rec.Code)
		}
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/admin/api/session", strings.NewReader(`{"key":"secret"}`))
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("锁定后应 429，实际 %d", rec.Code)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Fatal("429 应带 Retry-After")
	}
}

// GET 额度查询不得改调度状态（只更余额）。
func TestCreditsQueryDoesNotUnfreeze(t *testing.T) {
	// 上游按路径分流：额度接口返回 5000 积分，签到状态接口返回「已签到」。
	up := &upstream.Client{
		HTTP: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			body := `{}`
			switch {
			case strings.HasSuffix(r.URL.Path, upstream.EpEntUsage):
				body = `{"user_entitlement_pack_list":[{"entitlement_base_info":{"quota":{"credits_limit":5000}},"usage":{"credits_amount":0}}]}`
			case strings.HasSuffix(r.URL.Path, upstream.EpCheckinStatus):
				body = `{"checked_in":true,"credits":100,"enable":true}`
			}
			return &http.Response{
				StatusCode: 200,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(body)),
			}, nil
		})},
		AgentHost: "https://fake.example",
		UgHost:    "https://fake.example",
		OAuthHost: "https://fake.example",
		ClientID:  upstream.ClientID,
	}
	p := testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999})
	p.Cooldown("u1", pool.CoolPlan, 30*time.Minute, "test")
	h := newTestHandler(t, Config{Pool: p, Upstream: up})

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/admin/api/credits?refresh=1", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body)
	}
	st, _ := p.Status("u1")
	if !st.Cooling {
		t.Fatalf("GET 查询不应解冻账号：%+v", st)
	}
	if st.Credits != 5000 {
		t.Fatalf("余额应被回写：%d", st.Credits)
	}
}

// 顶层数组导出（[ {...}, {...} ]）必须走批量导入，不能被当成回调链接。
func TestImportTopLevelArray(t *testing.T) {
	dir := t.TempDir()
	h := newTestHandler(t, Config{AuthDir: dir})
	// uid 缺省从 JWT 的 data.id 推导，所以两条要给不同 id（否则会被判重复 uid）。
	jwtA := "eyJhbGciOiJub25lIn0.eyJkYXRhIjp7ImlkIjoiMTExIn19.sig"
	jwtB := "eyJhbGciOiJub25lIn0.eyJkYXRhIjp7ImlkIjoiMjIyIn19.sig"
	body := `[{"name":"a","token":"` + jwtA + `","refreshToken":"r1"},{"name":"b","token":"` + jwtB + `","refreshToken":"r2"}]`
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/admin/api/accounts/import", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body)
	}
	var out struct {
		Total   int `json:"total"`
		Created int `json:"created"`
	}
	json.Unmarshal(rec.Body.Bytes(), &out)
	if out.Total != 2 || out.Created != 2 {
		t.Fatalf("out=%+v", out)
	}
}

// 未知导入格式必须给出可操作的中文提示（列出支持的四类形态）。
func TestImportUnknownFormatHint(t *testing.T) {
	h := newTestHandler(t, Config{})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/admin/api/accounts/import", strings.NewReader(`{"foo":1}`))
	req.Header.Set("Content-Type", "application/json")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("code=%d", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{"回调链接", "accounts", "单个账号对象"} {
		if !strings.Contains(body, want) {
			t.Fatalf("提示缺少 %q：%s", want, body)
		}
	}
}

// 模型别名必须无损：静态表能判定的 id 不能被「首字母大写」改写成别的名字。
func TestMapModelNoLossyRewrite(t *testing.T) {
	h := newTestHandler(t, Config{DefaultModel: "glm-5.2"})
	for _, in := range []string{"glm-5.2", "GLM-5.2", "custom_model_1M", "DeepSeek-V4-Pro", "glm-5.2__dev"} {
		got, err := h.mapModel(in)
		if err != nil {
			t.Fatalf("mapModel(%q): %v", in, err)
		}
		if got != "glm-5.2" && got != "custom_model_1M" && got != "DeepSeek-V4-Pro" {
			t.Fatalf("mapModel(%q) → %q（有损改写）", in, got)
		}
	}
	// auto/空 → 默认模型；未知模型 → 错误
	if got, err := h.mapModel(""); err != nil || got != "glm-5.2" {
		t.Fatalf("auto → %q %v", got, err)
	}
	if _, err := h.mapModel("nope-not-a-model"); err == nil {
		t.Fatal("未知模型应报错")
	}
}

// 后台页面必须带防嵌套/防嗅探响应头，回调页必须 no-referrer（query 里有 token）。
func TestSecurityHeaders(t *testing.T) {
	h := newTestHandler(t, Config{})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/admin", nil))
	if rec.Header().Get("X-Frame-Options") != "DENY" {
		t.Fatalf("admin x-frame-options=%q", rec.Header().Get("X-Frame-Options"))
	}
	if !strings.Contains(rec.Header().Get("Content-Security-Policy"), "frame-ancestors 'none'") {
		t.Fatalf("admin csp=%q", rec.Header().Get("Content-Security-Policy"))
	}

	rec = httptest.NewRecorder()
	authorizeRender(rec, http.StatusOK, "ok", "detail")
	if rec.Header().Get("Referrer-Policy") != "no-referrer" {
		t.Fatalf("authorize referrer-policy=%q", rec.Header().Get("Referrer-Policy"))
	}
	if rec.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("authorize cache-control=%q", rec.Header().Get("Cache-Control"))
	}
}

// 登录回调必须带 state；构造 pending 后仅凭 nonce 才可消费，且只能消费一次。
func TestAuthorizeStateOneTime(t *testing.T) {
	h := newTestHandler(t, Config{PublicBaseURL: "https://trae.example.com"})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("POST", "/admin/api/login", strings.NewReader("{}")))
	if rec.Code != http.StatusOK {
		t.Fatalf("login start code=%d body=%s", rec.Code, rec.Body)
	}
	var start struct {
		PendingID   string `json:"pending_id"`
		CallbackURL string `json:"callback_url"`
		Warn        string `json:"warn"`
	}
	json.Unmarshal(rec.Body.Bytes(), &start)
	if !strings.HasPrefix(start.CallbackURL, "https://trae.example.com/authorize?tw2a_state=") {
		t.Fatalf("callback_url=%q", start.CallbackURL)
	}
	if start.Warn != "" {
		t.Fatalf("配置了公网地址不应告警：%q", start.Warn)
	}
	nonce := strings.TrimPrefix(start.CallbackURL, "https://trae.example.com/authorize?tw2a_state=")

	// state 不匹配 → 拒绝
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/authorize?tw2a_state=deadbeef", nil))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("错误 state code=%d", rec.Code)
	}
	// state 正确但回调缺少凭证 → 消费掉会话后报解析失败（不会二次可用）
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/authorize?tw2a_state="+nonce, nil))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("空回调 code=%d", rec.Code)
	}
	// 同一个 nonce 不能再用
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/authorize?tw2a_state="+nonce, nil))
	if !strings.Contains(rec.Body.String(), "登录回调被拒绝") {
		t.Fatalf("重放应被拒绝：%s", rec.Body)
	}
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("重放 code=%d", rec.Code)
	}
}
