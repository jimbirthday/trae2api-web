package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"trae2api-web/internal/auth"
	"trae2api-web/internal/upstream"
)

func gateHandler() *Handler {
	p := testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999})
	return NewHandler(Config{Pool: p, Upstream: upstream.New(), APIKey: "secret-k"})
}

// 面板页面未登录必须返回登录页（401），不能先把 HTML 给出去。
func TestAdminPageRequiresKey(t *testing.T) {
	h := gateHandler()

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/admin", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("code=%d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "输入 API Key") {
		t.Fatalf("should render login form, got: %.200s", body)
	}
	// 未登录绝不能泄漏控制台内容
	for _, leaked := range []string{"账号管理", "请求流水", "acctTable"} {
		if strings.Contains(body, leaked) {
			t.Fatalf("login page leaked console markup %q", leaked)
		}
	}

	// 带 Bearer 才给真页面
	req := httptest.NewRequest("GET", "/admin", nil)
	req.Header.Set("Authorization", "Bearer secret-k")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "acctTable") {
		t.Fatalf("authed code=%d", rec.Code)
	}
}

// 读接口同样要鉴权（此前 accounts/credits 等读接口是无鉴权的）。
func TestAdminReadAPIsRequireKey(t *testing.T) {
	h := gateHandler()
	for _, path := range []string{
		"/admin/api/accounts", "/admin/api/credits", "/admin/api/requests",
		"/admin/api/metrics", "/admin/api/config", "/admin/api/usage",
	} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest("GET", path, nil))
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("%s: code=%d, want 401", path, rec.Code)
		}
	}
}

// 用 key 换 cookie，之后仅凭 cookie 即可读面板与数据。
func TestAdminSessionCookieFlow(t *testing.T) {
	h := gateHandler()

	// 错误 key 换不到 cookie
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("POST", "/admin/api/session", strings.NewReader(`{"key":"wrong"}`)))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("wrong key code=%d", rec.Code)
	}
	if len(rec.Result().Cookies()) != 0 {
		t.Fatal("wrong key must not set cookie")
	}

	// 正确 key → cookie
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("POST", "/admin/api/session", strings.NewReader(`{"key":"secret-k"}`)))
	if rec.Code != http.StatusOK {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body)
	}
	cookies := rec.Result().Cookies()
	if len(cookies) != 1 || cookies[0].Name != adminCookieName {
		t.Fatalf("cookies=%v", cookies)
	}
	c := cookies[0]
	if !c.HttpOnly || c.SameSite != http.SameSiteStrictMode || c.Path != adminCookiePath {
		t.Fatalf("cookie attrs: httponly=%v samesite=%v path=%s", c.HttpOnly, c.SameSite, c.Path)
	}
	// cookie 值不得等于明文 key
	if c.Value == "secret-k" || strings.Contains(c.Value, "secret-k") {
		t.Fatal("cookie must not contain the raw key")
	}

	// 仅凭 cookie 访问页面 + 数据
	req := httptest.NewRequest("GET", "/admin", nil)
	req.AddCookie(c)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("cookie page code=%d", rec.Code)
	}
	req = httptest.NewRequest("GET", "/admin/api/accounts", nil)
	req.AddCookie(c)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("cookie api code=%d", rec.Code)
	}

	// 伪造 cookie 值无效
	bad := &http.Cookie{Name: adminCookieName, Value: "deadbeef", Path: adminCookiePath}
	req = httptest.NewRequest("GET", "/admin/api/accounts", nil)
	req.AddCookie(bad)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("forged cookie code=%d", rec.Code)
	}
}

// 换 key 后旧 cookie 立刻失效（派生值与 key 绑定）。
func TestAdminCookieInvalidatedOnKeyRotation(t *testing.T) {
	h := gateHandler()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("POST", "/admin/api/session", strings.NewReader(`{"key":"secret-k"}`)))
	c := rec.Result().Cookies()[0]

	h2 := NewHandler(Config{Pool: testPoolWith(), Upstream: upstream.New(), APIKey: "rotated-k"})
	req := httptest.NewRequest("GET", "/admin/api/accounts", nil)
	req.AddCookie(c)
	rec = httptest.NewRecorder()
	h2.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("old cookie should die after key rotation, code=%d", rec.Code)
	}
}

// 退出登录清除 cookie。
func TestAdminLogout(t *testing.T) {
	h := gateHandler()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("DELETE", "/admin/api/session", nil))
	cookies := rec.Result().Cookies()
	if len(cookies) != 1 || cookies[0].MaxAge >= 0 {
		t.Fatalf("logout should expire cookie, got %v", cookies)
	}
}

// 未配置 API Key 时不设门禁（本地场景保持可用）。
func TestAdminOpenWhenNoKey(t *testing.T) {
	h := NewHandler(Config{Pool: testPoolWith(), Upstream: upstream.New()})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/admin/api/accounts", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("code=%d", rec.Code)
	}
	var v map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
		t.Fatal(err)
	}
}
