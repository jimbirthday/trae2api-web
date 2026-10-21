package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"trae2api-web/internal/auth"
)

// 导出含明文 token，必须带 Bearer；缺 key 一律 401。
func TestExportRequiresBearer(t *testing.T) {
	p := testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999})
	h := NewHandler(Config{Pool: p, Upstream: newFakeUpstream(t, func(string) (int, string, bool) { return 200, soloSSE, true }),
		APIKey: "k1"})

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/admin/api/accounts/export", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("no-key code=%d", rec.Code)
	}

	req := httptest.NewRequest("GET", "/admin/api/accounts/export", nil)
	req.Header.Set("Authorization", "Bearer wrong")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("wrong-key code=%d", rec.Code)
	}

	req = httptest.NewRequest("GET", "/admin/api/accounts/export", nil)
	req.Header.Set("Authorization", "Bearer k1")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body)
	}
	var out struct {
		Format   string `json:"format"`
		Accounts []struct {
			Auth struct {
				AccessToken string `json:"accessToken"`
			} `json:"auth"`
			Account struct {
				UID string `json:"uid"`
			} `json:"account"`
		} `json:"accounts"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("json: %v", err)
	}
	if out.Format != "trae2api-web-accounts" || len(out.Accounts) != 1 {
		t.Fatalf("out=%+v", out)
	}
	// 导出必须是完整明文（用于迁移），而不是脱敏
	if out.Accounts[0].Auth.AccessToken != "at1" || out.Accounts[0].Account.UID != "u1" {
		t.Fatalf("acct=%+v", out.Accounts[0])
	}
	// 导出的内容必须能被本仓库 Parse 回读（闭环）
	raw, _ := json.Marshal(out.Accounts[0])
	if _, err := auth.Parse(raw); err != nil {
		t.Fatalf("reimport parse: %v", err)
	}
}

// 连通性测试：上游 200 → ok=true，且不污染调度状态（不进冷却、不计连败）。
func TestAccountTestOK(t *testing.T) {
	p := testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999})
	up := newFakeUpstream(t, func(string) (int, string, bool) { return 200, soloSSE, true })
	h := NewHandler(Config{Pool: p, Upstream: up})

	req := httptest.NewRequest("POST", "/admin/api/accounts/u1/test?model=glm-5.2", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body)
	}
	var out map[string]any
	json.Unmarshal(rec.Body.Bytes(), &out)
	if out["ok"] != true {
		t.Fatalf("out=%v", out)
	}
	if p.InFlight("u1") != 0 {
		t.Fatal("test endpoint leaked in-flight lease")
	}
}

// 连通性测试：上游 1005 → ok=false 且带 error；同样不得冷却账号。
func TestAccountTestUpstreamFailure(t *testing.T) {
	p := testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999})
	up := newFakeUpstream(t, func(string) (int, string, bool) {
		return 200, "event:error\ndata:{\"code\":1005,\"message\":\"plan limit\"}\n\n", true
	})
	h := NewHandler(Config{Pool: p, Upstream: up})

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("POST", "/admin/api/accounts/u1/test", nil))
	if rec.Code != 200 {
		t.Fatalf("code=%d", rec.Code)
	}
	var out map[string]any
	json.Unmarshal(rec.Body.Bytes(), &out)
	if out["ok"] != false || out["error"] == nil {
		t.Fatalf("out=%v", out)
	}
	for _, s := range p.List() {
		if s.Cooling {
			t.Fatalf("test endpoint must not cool down account: %+v", s)
		}
		if s.ConsecutiveFails != 0 {
			t.Fatalf("test endpoint must not count failure: %+v", s)
		}
	}
}

// 未知 uid → 404。
func TestAccountTestUnknownUID(t *testing.T) {
	p := testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999})
	h := NewHandler(Config{Pool: p, Upstream: newFakeUpstream(t, func(string) (int, string, bool) { return 200, soloSSE, true })})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("POST", "/admin/api/accounts/nope/test", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("code=%d", rec.Code)
	}
}

// 中转格式批量导入：一次灌多个号，逐条结果 + 落盘 + 入池。
func TestImportRelayBatch(t *testing.T) {
	dir := t.TempDir()
	p := testPoolWith()
	h := NewHandler(Config{Pool: p, AuthDir: dir,
		Upstream: newFakeUpstream(t, func(string) (int, string, bool) { return 200, soloSSE, true })})

	payload := `{"format":"trae-cn-relay-accounts","version":1,"accounts":[
	  {"user_id":"111","label":"甲","token":"jwt-1","expired_at":"1793756328612"},
	  {"user_id":"222","label":"乙","token":"jwt-2","expired_at":"1793756328612"}
	]}`
	body := `{"json":` + mustJSON(t, payload) + `}`
	req := httptest.NewRequest("POST", "/admin/api/accounts/import", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body)
	}
	var out struct {
		Created int `json:"created"`
		Total   int `json:"total"`
	}
	json.Unmarshal(rec.Body.Bytes(), &out)
	if out.Created != 2 || out.Total != 2 {
		t.Fatalf("out=%+v", out)
	}
	if len(p.List()) != 2 {
		t.Fatalf("pool size=%d", len(p.List()))
	}
	for _, uid := range []string{"111", "222"} {
		fp := filepath.Join(dir, "trae-"+uid+".json")
		if _, err := os.Stat(fp); err != nil {
			t.Fatalf("missing %s", fp)
		}
	}
	// 再导入一次 → updated 而不是重复新增
	rec = httptest.NewRecorder()
	req = httptest.NewRequest("POST", "/admin/api/accounts/import", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	h.ServeHTTP(rec, req)
	json.Unmarshal(rec.Body.Bytes(), &out)
	if out.Created != 0 || len(p.List()) != 2 {
		t.Fatalf("reimport out=%+v pool=%d", out, len(p.List()))
	}
}

func mustJSON(t *testing.T, s string) string {
	t.Helper()
	b, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// 面板保存 pool.max_in_flight 后必须立刻生效（不用重启）。
func TestSaveConfigHotAppliesMaxInFlight(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.json")
	if err := os.WriteFile(cfgPath, []byte(`{"pool":{"max_in_flight":2}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	p := testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999})
	h := NewHandler(Config{Pool: p, Upstream: newFakeUpstream(t, func(string) (int, string, bool) {
		return 200, soloSSE, true
	}), ConfigPath: cfgPath})

	req := httptest.NewRequest("POST", "/admin/api/config",
		strings.NewReader(`{"pool":{"max_in_flight":5}}`))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body)
	}
	if got := p.MaxInFlight(); got != 5 {
		t.Fatalf("max_in_flight=%d want 5（应即时生效）", got)
	}
	var out struct {
		Hot []string `json:"hot_applied"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	if len(out.Hot) != 1 || out.Hot[0] != "pool.max_in_flight" {
		t.Fatalf("hot_applied=%v", out.Hot)
	}
}
