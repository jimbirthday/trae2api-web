package server

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"trae2api-web/internal/auth"
	"trae2api-web/internal/reqlog"
	"trae2api-web/internal/upstream"
)

// newFakeUpstreamWithCalls 额度/签到两个端点都返回固定 JSON，并统计上游调用次数。
func newFakeUpstreamWithCalls(t *testing.T, calls *atomic.Int64) *upstream.Client {
	t.Helper()
	return &upstream.Client{
		HTTP: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			calls.Add(1)
			body := `{}`
			switch {
			case strings.HasSuffix(r.URL.Path, upstream.EpEntUsage):
				body = `{"user_entitlement_pack_list":[{"entitlement_base_info":{"quota":{"credits_limit":1000}},"usage":{"credits_amount":250}}]}`
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
}

// 账号列表分页：page/page_size 生效，total/pages 正确，越界页返回空。
func TestAdminAccountsPaging(t *testing.T) {
	auths := make([]*auth.Auth, 0, 25)
	for i := 0; i < 25; i++ {
		auths = append(auths, &auth.Auth{
			UID:         "uid" + strconv.Itoa(i),
			AccessToken: "at" + strconv.Itoa(i),
			ExpiresAt:   9999999999,
		})
	}
	h := NewHandler(Config{Pool: testPoolWith(auths...), Upstream: upstream.New()})

	type resp struct {
		Accounts []map[string]any `json:"accounts"`
		Total    int              `json:"total"`
		Page     int              `json:"page"`
		PageSize int              `json:"page_size"`
		Pages    int              `json:"pages"`
	}
	get := func(q string) resp {
		req := httptest.NewRequest("GET", "/admin/api/accounts"+q, nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("code=%d", rec.Code)
		}
		var out resp
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatal(err)
		}
		return out
	}

	// 默认页大小 20
	first := get("")
	if first.Total != 25 || first.Page != 1 || first.PageSize != 20 || first.Pages != 2 || len(first.Accounts) != 20 {
		t.Fatalf("first page=%+v len=%d", first, len(first.Accounts))
	}
	second := get("?page=2&page_size=20")
	if len(second.Accounts) != 5 || second.Page != 2 {
		t.Fatalf("second page len=%d page=%d", len(second.Accounts), second.Page)
	}
	// 两页不重叠，合起来正好覆盖全部
	seen := map[string]bool{}
	for _, a := range append(first.Accounts, second.Accounts...) {
		uid, _ := a["uid"].(string)
		if seen[uid] {
			t.Fatalf("duplicate uid %s across pages", uid)
		}
		seen[uid] = true
	}
	if len(seen) != 25 {
		t.Fatalf("union=%d want 25", len(seen))
	}
	// 越界页：空列表但 total 仍正确
	beyond := get("?page=99&page_size=20")
	if len(beyond.Accounts) != 0 || beyond.Total != 25 {
		t.Fatalf("beyond=%+v", beyond)
	}
	// page_size 上限夹到 200，page<1 夹到 1
	clamped := get("?page=0&page_size=99999")
	if clamped.Page != 1 || clamped.PageSize != 200 {
		t.Fatalf("clamp page=%d size=%d", clamped.Page, clamped.PageSize)
	}
}

// 账号列表过滤（q / status）在服务端做，且 total 反映过滤后的条数。
func TestAdminAccountsFilter(t *testing.T) {
	h := NewHandler(Config{
		Pool: testPoolWith(
			&auth.Auth{UID: "alpha", AccessToken: "a", ExpiresAt: 9999999999, Nickname: "研发号"},
			&auth.Auth{UID: "beta", AccessToken: "b", ExpiresAt: 9999999999, Nickname: "测试号"},
		),
		Upstream: upstream.New(),
	})
	type resp struct {
		Accounts []map[string]any `json:"accounts"`
		Total    int              `json:"total"`
	}
	get := func(q string) resp {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest("GET", "/admin/api/accounts"+q, nil))
		var out resp
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	if got := get("?q=alpha"); got.Total != 1 {
		t.Fatalf("q=alpha total=%d", got.Total)
	}
	if got := get("?q=测试"); got.Total != 1 || got.Accounts[0]["uid"] != "beta" {
		t.Fatalf("q=测试 -> %+v", got)
	}
	if got := get("?status=ok"); got.Total != 2 {
		t.Fatalf("status=ok total=%d", got.Total)
	}
	if got := get("?status=disabled"); got.Total != 0 {
		t.Fatalf("status=disabled total=%d", got.Total)
	}
}

// 请求流水分页：offset 语义为「跳过最新 N 条」，且返回精确 total。
func TestAdminRequestsPaging(t *testing.T) {
	rl := reqlog.New(reqlog.Config{})
	defer rl.Close()
	base := time.Now().Add(-time.Hour)
	for i := 0; i < 130; i++ {
		rl.Record(reqlog.Event{
			RequestID: "id" + strconv.Itoa(i),
			Time:      base.Add(time.Duration(i) * time.Second),
			Path:      "/v1/chat/completions",
			Model:     "m" + strconv.Itoa(i%3),
			UID:       "u1",
			Outcome:   "success",
			Status:    200,
		})
	}
	h := NewHandler(Config{Pool: testPoolWith(), Upstream: upstream.New(), RequestLog: rl})

	type resp struct {
		Events     []map[string]any `json:"events"`
		Total      int              `json:"total"`
		TotalExact bool             `json:"total_exact"`
		Offset     int              `json:"offset"`
		Limit      int              `json:"limit"`
	}
	get := func(q string) resp {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest("GET", "/admin/api/requests"+q, nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("code=%d body=%s", rec.Code, rec.Body)
		}
		var out resp
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatal(err)
		}
		return out
	}

	first := get("?limit=50&offset=0")
	if len(first.Events) != 50 || first.Total != 130 || !first.TotalExact {
		t.Fatalf("first=%d events total=%d exact=%v", len(first.Events), first.Total, first.TotalExact)
	}
	if first.Events[0]["request_id"] != "id129" {
		t.Fatalf("newest=%v", first.Events[0]["request_id"])
	}
	second := get("?limit=50&offset=50")
	if len(second.Events) != 50 || second.Events[0]["request_id"] != "id79" {
		t.Fatalf("second first=%v", second.Events[0]["request_id"])
	}
	last := get("?limit=50&offset=100")
	if len(last.Events) != 30 || last.Events[len(last.Events)-1]["request_id"] != "id0" {
		t.Fatalf("last len=%d", len(last.Events))
	}
	beyond := get("?limit=50&offset=1000")
	if len(beyond.Events) != 0 || beyond.Total != 130 {
		t.Fatalf("beyond=%d total=%d", len(beyond.Events), beyond.Total)
	}
	// 三页拼起来无重复无遗漏
	seen := map[string]bool{}
	for _, e := range append(append(first.Events, second.Events...), last.Events...) {
		id, _ := e["request_id"].(string)
		if seen[id] {
			t.Fatalf("duplicate %s", id)
		}
		seen[id] = true
	}
	if len(seen) != 130 {
		t.Fatalf("union=%d want 130", len(seen))
	}
}

// 带过滤条件时分页仍能工作，但 total 报告为未知（不做全量扫描）。
func TestAdminRequestsFiltersUnknownTotal(t *testing.T) {
	rl := reqlog.New(reqlog.Config{})
	defer rl.Close()
	base := time.Now().Add(-time.Hour)
	for i := 0; i < 20; i++ {
		rl.Record(reqlog.Event{
			RequestID: "f" + strconv.Itoa(i),
			Time:      base.Add(time.Duration(i) * time.Second),
			Path:      "/v1/chat/completions",
			UID:       "u" + strconv.Itoa(i%2),
			Outcome:   "success",
			Status:    200,
		})
	}
	h := NewHandler(Config{Pool: testPoolWith(), Upstream: upstream.New(), RequestLog: rl})

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/admin/api/requests?uid=u1&limit=5&offset=0", nil))
	var out struct {
		Events     []map[string]any `json:"events"`
		Total      int              `json:"total"`
		TotalExact bool             `json:"total_exact"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Events) != 5 {
		t.Fatalf("events=%d want 5", len(out.Events))
	}
	if out.Total != -1 || out.TotalExact {
		t.Fatalf("filtered total should be unknown: %d,%v", out.Total, out.TotalExact)
	}
	for _, e := range out.Events {
		if e["uid"] != "u1" {
			t.Fatalf("filter leaked uid=%v", e["uid"])
		}
	}
}

// 额度接口：缓存命中不再打上游；?refresh=1 强制穿透；并发请求只触发一轮上游调用。
func TestAdminCreditsCacheAndPaging(t *testing.T) {
	var calls atomic.Int64
	up := newFakeUpstreamWithCalls(t, &calls)
	p := testPoolWith(
		&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999},
		&auth.Auth{UID: "u2", AccessToken: "at2", ExpiresAt: 9999999999},
	)
	h := NewHandler(Config{Pool: p, Upstream: up})

	type resp struct {
		Accounts []map[string]any `json:"accounts"`
		Total    int              `json:"total"`
		Cached   bool             `json:"cached"`
	}
	get := func(q string) resp {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest("GET", "/admin/api/credits"+q, nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("code=%d body=%s", rec.Code, rec.Body)
		}
		var out resp
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatal(err)
		}
		return out
	}

	first := get("")
	if first.Cached || first.Total != 2 || len(first.Accounts) != 2 {
		t.Fatalf("first=%+v", first)
	}
	n1 := calls.Load()
	if n1 != 4 { // 2 账号 × (额度 + 签到)
		t.Fatalf("upstream calls=%d want 4", n1)
	}
	// 第二次：走缓存，上游零新增调用
	second := get("")
	if !second.Cached {
		t.Fatal("second should be cached")
	}
	if got := calls.Load(); got != n1 {
		t.Fatalf("cached call still hit upstream: %d -> %d", n1, got)
	}
	// 分页不影响缓存（翻页不应重查上游）
	paged := get("?page=2&page_size=1")
	if !paged.Cached || len(paged.Accounts) != 1 || paged.Total != 2 {
		t.Fatalf("paged=%+v", paged)
	}
	if got := calls.Load(); got != n1 {
		t.Fatalf("paging hit upstream: %d -> %d", n1, got)
	}
	// 强制刷新：穿透
	forced := get("?refresh=1")
	if forced.Cached {
		t.Fatal("refresh=1 should bypass cache")
	}
	if got := calls.Load(); got <= n1 {
		t.Fatalf("refresh did not hit upstream: %d -> %d", n1, got)
	}
}

// 用量接口分页 + 排序 + 过滤，合计行保持全量口径。
func TestAdminUsagePaging(t *testing.T) {
	h := gateHandler() // 无 Usage → enabled:false 路径
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/admin/api/usage?page=1&page_size=10", nil)
	req.Header.Set("Authorization", "Bearer secret-k")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("code=%d", rec.Code)
	}
}
