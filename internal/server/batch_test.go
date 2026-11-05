package server

import (
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"trae2api-web/internal/auth"
)

func TestBatchStatusSelection(t *testing.T) {
	for _, tc := range []struct {
		body string
		code int
	}{
		{`{"action":"pause","uids":["u2","u2"]}`, 200},
		{`{"action":"pause","uids":[]}`, 400},
		{`{"action":"pause","uids":["u1","missing"]}`, 400},
		{`{"action":"pause","uids":["../u2"]}`, 400},
		{`{"action":"invalid","uids":["u1"]}`, 400},
	} {
		t.Run(tc.body, func(t *testing.T) {
			p := testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1"}, &auth.Auth{UID: "u2", AccessToken: "at2"})
			h := newTestHandler(t, Config{Pool: p})
			rec := doJSON(t, h, "POST", "/admin/api/accounts/set_all", tc.body)
			if rec.Code != tc.code {
				t.Fatalf("code=%d body=%s", rec.Code, rec.Body)
			}
			first, _ := p.Status("u1")
			second, _ := p.Status("u2")
			if first.Paused || second.Paused != (tc.code == 200) {
				t.Fatalf("selection changed unexpected accounts: u1=%v u2=%v", first.Paused, second.Paused)
			}
			if tc.code == 200 && !strings.Contains(rec.Body.String(), `"changed":1`) {
				t.Fatal("duplicate UID was counted twice")
			}
		})
	}
}

func TestBatchCheckinSelection(t *testing.T) {
	p := testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1"}, &auth.Auth{UID: "u2", AccessToken: "at2"})
	var calls atomic.Int64
	h := newTestHandler(t, Config{Pool: p, Upstream: newFakeUpstreamWithCalls(t, &calls)})
	rec := doJSON(t, h, "POST", "/admin/api/checkin_all", `{"uids":["u2","u2"]}`)
	if rec.Code != 200 || calls.Load() != 2 || p.CheckinDoneToday("u1") || !p.CheckinDoneToday("u2") {
		t.Fatalf("incorrect checkin scope: code=%d calls=%d body=%s", rec.Code, calls.Load(), rec.Body)
	}
	for _, body := range []string{`{"uids":[]}`, `{"uids":["missing"]}`, `{`} {
		rec = doJSON(t, h, "POST", "/admin/api/checkin_all", body)
		if rec.Code != 400 || calls.Load() != 2 {
			t.Fatalf("invalid selection contacted upstream: code=%d calls=%d", rec.Code, calls.Load())
		}
	}
}

func TestBatchExportSelection(t *testing.T) {
	p := testPoolWith(&auth.Auth{UID: "u1", AccessToken: "first-account-secret-token"}, &auth.Auth{UID: "u2", AccessToken: "second-account-secret-token"})
	h := newTestHandler(t, Config{Pool: p, APIKey: "test-key"})
	for _, raw := range []bool{false, true} {
		path := "/admin/api/accounts/export"
		if raw {
			path += "?raw=1"
		}
		req := httptest.NewRequest("POST", path, strings.NewReader(`{"uids":["u2"]}`))
		req.Header.Set("Authorization", "Bearer test-key")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		var out struct {
			Accounts []struct {
				Account struct{ UID string }
				Auth    struct{ AccessToken string }
			}
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatal(err)
		}
		if rec.Code != 200 || len(out.Accounts) != 1 || out.Accounts[0].Account.UID != "u2" || strings.Contains(rec.Body.String(), "first-account") {
			t.Fatalf("incorrect export scope: %s", rec.Body)
		}
		if (out.Accounts[0].Auth.AccessToken == "second-account-secret-token") != raw {
			t.Fatal("export did not respect masking")
		}
	}
	rec := doJSON(t, h, "POST", "/admin/api/accounts/export", `{"uids":["u2"]}`)
	if rec.Code != 401 {
		t.Fatal("selected export bypassed authentication")
	}
	for _, body := range []string{`{}`, `{"uids":null}`, `{"uids":[]}`, `{"uids":["missing"]}`, `{"uids":["../u1"]}`, `{`} {
		req := httptest.NewRequest("POST", "/admin/api/accounts/export?raw=1", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer test-key")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != 400 || strings.Contains(rec.Body.String(), "account-secret-token") {
			t.Fatalf("invalid selection exported credentials: code=%d body=%s", rec.Code, rec.Body)
		}
	}
}

func TestDeleteAccountKeepsPoolOnFileError(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "keep"), []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	p := testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at", FilePath: dir})
	h := newTestHandler(t, Config{Pool: p})
	rec := doJSON(t, h, "DELETE", "/admin/api/accounts/u1", "")
	if _, exists := p.Status("u1"); rec.Code != 500 || !exists {
		t.Fatalf("file error lost account or reported success: code=%d", rec.Code)
	}
}

func TestDeleteMissingAccountKeepsOrphanFile(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "trae-missing.json")
	if err := os.WriteFile(file, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	h := newTestHandler(t, Config{AuthDir: dir})
	rec := doJSON(t, h, "DELETE", "/admin/api/accounts/missing", "")
	if _, err := os.Stat(file); rec.Code != 404 || err != nil {
		t.Fatalf("missing account deleted unrelated file: code=%d err=%v", rec.Code, err)
	}
}

func TestAccountSummaryIgnoresPageAndFilter(t *testing.T) {
	p := testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1"}, &auth.Auth{UID: "u2", AccessToken: "at2"})
	p.SetEnabled("u2", false, "test")
	p.SetCredits("u1", 100, 100)
	p.SetCredits("u2", 200, 200)
	h := newTestHandler(t, Config{Pool: p})
	rec := doJSON(t, h, "GET", "/admin/api/accounts?page_size=1&q=u2", "")
	var out struct {
		Total   int
		Summary map[string]int64
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if rec.Code != 200 || out.Total != 1 || out.Summary["total"] != 2 || out.Summary["available"] != 1 || out.Summary["attention"] != 1 || out.Summary["credits"] != 300 {
		t.Fatalf("incorrect summary: %s", rec.Body)
	}
}
