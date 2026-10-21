package scheduler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"trae2api-web/internal/auth"
	"trae2api-web/internal/pool"
	"trae2api-web/internal/upstream"
)

// 进程在计划时间之后重启：当天该跑的任务必须补跑一次，且只补一次。
func TestCatchUpRunsMissedTaskOnce(t *testing.T) {
	f := &fakeUpstream{resourceRemain: 100}
	srv := f.server()
	defer srv.Close()

	dir := t.TempDir()
	p := pool.New("")
	p.Add(&auth.Auth{UID: "u1", AccessToken: "at", RefreshToken: "rt", ExpiresAt: 9999999999})

	// 把三个任务都设在「1 小时前」，保证 catchUp 认为今天该跑但没跑。
	hour := time.Now().Add(-time.Hour).Hour()
	s := New(Config{
		Pool:         p,
		Upstream:     newTestClient(srv),
		CheckinHour:  hour,
		RefreshHours: []int{hour},
		BalanceHours: []int{hour},
		StateFile:    filepath.Join(dir, "schedule.json"),
	})
	s.catchUp(context.Background())

	if got := f.checkinCalls.Load(); got != 1 {
		t.Fatalf("checkin calls=%d want 1 (catch-up)", got)
	}
	// 同一天再次 catchUp 不应重复跑。
	s.catchUp(context.Background())
	if got := f.checkinCalls.Load(); got != 1 {
		t.Fatalf("checkin calls=%d want 1 (no duplicate same day)", got)
	}

	// 运行记录必须落盘，供下次启动判断。
	raw, err := os.ReadFile(filepath.Join(dir, "schedule.json"))
	if err != nil {
		t.Fatalf("schedule.json not written: %v", err)
	}
	var f2 runLogFile
	if err := json.Unmarshal(raw, &f2); err != nil {
		t.Fatalf("schedule.json invalid: %v", err)
	}
	today := time.Now().Format(dateLayout)
	for _, key := range []string{"checkin", "refresh:" + itoa(hour), "balance:" + itoa(hour)} {
		if f2.LastRun[key] != today {
			t.Fatalf("last_run[%s]=%q want %q", key, f2.LastRun[key], today)
		}
	}

	// 重启（新 Scheduler 读同一文件）后同一天不得重跑。
	s2 := New(Config{
		Pool:         p,
		Upstream:     newTestClient(srv),
		CheckinHour:  hour,
		RefreshHours: []int{hour},
		BalanceHours: []int{hour},
		StateFile:    filepath.Join(dir, "schedule.json"),
	})
	s2.catchUp(context.Background())
	if got := f.checkinCalls.Load(); got != 1 {
		t.Fatalf("restart re-ran today's task: calls=%d", got)
	}
}

// ctx 取消后不再派发新账号。
func TestRunCheckinContextCancel(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if strings.HasSuffix(r.URL.Path, "/checkin_credits/status") {
			w.Write([]byte(`{"checked_in":true,"credits":10,"enable":true}`))
			return
		}
		w.Write([]byte(`{"is_credits_billing":true,"user_entitlement_pack_list":[]}`))
	}))
	defer srv.Close()

	p := pool.New("")
	for _, uid := range []string{"u1", "u2", "u3", "u4", "u5", "u6", "u7", "u8"} {
		p.Add(&auth.Auth{UID: uid, AccessToken: "at", RefreshToken: "rt", ExpiresAt: 9999999999})
	}
	s := New(Config{Pool: p, Upstream: newTestClient(srv), Concurrency: 1})

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 立刻取消：一个请求都不该发出去
	s.RunCheckinContext(ctx)
	if n := hits.Load(); n != 0 {
		t.Fatalf("cancelled run issued %d upstream calls", n)
	}
}

// 并发上限必须生效（否则批量签到会把上游打限流）。
func TestRunCheckinConcurrencyBounded(t *testing.T) {
	f := &fakeUpstream{resourceRemain: 10}
	srv := f.server()
	defer srv.Close()

	p := pool.New("")
	for _, uid := range []string{"u1", "u2", "u3", "u4", "u5", "u6"} {
		p.Add(&auth.Auth{UID: uid, AccessToken: "at", RefreshToken: "rt", ExpiresAt: 9999999999})
	}
	s := New(Config{Pool: p, Upstream: newTestClient(srv), Concurrency: 2})
	if s.cfg.Concurrency != 2 {
		t.Fatalf("concurrency=%d want 2", s.cfg.Concurrency)
	}
	s.RunCheckinNow()
	if got := f.checkinCalls.Load(); got != 6 {
		t.Fatalf("checkin calls=%d want 6", got)
	}
}

// 并发上限的夹紧：0 回落默认，过大被钳到上限。
func TestConcurrencyClamp(t *testing.T) {
	if got := New(Config{Concurrency: 0}).cfg.Concurrency; got != defaultConcurrency {
		t.Fatalf("zero concurrency=%d want %d", got, defaultConcurrency)
	}
	if got := New(Config{Concurrency: 1000}).cfg.Concurrency; got != maxConcurrencyCap {
		t.Fatalf("huge concurrency=%d want %d", got, maxConcurrencyCap)
	}
}

func newTestClient(srv *httptest.Server) *upstream.Client {
	return &upstream.Client{
		HTTP:      srv.Client(),
		AgentHost: srv.URL,
		UgHost:    srv.URL,
		OAuthHost: srv.URL,
		ClientID:  upstream.ClientID,
	}
}
