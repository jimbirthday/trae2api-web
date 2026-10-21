package server

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"trae2api-web/internal/upstream"
)

// 兼容 3003：上游对「该账号跑不了这个模型」返回 `event:error code=3003 all models failed`。
// 这类错误必须只锁该模型，不能把整个账号熔断（否则其他模型也跟着不可用）。
func TestStreamModelUnavailableDoesNotCoolAccount(t *testing.T) {
	up := newFakeUpstream(t, func(string) (int, string, bool) {
		return 200, "event:error\ndata:{\"code\":3003,\"message\":\"all models failed\"}\n\n", true
	})
	up.SetModelErrorRules([]upstream.ModelErrorRule{{Code: 3003, Kind: "model_unavailable"}})

	p, _ := newSchedPool(t, 1)
	h := NewHandler(Config{Pool: p, Upstream: up, MaxRotate: 1, ErrCooldown: time.Minute})

	req := httptest.NewRequest("POST", "/v1/chat/completions",
		strings.NewReader(`{"model":"custom_model_placeholder","stream":true,"messages":[]}`))
	h.ServeHTTP(httptest.NewRecorder(), req)

	st, _ := p.Status("u0")
	if st.Cooling {
		t.Fatalf("3003 must not cool the whole account: %+v", st)
	}
	if len(st.RateLimitedModels) != 1 || st.RateLimitedModels[0].Model != "custom_model_placeholder" {
		t.Fatalf("want model cooldown for custom_model_placeholder: %+v", st.RateLimitedModels)
	}
	// 其他模型不受影响。
	if p.PickByUIDForModel("u0", "glm-5.2") == nil {
		t.Fatal("account should still route glm-5.2")
	}
}
