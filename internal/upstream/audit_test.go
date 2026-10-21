package upstream

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"trae2api-web/internal/auth"
)

// 空流（只有 metadata/done，没有任何内容帧）必须报错，不能伪造
// content:"" + finish_reason:"stop" 的成功响应。
func TestAggregateEmptyStreamIsError(t *testing.T) {
	sse := "event:metadata\ndata:{\"model\":\"glm-5.2\"}\n\n" +
		"event:done\ndata:{\"finish_reason\":\"stop\"}\n\n"
	resp, err := Aggregate(strings.NewReader(sse))
	if !errors.Is(err, ErrEmptyCompletion) {
		t.Fatalf("want ErrEmptyCompletion, got resp=%v err=%v", resp, err)
	}
}

// 有内容但缺 done（连接中途被掐断）→ 截断错误。
func TestAggregateTruncatedStreamIsError(t *testing.T) {
	sse := "event:output\ndata:{\"response\":\"半句话\"}\n\n"
	resp, err := Aggregate(strings.NewReader(sse))
	if !errors.Is(err, ErrTruncatedStream) {
		t.Fatalf("want ErrTruncatedStream, got resp=%v err=%v", resp, err)
	}
}

// 完全空的 body（204/3xx 被当成流时会走到这里）同样是错误。
func TestAggregateEmptyBodyIsError(t *testing.T) {
	if _, err := Aggregate(strings.NewReader("")); !errors.Is(err, ErrEmptyCompletion) {
		t.Fatalf("want ErrEmptyCompletion, got %v", err)
	}
}

// model 字段必须回填成客户端请求的模型名（上游 metadata 常常为空）。
func TestAggregateWithModelFillsRequestedModel(t *testing.T) {
	resp, err := AggregateWithModel(strings.NewReader(soloSSEFixture), "glm-5.2")
	if err != nil {
		t.Fatal(err)
	}
	if resp["id"] == "" || !strings.HasPrefix(resp["id"].(string), "chatcmpl-") {
		t.Errorf("id: %v", resp["id"])
	}
	if got := resp["model"]; got != "glm-5.2" {
		t.Errorf("model=%v want glm-5.2", got)
	}
}

// SSE 多行 data: 必须累积成一份完整 JSON（历史上逐行解析会丢后续行）。
func TestScanLineAccumulatesMultiLineData(t *testing.T) {
	st := &sseState{}
	ev := scanLine(st, "event:output")
	if ev != nil {
		t.Fatal("event line should not emit")
	}
	if ev := scanLine(st, `data:{"response":"ab",`); ev != nil {
		t.Fatal("partial data line should not emit")
	}
	if ev := scanLine(st, `data:"reasoning_content":""}`); ev != nil {
		t.Fatal("last data line should not emit before the blank line")
	}
	ev = scanLine(st, "")
	if ev == nil {
		t.Fatal("blank line should emit accumulated event")
	}
	if ev.Event != "output" || ev.Response != "ab" {
		t.Fatalf("event=%q response=%q", ev.Event, ev.Response)
	}
}

// 204 / 3xx 不能当 SSE 流解析，必须显式报错。
func TestChatStreamRejectsNon200(t *testing.T) {
	cases := []struct {
		name   string
		status int
		header http.Header
	}{
		{"204", 204, http.Header{}},
		{"302", 302, http.Header{"Location": []string{"https://elsewhere.example/x"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := testClient(func(r *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: tc.status, Header: tc.header,
					Body: io.NopCloser(strings.NewReader(""))}, nil
			})
			// 与生产一致：StreamHTTP 不跟随重定向，否则 302 会被跟成 200/GET。
			c.StreamHTTP = &http.Client{
				Transport:     c.HTTP.Transport,
				CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
			}
			rc, status, _, err := c.ChatStream(&auth.Auth{AccessToken: "at", UID: "u1"}, []byte(`{"model":"m"}`))
			if rc != nil {
				rc.Close()
				t.Fatal("rc must be nil for non-stream status")
			}
			if err == nil {
				t.Fatalf("status=%d must be an error, got nil", status)
			}
			if status != tc.status {
				t.Errorf("status=%d want %d", status, tc.status)
			}
			if tc.status == 302 && !strings.Contains(err.Error(), "elsewhere.example") {
				t.Errorf("redirect error should carry Location for debugging: %v", err)
			}
		})
	}
}

// 上游响应头之后静默挂住 → 空闲看门狗中断，并报 ErrStreamIdleTimeout。
func TestChatStreamIdleTimeout(t *testing.T) {
	pr, pw := io.Pipe() // 永不写入：模拟上游挂住
	defer pw.Close()
	c := testClient(func(r *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: 200,
			Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
			Body:       pr,
		}, nil
	})
	c.StreamIdleTimeout = 60 * time.Millisecond
	rc, status, _, err := c.ChatStream(&auth.Auth{AccessToken: "at", UID: "u1"}, []byte(`{"model":"m"}`))
	if err != nil || status != 200 {
		t.Fatalf("status=%d err=%v", status, err)
	}
	defer rc.Close()

	done := make(chan error, 1)
	go func() {
		buf := make([]byte, 64)
		_, rerr := rc.Read(buf)
		done <- rerr
	}()
	select {
	case rerr := <-done:
		if !errors.Is(rerr, ErrStreamIdleTimeout) {
			t.Fatalf("want ErrStreamIdleTimeout, got %v", rerr)
		}
		if !IsUpstreamTimeout(rerr) {
			t.Error("idle timeout must classify as upstream timeout (不罚号)")
		}
		if IsClientGone(rerr) {
			t.Error("idle timeout must not look like client-gone")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("idle timeout did not fire")
	}
}

// 空闲超时关闭后必须能正常 Close（幂等，且不 panic）。
func TestIdleTimeoutBodyCloseIsIdempotent(t *testing.T) {
	pr, pw := io.Pipe()
	c := testClient(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: pr}, nil
	})
	c.StreamIdleTimeout = time.Hour
	rc, _, _, err := c.ChatStream(&auth.Auth{AccessToken: "at", UID: "u1"}, []byte(`{"model":"m"}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("first close: %v", err)
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
	pw.Close()
}

// 1005 只能按 JSON code 字段命中：message 里恰好出现的 "1005" 不能触发 plan 冷却。
func TestClassifyUsesStructuredBizCode(t *testing.T) {
	if got := Classify(400, `{"code":11101,"message":"order 1005 not found"}`); got != ErrClient {
		t.Errorf("substring 1005 must not mean plan limit, got %v", got)
	}
	if got := Classify(400, `{"code":1005,"message":"quota"}`); got != ErrPlanLimit {
		t.Errorf("structured 1005 should be plan limit, got %v", got)
	}
	// 非 JSON 的纯文本且没有 1005：宁可保守按 client 处理。
	// plan 误判的代价是给健康号打 12h 硬冷却，比「少判一次」严重得多。
	if got := Classify(403, `plan limit exceeded`); got != ErrClient {
		t.Errorf("plain text without biz code should stay ErrClient, got %v", got)
	}
	// 401 但没有会话失效标记、body 有内容 → 可重试的 client 错误（不硬禁用账号）
	if got := Classify(401, `{"code":4001,"message":"invalid param"}`); got != ErrClient {
		t.Errorf("401 without session markers should be ErrClient, got %v", got)
	}
	if got := Classify(401, `{"code":1001,"message":"login required"}`); got != ErrSessionDead {
		t.Errorf("401 with login marker should be ErrSessionDead, got %v", got)
	}
}

// 不支持的上游参数必须被剥掉（否则上游严格解码直接 4xx），
// 但未知的透传字段要保留。
func TestPrepareBodyDropsUnsupportedAndMapsMaxCompletionTokens(t *testing.T) {
	src := []byte(`{"model":"glm-5.2","messages":[{"role":"user","content":"hi"}],
		"n":2,"logprobs":true,"seed":7,"parallel_tool_calls":false,"response_format":{"type":"json_object"},
		"max_completion_tokens":128,"custom_field":{"a":1}}`)
	out := PrepareBody(src)
	s := string(out)
	for _, banned := range []string{`"n":`, `"logprobs":`, `"seed":`, `"parallel_tool_calls":`, `"response_format":`, `"max_completion_tokens":`} {
		if strings.Contains(s, banned) {
			t.Errorf("field %s must be dropped: %s", banned, s)
		}
	}
	if !strings.Contains(s, `"max_tokens":128`) {
		t.Errorf("max_completion_tokens must map to max_tokens: %s", s)
	}
	if !strings.Contains(s, `"custom_field"`) {
		t.Errorf("unknown fields must pass through: %s", s)
	}
}

// 空流在流式路径上也必须报错（而不是补个 [DONE] 就当成功），
// 并且 SuppressErrorFrame 时调用方还能拿到错误去换号。
func TestStreamEmptyIsError(t *testing.T) {
	sse := "event:metadata\ndata:{}\n\n" + "event:done\ndata:{\"finish_reason\":\"stop\"}\n\n"
	var sb strings.Builder
	err, st := runStream(&sbWriter{b: &sb}, sse, StreamOptions{SuppressErrorFrame: true})
	if !errors.Is(err, ErrEmptyCompletion) {
		t.Fatalf("want ErrEmptyCompletion, got %v (body=%s)", err, sb.String())
	}
	if strings.Contains(sb.String(), `"error"`) {
		t.Errorf("SuppressErrorFrame must not write error frame: %s", sb.String())
	}
	if st.HasOutput() {
		t.Error("empty stream must not report output (否则会被记成成功并钉死粘性)")
	}
}

// 流式：有内容但无 done → 截断错误，且客户端已收到内容。
func TestStreamTruncatedIsError(t *testing.T) {
	sse := "event:output\ndata:{\"response\":\"半句\"}\n\n"
	var sb strings.Builder
	err, st := runStream(&sbWriter{b: &sb}, sse, StreamOptions{SuppressErrorFrame: true})
	if !errors.Is(err, ErrTruncatedStream) {
		t.Fatalf("want ErrTruncatedStream, got %v", err)
	}
	if !st.HasOutput() {
		t.Error("truncated stream did deliver content, HasOutput must be true")
	}
}

// 正常流必须返回 nil，Stats().HasOutput() 为真。
func TestStreamNormalHasOutput(t *testing.T) {
	var sb strings.Builder
	err, st := runStream(&sbWriter{b: &sb}, soloSSEFixture, StreamOptions{Model: "glm-5.2"})
	if err != nil {
		t.Fatalf("normal stream should succeed: %v", err)
	}
	if !st.HasOutput() {
		t.Error("normal stream must report output")
	}
}

// runStream 用指定 writer 跑一遍流转换，返回 Run 的错误与计时快照。
func runStream(w http.ResponseWriter, sse string, opts StreamOptions) (error, StreamStats) {
	s := NewSSEWriter(w, opts.Model)
	s.StartHeartbeat(opts.Heartbeat)
	defer s.Close()
	err := s.Run(strings.NewReader(sse), opts)
	return err, s.Stats()
}
