package upstream

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

// 上游内部计数器绝不能出现在 usage 里；字段名必须是 OpenAI 标准。
func TestNormalizeUsageStandardFields(t *testing.T) {
	raw := map[string]any{
		"prompt_tokens":                 float64(19),
		"completion_tokens":             float64(142),
		"total_tokens":                  float64(161),
		"prompt_tokens_total":           float64(0),
		"completion_tokens_total":       float64(0),
		"total_tokens_total":            float64(0),
		"reasoning_tokens":              float64(140),
		"reasoning_tokens_total":        float64(0),
		"cache_read_input_tokens":       float64(7),
		"cache_read_input_tokens_total": float64(0),
		"cache_creation_input_tokens":   float64(0),
		"cluster":                       "normal_context",
		"name":                          "",
	}
	u := normalizeUsage(raw)
	if u == nil {
		t.Fatal("nil usage")
	}
	allowed := map[string]bool{
		"prompt_tokens": true, "completion_tokens": true, "total_tokens": true,
		"prompt_tokens_details": true, "completion_tokens_details": true,
	}
	for k := range u {
		if !allowed[k] {
			t.Errorf("unexpected key %q in usage", k)
		}
	}
	if u["total_tokens"] != int64(161) {
		t.Errorf("total_tokens=%v", u["total_tokens"])
	}
	pd, _ := u["prompt_tokens_details"].(map[string]any)
	if pd["cached_tokens"] != int64(7) {
		t.Errorf("cached_tokens=%v", u["prompt_tokens_details"])
	}
	cd, _ := u["completion_tokens_details"].(map[string]any)
	if cd["reasoning_tokens"] != int64(140) {
		t.Errorf("reasoning_tokens=%v", u["completion_tokens_details"])
	}
}

// total 缺失或为 0 时按 prompt+completion 补算，避免下游统计总和为 0。
func TestNormalizeUsageFillsTotal(t *testing.T) {
	u := normalizeUsage(map[string]any{"prompt_tokens": float64(3), "completion_tokens": float64(4)})
	if u["total_tokens"] != int64(7) {
		t.Fatalf("total=%v", u["total_tokens"])
	}
}

// 完全没有用量字段时不下发 usage（不要吐一个全 0 的假对象）。
func TestNormalizeUsageEmpty(t *testing.T) {
	if u := normalizeUsage(nil); u != nil {
		t.Errorf("nil => %v", u)
	}
	if u := normalizeUsage(map[string]any{"cluster": "x"}); u != nil {
		t.Errorf("no token fields => %v", u)
	}
}

// 无 cached/reasoning 时不应出现空的 details 对象。
func TestNormalizeUsageOmitsEmptyDetails(t *testing.T) {
	u := normalizeUsage(map[string]any{"prompt_tokens": float64(1), "completion_tokens": float64(2)})
	if _, ok := u["prompt_tokens_details"]; ok {
		t.Errorf("empty details should be omitted: %v", u)
	}
	if _, ok := u["completion_tokens_details"]; ok {
		t.Errorf("empty details should be omitted: %v", u)
	}
}

// 默认不下发 usage chunk；显式 include_usage 才发，且必须是 choices:[]。
func TestStreamUsageChunkOnlyWhenRequested(t *testing.T) {
	sse := "event:output\ndata:{\"response\":\"hi\",\"reasoning_content\":\"\"}\n\n" +
		"event:token_usage\ndata:{\"prompt_tokens\":5,\"completion_tokens\":2,\"total_tokens\":7}\n\n" +
		"event:done\ndata:{\"finish_reason\":\"stop\"}\n\n"

	run := func(include bool) (string, []map[string]any) {
		var sb strings.Builder
		w := &sbWriter{b: &sb}
		if err := StreamWithOptions(w, strings.NewReader(sse), StreamOptions{IncludeUsage: include}); err != nil {
			t.Fatal(err)
		}
		var chunks []map[string]any
		for _, line := range strings.Split(sb.String(), "\n") {
			line = strings.TrimSpace(line)
			if !strings.HasPrefix(line, "data: {") {
				continue
			}
			var m map[string]any
			if json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &m) == nil {
				chunks = append(chunks, m)
			}
		}
		return sb.String(), chunks
	}

	// 未要求：不得出现 usage chunk
	body, chunks := run(false)
	if strings.Contains(body, `"usage"`) {
		t.Errorf("usage chunk must not be sent by default: %s", body)
	}
	var sawFinish bool
	for _, c := range chunks {
		if ch, ok := c["choices"].([]any); ok && len(ch) > 0 {
			if ch[0].(map[string]any)["finish_reason"] == "stop" {
				sawFinish = true
			}
		}
	}
	if !sawFinish {
		t.Error("finish_reason chunk missing")
	}

	// 要求了：必须发 usage chunk，choices 为空数组，且在 [DONE] 之前
	body, chunks = run(true)
	if !strings.Contains(body, `"usage"`) {
		t.Fatalf("usage chunk missing when requested: %s", body)
	}
	last := chunks[len(chunks)-1]
	ch, ok := last["choices"].([]any)
	if !ok || len(ch) != 0 {
		t.Errorf("usage chunk must have empty choices, got %v", last["choices"])
	}
	u, ok := last["usage"].(map[string]any)
	if !ok {
		t.Fatalf("usage=%v", last["usage"])
	}
	// 这里是从 SSE 文本重新解析出来的，数字是 float64。
	if v, ok := u["total_tokens"].(float64); !ok || v != 7 {
		t.Errorf("total_tokens=%v", u["total_tokens"])
	}
	if strings.Index(body, `"usage"`) > strings.Index(body, "data: [DONE]") {
		t.Error("usage chunk must precede [DONE]")
	}
}

// 流内错误必须是 OpenAI 的 data 帧 error 对象，而不是 event: error。
func TestStreamErrorFrameIsOpenAI(t *testing.T) {
	sse := "event:output\ndata:{\"response\":\"部分\",\"reasoning_content\":\"\"}\n\n" +
		"event:error\ndata:{\"code\":1005,\"message\":\"quota exceeded\"}\n\n"
	var sb strings.Builder
	w := &sbWriter{b: &sb}
	if err := StreamWithOptions(w, strings.NewReader(sse), StreamOptions{}); err != nil {
		t.Fatal(err)
	}
	body := sb.String()
	if strings.Contains(body, "event: error") {
		t.Errorf("should not use event: error (not OpenAI): %s", body)
	}
	var found bool
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "data: {") {
			continue
		}
		var m map[string]any
		if json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &m) != nil {
			continue
		}
		if e, ok := m["error"].(map[string]any); ok {
			found = true
			if e["message"] == "" || e["type"] == "" {
				t.Errorf("error object incomplete: %v", e)
			}
		}
	}
	if !found {
		t.Errorf("no error object in stream: %s", body)
	}
	if !strings.Contains(body, "data: [DONE]") {
		t.Error("stream must end with [DONE]")
	}
}

// sbWriter 测试用：把 SSE 写进 strings.Builder（不 flush、不报错）。
type sbWriter struct{ b *strings.Builder }

func (w *sbWriter) Header() http.Header         { return http.Header{} }
func (w *sbWriter) Write(p []byte) (int, error) { return w.b.Write(p) }
func (w *sbWriter) WriteHeader(statusCode int)  {}
