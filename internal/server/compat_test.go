package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"trae2api-web/internal/auth"
)

// compatHandler 构造一个能跑通 chat 内核的 handler（fake 上游返回 soloSSE）。
func compatHandler(t *testing.T, alias map[string]string) *Handler {
	t.Helper()
	up := newFakeUpstream(t, func(authz string) (int, string, bool) { return 200, soloSSE, true })
	return NewHandler(Config{
		Pool:         testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999}),
		Upstream:     up,
		ModelAliases: alias,
	})
}

func doJSON(t *testing.T, h *Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	h.ServeHTTP(rec, req)
	return rec
}

// TestEmbeddingsReturns501 明确记录「501 而不是 404」这个决定：
// 404 会让调用方以为路径写错去改 base_url，浪费时间。
func TestEmbeddingsReturns501(t *testing.T) {
	h := compatHandler(t, nil)
	rec := doJSON(t, h, "POST", "/v1/embeddings", `{"model":"glm-5.2","input":"hi"}`)
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body)
	}
	var resp struct {
		Error struct {
			Code string `json:"code"`
			Type string `json:"type"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("not json: %v %s", err, rec.Body)
	}
	if resp.Error.Code != "not_implemented" || resp.Error.Type != "invalid_request_error" {
		t.Errorf("error=%+v", resp.Error)
	}
}

// TestLegacyCompletionsNonStream 校验协议翻译：text_completion 形状 +
// model 保真（回显客户端传的名字）+ usage 字段映射。
func TestLegacyCompletionsNonStream(t *testing.T) {
	h := compatHandler(t, nil)
	rec := doJSON(t, h, "POST", "/v1/completions",
		`{"model":"glm-5.2","prompt":"say hi","max_tokens":32}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body)
	}
	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("not json: %v", err)
	}
	if resp["object"] != "text_completion" {
		t.Errorf("object=%v", resp["object"])
	}
	if resp["model"] != "glm-5.2" {
		t.Errorf("model=%v", resp["model"])
	}
	if id, _ := resp["id"].(string); !strings.HasPrefix(id, "cmpl-") {
		t.Errorf("id=%v", resp["id"])
	}
	choices := resp["choices"].([]any)
	if len(choices) != 1 {
		t.Fatalf("choices=%v", choices)
	}
	c := choices[0].(map[string]any)
	if c["text"] != "你好" {
		t.Errorf("text=%q", c["text"])
	}
	if c["finish_reason"] != "stop" || c["index"] != float64(0) || c["logprobs"] != nil {
		t.Errorf("choice=%v", c)
	}
	u := resp["usage"].(map[string]any)
	if u["prompt_tokens"] != float64(5) || u["completion_tokens"] != float64(2) ||
		u["total_tokens"] != float64(7) {
		t.Errorf("usage=%v", u)
	}
}

// TestLegacyCompletionsModelAlias 客户端传别名时：
// 转发用目标模型，但响应 model 必须是客户端传的原始名。
func TestLegacyCompletionsModelAlias(t *testing.T) {
	h := compatHandler(t, map[string]string{"my-fast": "glm-5.2"})
	rec := doJSON(t, h, "POST", "/v1/completions", `{"model":"my-fast","prompt":"hi"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body)
	}
	var resp map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp["model"] != "my-fast" {
		t.Errorf("model 应回显客户端原始名, got %v", resp["model"])
	}
}

// TestLegacyCompletionsBatch 批量 prompt：一个请求 N 个 choice，index 递增，
// usage 是各次调用的合计。
func TestLegacyCompletionsBatch(t *testing.T) {
	h := compatHandler(t, nil)
	rec := doJSON(t, h, "POST", "/v1/completions",
		`{"model":"glm-5.2","prompt":["a","b","c"]}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body)
	}
	var resp map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	choices := resp["choices"].([]any)
	if len(choices) != 3 {
		t.Fatalf("choices=%d", len(choices))
	}
	for i, ch := range choices {
		c := ch.(map[string]any)
		if c["index"] != float64(i) {
			t.Errorf("index[%d]=%v", i, c["index"])
		}
		if c["text"] != "你好" {
			t.Errorf("text[%d]=%v", i, c["text"])
		}
	}
	u := resp["usage"].(map[string]any)
	if u["total_tokens"] != float64(21) {
		t.Errorf("usage 应合计 3 次 = 21, got %v", u["total_tokens"])
	}
}

// TestLegacyCompletionsBatchLimit 批量上限：单个 HTTP 请求不能轻易打满号池。
func TestLegacyCompletionsBatchLimit(t *testing.T) {
	h := compatHandler(t, nil)
	prompts := make([]string, maxLegacyPrompts+1)
	for i := range prompts {
		prompts[i] = `"p"`
	}
	rec := doJSON(t, h, "POST", "/v1/completions",
		`{"model":"glm-5.2","prompt":[`+strings.Join(prompts, ",")+`]}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body)
	}
	if !strings.Contains(rec.Body.String(), "at most 8 prompts") {
		t.Errorf("body=%s", rec.Body)
	}
}

// TestLegacyCompletionsTokenIDPromptRejected token id 必须明确 400：
// 我们没有分词器，当字符串转过去只会得到乱码，比报错更糟。
func TestLegacyCompletionsTokenIDPromptRejected(t *testing.T) {
	h := compatHandler(t, nil)
	rec := doJSON(t, h, "POST", "/v1/completions",
		`{"model":"glm-5.2","prompt":[1,2,3]}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body)
	}
	if !strings.Contains(rec.Body.String(), "token-id") {
		t.Errorf("body=%s", rec.Body)
	}
}

// TestLegacyCompletionsMissingPrompt 缺 prompt 要给清晰错误，而不是 500。
func TestLegacyCompletionsMissingPrompt(t *testing.T) {
	h := compatHandler(t, nil)
	rec := doJSON(t, h, "POST", "/v1/completions", `{"model":"glm-5.2"}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("code=%d", rec.Code)
	}
}

// TestLegacyCompletionsStream 流式翻译：帧必须是 text_completion 形状，
// 且以 [DONE] 收尾（否则客户端一直等）。
func TestLegacyCompletionsStream(t *testing.T) {
	h := compatHandler(t, nil)
	rec := doJSON(t, h, "POST", "/v1/completions",
		`{"model":"glm-5.2","prompt":"hi","stream":true}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Errorf("content-type=%q", ct)
	}
	body := rec.Body.String()
	if !strings.Contains(body, `"object":"text_completion"`) {
		t.Errorf("缺 text_completion 帧:\n%s", body)
	}
	if !strings.Contains(body, `"text":"你好"`) {
		t.Errorf("缺正文帧（思考不该混进 text）:\n%s", body)
	}
	if !strings.Contains(body, `"finish_reason":"stop"`) {
		t.Errorf("缺终止帧:\n%s", body)
	}
	if !strings.HasSuffix(strings.TrimSpace(body), "data: [DONE]") {
		t.Errorf("流未以 [DONE] 收尾:\n%s", body)
	}
}

// TestLegacyCompletionsStreamRejectsBatch 批量 + 流式明确拒绝：
// 只回第一个 prompt 的半成品流会让客户端静默丢结果。
func TestLegacyCompletionsStreamRejectsBatch(t *testing.T) {
	h := compatHandler(t, nil)
	rec := doJSON(t, h, "POST", "/v1/completions",
		`{"model":"glm-5.2","prompt":["a","b"],"stream":true}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("code=%d", rec.Code)
	}
}

// TestResponsesNonStream 校验 Responses 形状：output 数组 + output_text 便捷字段
// + usage 用 input/output 命名（不是 chat 的 prompt/completion）。
func TestResponsesNonStream(t *testing.T) {
	h := compatHandler(t, nil)
	rec := doJSON(t, h, "POST", "/v1/responses",
		`{"model":"glm-5.2","input":"hi","instructions":"be nice","max_output_tokens":64}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body)
	}
	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("not json: %v", err)
	}
	if resp["object"] != "response" || resp["status"] != "completed" {
		t.Errorf("object/status=%v/%v", resp["object"], resp["status"])
	}
	if resp["model"] != "glm-5.2" {
		t.Errorf("model=%v", resp["model"])
	}
	if id, _ := resp["id"].(string); !strings.HasPrefix(id, "resp_") {
		t.Errorf("id=%v", resp["id"])
	}
	if resp["output_text"] != "你好" {
		t.Errorf("output_text=%q", resp["output_text"])
	}
	output := resp["output"].([]any)
	// 上游给了思考 → 先 reasoning item，再 message item。
	if len(output) != 2 {
		t.Fatalf("output 长度=%d: %v", len(output), output)
	}
	if output[0].(map[string]any)["type"] != "reasoning" {
		t.Errorf("output[0]=%v", output[0])
	}
	msg := output[1].(map[string]any)
	if msg["type"] != "message" || msg["role"] != "assistant" || msg["status"] != "completed" {
		t.Errorf("message=%v", msg)
	}
	part := msg["content"].([]any)[0].(map[string]any)
	if part["type"] != "output_text" || part["text"] != "你好" {
		t.Errorf("content part=%v", part)
	}
	u := resp["usage"].(map[string]any)
	if u["input_tokens"] != float64(5) || u["output_tokens"] != float64(2) ||
		u["total_tokens"] != float64(7) {
		t.Errorf("usage=%v", u)
	}
	if _, ok := u["output_tokens_details"].(map[string]any); !ok {
		t.Errorf("缺 output_tokens_details: %v", u)
	}
}

// TestResponsesInputStream 数组形式的 input（含 input_text 分片）要能展开成
// 纯文本 messages，否则上游会因 content 是数组而 4xx。
func TestResponsesInputStream(t *testing.T) {
	h := compatHandler(t, nil)
	rec := doJSON(t, h, "POST", "/v1/responses", `{"model":"glm-5.2","input":[
		{"role":"user","content":[{"type":"input_text","text":"hi"}]}]}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body)
	}
	var resp map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp["output_text"] != "你好" {
		t.Errorf("output_text=%v", resp["output_text"])
	}
}

// TestResponsesRejectsNonTextPart 图片/文件等附件明确报错而不是静默丢弃。
func TestResponsesRejectsNonTextPart(t *testing.T) {
	h := compatHandler(t, nil)
	rec := doJSON(t, h, "POST", "/v1/responses", `{"model":"glm-5.2","input":[
		{"role":"user","content":[{"type":"input_image","image_url":"http://x/y.png"}]}]}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body)
	}
	if !strings.Contains(rec.Body.String(), "unsupported content type") {
		t.Errorf("body=%s", rec.Body)
	}
}

// TestResponsesMissingInput 空 input 明确 400。
func TestResponsesMissingInput(t *testing.T) {
	h := compatHandler(t, nil)
	rec := doJSON(t, h, "POST", "/v1/responses", `{"model":"glm-5.2"}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("code=%d", rec.Code)
	}
}

// TestResponsesStream 事件序列：必须有 created → ...delta... → completed，
// 且**不**发 [DONE]（Responses 规范不用它，发了部分 SDK 会报未知事件）。
func TestResponsesStream(t *testing.T) {
	h := compatHandler(t, nil)
	rec := doJSON(t, h, "POST", "/v1/responses",
		`{"model":"glm-5.2","input":"hi","stream":true}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body)
	}
	body := rec.Body.String()
	for _, want := range []string{
		"event: response.created",
		"event: response.in_progress",
		"event: response.output_text.delta",
		"event: response.output_text.done",
		"event: response.content_part.done",
		"event: response.output_item.done",
		"event: response.completed",
		`"output_text":"你好"`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("缺 %q\n---\n%s", want, body)
		}
	}
	if strings.Contains(body, "[DONE]") {
		t.Errorf("Responses 流不应发 [DONE]:\n%s", body)
	}
	// sequence_number 必须单调递增：SDK 靠它去重/排序。
	var last float64 = -1
	for _, line := range strings.Split(body, "\n") {
		if !strings.HasPrefix(line, "data: {") {
			continue
		}
		var ev map[string]any
		if json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &ev) != nil {
			continue
		}
		if n, ok := ev["sequence_number"].(float64); ok {
			if n <= last {
				t.Errorf("sequence_number 未递增: %v after %v", n, last)
			}
			last = n
		}
	}
}

// TestResponsesStreamReasoning event 顺序要求 reasoning item 先于 message
// （Responses 的 output 顺序即 item 编号顺序）。
func TestResponsesStreamReasoning(t *testing.T) {
	h := compatHandler(t, nil)
	rec := doJSON(t, h, "POST", "/v1/responses", `{"model":"glm-5.2","input":"hi","stream":true}`)
	body := rec.Body.String()
	rsAt := strings.Index(body, `"type":"reasoning"`)
	msgAt := strings.Index(body, `"type":"message"`)
	if rsAt < 0 || msgAt < 0 || rsAt > msgAt {
		t.Fatalf("reasoning 应先于 message (rs=%d msg=%d)\n%s", rsAt, msgAt, body)
	}
}

// TestAliasOneHopOnly 别名只跳一级：a→b 就返回 b，绝不会顺着 b→c 继续跳。
// 递归解析会让「别名链」变成隐式路由，排查事故时看不出请求到底去了哪个模型。
func TestAliasOneHopOnly(t *testing.T) {
	h := compatHandler(t, map[string]string{"a": "b", "b": "kimi-k3"})
	if got := h.resolveModelAlias("a"); got != "b" {
		t.Fatalf("a → %q, want b（不做传递）", got)
	}
}

// TestAliasCycleNoHang A→B→A 这种环必须立即返回，不能死循环。
func TestAliasCycleNoHang(t *testing.T) {
	h := compatHandler(t, map[string]string{"x1": "x2", "x2": "x1"})
	done := make(chan string, 1)
	go func() { done <- h.resolveModelAlias("x1") }()
	select {
	case got := <-done:
		if got != "x2" {
			t.Errorf("x1 → %q", got)
		}
	case <-timeAfterSecond():
		t.Fatal("别名解析疑似死循环")
	}
}

// TestAliasDoesNotHijackRealModel 真实静态模型名绝不能被别名改写：
// 否则「显式指定 glm-5.2」会被偷偷导到别的模型。
func TestAliasDoesNotHijackRealModel(t *testing.T) {
	h := compatHandler(t, map[string]string{"glm-5.2": "kimi-k3", "GLM-5.2": "kimi-k3"})
	if got := h.resolveModelAlias("glm-5.2"); got != "glm-5.2" {
		t.Errorf("静态模型被别名劫持: %q", got)
	}
}

// TestAliasCaseInsensitive 客户端可能把模型名规范化后再发（MY-GPT）。
func TestAliasCaseInsensitive(t *testing.T) {
	h := compatHandler(t, map[string]string{"my-gpt": "glm-5.2"})
	if got := h.resolveModelAlias("MY-GPT"); got != "glm-5.2" {
		t.Errorf("MY-GPT → %q", got)
	}
}

// TestAliasEnvFallback env 兜底：配置文件的读取在 cmd/server（本包无权改），
// env 让功能至少能立刻用起来。
func TestAliasEnvFallback(t *testing.T) {
	t.Setenv("TW2A_MODEL_ALIASES", " a = glm-5.2 , bad-pair , b=kimi-k3 , c=c")
	h := NewHandler(Config{})
	if got := h.cfg.ModelAliases["a"]; got != "glm-5.2" {
		t.Errorf("a=%q", got)
	}
	if got := h.cfg.ModelAliases["b"]; got != "kimi-k3" {
		t.Errorf("b=%q", got)
	}
	if _, ok := h.cfg.ModelAliases["c"]; ok {
		t.Error("自映射应被丢弃")
	}
}

// TestAliasAppliedInChat 端到端：chat 请求用别名也能命中静态模型并正常返回。
func TestAliasAppliedInChat(t *testing.T) {
	h := compatHandler(t, map[string]string{"my-gpt": "glm-5.2"})
	rec := doJSON(t, h, "POST", "/v1/chat/completions",
		`{"model":"my-gpt","messages":[{"role":"user","content":"hi"}]}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body)
	}
	var resp map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp["model"] != "my-gpt" {
		t.Errorf("响应 model 应回显客户端名, got %v", resp["model"])
	}
}

// TestAliasUnknownStillRejected 别名指向不存在的模型 → 仍然 400（不静默透传）。
func TestAliasUnknownStillRejected(t *testing.T) {
	h := compatHandler(t, map[string]string{"my-gpt": "no-such-model"})
	rec := doJSON(t, h, "POST", "/v1/chat/completions",
		`{"model":"my-gpt","messages":[{"role":"user","content":"hi"}]}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body)
	}
}

// TestCompatEndpointsRequireAuth 新端点必须走鉴权（除 /metrics 外）。
func TestCompatEndpointsRequireAuth(t *testing.T) {
	h := NewHandler(Config{APIKey: "k", Pool: testPoolWith()})
	for _, p := range []string{"/v1/completions", "/v1/responses", "/v1/embeddings"} {
		rec := doJSON(t, h, "POST", p, `{}`)
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("%s 应 401, got %d", p, rec.Code)
		}
	}
}

// timeAfterSecond 给「防死循环」用例一个 1s 上限。
func timeAfterSecond() <-chan time.Time { return time.After(time.Second) }

// TestModelListIncludesAliases 别名必须出现在 /v1/models：
// 大多数客户端只能从列表里选模型，不在列表里等于「配了也用不上」。
func TestModelListIncludesAliases(t *testing.T) {
	h := compatHandler(t, map[string]string{"my-gpt": "glm-5.2"})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/v1/models", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("code=%d", rec.Code)
	}
	var resp struct {
		Data []struct {
			ID      string `json:"id"`
			OwnedBy string `json:"owned_by"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, m := range resp.Data {
		if m.ID == "my-gpt" {
			found = true
			if m.OwnedBy != "alias" {
				t.Errorf("别名条目 owned_by=%q，应为 alias，便于与真实模型区分", m.OwnedBy)
			}
		}
	}
	if !found {
		t.Fatalf("/v1/models 未包含别名 my-gpt: %s", rec.Body)
	}
}

// TestAdminAliasesEndpoint 面板读写：GET 回显 + POST 校验 + 立即生效。
func TestAdminAliasesEndpoint(t *testing.T) {
	h := compatHandler(t, nil)
	// 没有 ConfigPath 时 persistAliases 只在内存生效（不报错），
	// 这正是纯 env 部署的期望行为。
	rec := doJSON(t, h, "POST", "/admin/api/aliases",
		`{"aliases":{"fast":"glm-5.2","my-gpt":"kimi-k3"}}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body)
	}
	if got := h.resolveModelAlias("fast"); got != "glm-5.2" {
		t.Errorf("保存后未立即生效: %q", got)
	}

	rec = doJSON(t, h, "GET", "/admin/api/aliases", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET code=%d", rec.Code)
	}
	var resp struct {
		Aliases map[string]string `json:"aliases"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Aliases["fast"] != "glm-5.2" || resp.Aliases["my-gpt"] != "kimi-k3" {
		t.Errorf("aliases=%v", resp.Aliases)
	}
}

// TestAdminAliasesRejectsSelfMapping 自映射（A→A）会形成无效条目，
// 直接 400 而不是默默存下去。
func TestAdminAliasesRejectsSelfMapping(t *testing.T) {
	h := compatHandler(t, nil)
	rec := doJSON(t, h, "POST", "/admin/api/aliases", `{"aliases":{"x":"X"}}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body)
	}
	rec = doJSON(t, h, "POST", "/admin/api/aliases", `{"aliases":{"x":""}}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("空目标应 400, code=%d", rec.Code)
	}
}

// TestAdminAliasesHotReloadNoRace 面板热更新与并发读别名不能有竞态
// （-race 下跑；别名在 mapModel 上，每个 chat 请求都会读）。
func TestAdminAliasesHotReloadNoRace(t *testing.T) {
	h := compatHandler(t, map[string]string{"a": "glm-5.2"})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 200; i++ {
			h.setAliases(map[string]string{"a": "glm-5.2"})
		}
	}()
	for i := 0; i < 200; i++ {
		if got := h.resolveModelAlias("a"); got != "glm-5.2" {
			t.Errorf("并发读得到 %q", got)
			break
		}
	}
	<-done
}

// TestAliasFromConfigFile config.json 的 model_aliases 在不改 cmd 层的前提下
// 也要生效（cmd/server 的 Config 结构体没有该字段，本包自己读文件）。
func TestAliasFromConfigFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, []byte(`{"model_aliases":{"from-file":"glm-5.2"},"listen":":8000"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	h := NewHandler(Config{ConfigPath: path})
	if got := h.resolveModelAlias("from-file"); got != "glm-5.2" {
		t.Fatalf("from-file → %q", got)
	}
	// 面板保存必须保留 config.json 的其它字段（不能整文件覆盖）。
	if err := h.persistAliases(map[string]string{"new-one": "kimi-k3"}); err != nil {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(path)
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("写回后不是合法 JSON: %v", err)
	}
	if doc["listen"] != ":8000" {
		t.Errorf("其它字段被丢失: %v", doc)
	}
	aliases, _ := doc["model_aliases"].(map[string]any)
	if aliases["new-one"] != "kimi-k3" {
		t.Errorf("别名未写入: %v", doc)
	}
	if _, err := os.Stat(path + ".bak"); err != nil {
		t.Errorf("应留下 .bak 备份: %v", err)
	}
}
