package upstream

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestPrepareBodyForcesStreamAndFunction(t *testing.T) {
	out := PrepareBody([]byte(`{"model":"glm-5.2","messages":[{"role":"user","content":"hi"}]}`))
	var m map[string]any
	json.Unmarshal(out, &m)
	if m["stream"] != true {
		t.Errorf("stream=%v", m["stream"])
	}
	if m["function"] != "solo_work_lite" {
		t.Errorf("function=%v", m["function"])
	}
	if m["config_name"] != "glm-5.2" || m["model"] != "glm-5.2" {
		t.Errorf("model fields=%v / %v", m["config_name"], m["model"])
	}
	msgs := m["messages"].([]any)
	first := msgs[0].(map[string]any)
	content := first["content"].([]any)
	if content[0].(map[string]any)["type"] != "text" || content[0].(map[string]any)["text"] != "hi" {
		t.Errorf("content rewrite=%v", content)
	}
}

func TestPrepareBodyKeepsArrayContent(t *testing.T) {
	out := PrepareBody([]byte(`{"model":"glm-5.2","messages":[{"role":"user","content":[{"type":"text","text":"hi"}]}]}`))
	var m map[string]any
	json.Unmarshal(out, &m)
	msgs := m["messages"].([]any)
	content := msgs[0].(map[string]any)["content"].([]any)
	if len(content) != 1 {
		t.Errorf("array content should pass through: %v", content)
	}
}

func TestPrepareBodyToolChoiceFunctionObject(t *testing.T) {
	out := PrepareBody([]byte(`{"model":"glm-5.2","tool_choice":{"type":"function","function":{"name":"get_weather"}},"tools":[{"type":"function","function":{"name":"get_weather"}}]}`))
	var m map[string]any
	json.Unmarshal(out, &m)
	if m["tool_choice"] != "get_weather" {
		t.Errorf("tool_choice=%v", m["tool_choice"])
	}
	if _, ok := m["tools"]; !ok {
		t.Error("tools should be kept for function choice")
	}
}

func TestPrepareBodyToolChoiceNone(t *testing.T) {
	out := PrepareBody([]byte(`{"model":"glm-5.2","tool_choice":"none","tools":[{}],"functions":[{}]}`))
	var m map[string]any
	json.Unmarshal(out, &m)
	if _, ok := m["tool_choice"]; ok {
		t.Error("tool_choice should be deleted")
	}
	if _, ok := m["tools"]; ok {
		t.Error("tools should be deleted")
	}
	if _, ok := m["functions"]; ok {
		t.Error("functions should be deleted")
	}
}

func TestPrepareBodyInvalidJSON(t *testing.T) {
	in := []byte(`{broken`)
	out := PrepareBody(in)
	if string(out) != string(in) {
		t.Error("invalid json should pass through unchanged")
	}
}

// 捕获的 SOLO llm_utils_chat SSE 样例（真实结构，token 无）。
const soloSSEFixture = "id:1\nevent:metadata\ndata:{\"model\":\"\",\"session_id\":\"897f0f3f-935a-4f42-a0fc-60f5140ccd02\",\"prompt_completion_id\":0}\n\n" +
	"id:2\nevent:timing_cost\ndata:{\"name\":\"llm_raw_chat_v2\",\"preprocess_timing\":71}\n\n" +
	"event:output\ndata:{\"response\":\"中国\",\"reasoning_content\":\"让我想想\",\"tool_calls\":null}\n\n" +
	"event:output\ndata:{\"response\":\"的首都是北京。\",\"reasoning_content\":\"\",\"tool_calls\":null}\n\n" +
	"event:extra_info\ndata:{\"reasoning_content\":\"让我想想\"}\n\n" +
	"event:token_usage\ndata:{\"prompt_tokens\":21,\"completion_tokens\":142,\"total_tokens\":163,\"reasoning_tokens\":135}\n\n" +
	"event:done\ndata:{\"finish_reason\":\"stop\"}\n\n"

func TestAggregateSOLO(t *testing.T) {
	resp, err := Aggregate(strings.NewReader(soloSSEFixture))
	if err != nil {
		t.Fatal(err)
	}
	if resp["object"] != "chat.completion" {
		t.Errorf("object=%v", resp["object"])
	}
	choices := resp["choices"].([]any)
	msg := choices[0].(map[string]any)["message"].(map[string]any)
	if msg["content"] != "中国的首都是北京。" {
		t.Errorf("content=%q", msg["content"])
	}
	if msg["reasoning_content"] != "让我想想" {
		t.Errorf("reasoning=%q", msg["reasoning_content"])
	}
	if choices[0].(map[string]any)["finish_reason"] != "stop" {
		t.Errorf("finish_reason=%v", choices[0].(map[string]any)["finish_reason"])
	}
	usage := resp["usage"].(map[string]any)
	if n, ok := usage["total_tokens"].(int64); !ok || n != 163 {
		t.Errorf("total_tokens=%v (%T)", usage["total_tokens"], usage["total_tokens"])
	}
	if n, ok := usage["prompt_tokens"].(int64); !ok || n != 21 {
		t.Errorf("prompt_tokens=%v", usage["prompt_tokens"])
	}
	if n, ok := usage["completion_tokens"].(int64); !ok || n != 142 {
		t.Errorf("completion_tokens=%v", usage["completion_tokens"])
	}
	// OpenAI 标准：思考 token 走 completion_tokens_details.reasoning_tokens
	details, ok := usage["completion_tokens_details"].(map[string]any)
	if !ok {
		t.Fatalf("missing completion_tokens_details: %v", usage)
	}
	if n, ok := details["reasoning_tokens"].(int64); !ok || n != 135 {
		t.Errorf("reasoning_tokens=%v", details["reasoning_tokens"])
	}
	// 上游的内部计数器绝不能透给客户端
	for _, leaked := range []string{"reasoning_tokens_total", "prompt_tokens_total", "cluster", "name"} {
		if _, bad := usage[leaked]; bad {
			t.Errorf("leaked upstream field %q into usage: %v", leaked, usage)
		}
	}
}

// usage 必须严格只有 OpenAI 定义的键，多一个字段严格 SDK 就会报错。
func TestAggregateUsageKeysAreOpenAIStandard(t *testing.T) {
	resp, err := Aggregate(strings.NewReader(soloSSEFixture))
	if err != nil {
		t.Fatal(err)
	}
	usage := resp["usage"].(map[string]any)
	allowed := map[string]bool{
		"prompt_tokens": true, "completion_tokens": true, "total_tokens": true,
		"prompt_tokens_details": true, "completion_tokens_details": true,
	}
	for k := range usage {
		if !allowed[k] {
			t.Errorf("unexpected usage key %q", k)
		}
	}
}

func TestAggregateSOLOError(t *testing.T) {
	raw := "event:error\ndata:{\"code\":4001,\"message\":\"We're sorry, the param is invalid.\",\"extra\":null}\n\n" +
		"event:done\ndata:{\"finish_reason\":\"stop\"}\n\n"
	_, err := Aggregate(strings.NewReader(raw))
	if err == nil {
		t.Fatal("want error from event:error")
	}
}

func TestParseSOLOLine(t *testing.T) {
	ev, err := ParseSOLOLine("output", `{"response":"hi","reasoning_content":"think","tool_calls":null}`)
	if err != nil {
		t.Fatal(err)
	}
	if ev.Response != "hi" || ev.Reasoning != "think" {
		t.Errorf("ev=%+v", ev)
	}
	if string(ev.ToolCalls) != "null" {
		t.Errorf("tool_calls=%s", ev.ToolCalls)
	}
	ev, err = ParseSOLOLine("done", `{"finish_reason":"stop"}`)
	if err != nil || ev.FinishReason != "stop" {
		t.Errorf("done: %+v %v", ev, err)
	}
}

func TestAggregateSOLOToolCalls(t *testing.T) {
	raw := "event:output\ndata:{\"response\":\"\",\"reasoning_content\":\"\",\"tool_calls\":[{\"id\":\"call_a\",\"type\":\"function\",\"function\":{\"name\":\"get_weather\",\"arguments\":\"{\\\"city\\\":\\\"北京\\\"}\"},\"index\":0}]}\n\n" +
		"event:done\ndata:{\"finish_reason\":\"tool_calls\"}\n\n"
	resp, err := Aggregate(strings.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	choice := resp["choices"].([]any)[0].(map[string]any)
	if choice["finish_reason"] != "tool_calls" {
		t.Errorf("finish_reason=%v", choice["finish_reason"])
	}
	msg := choice["message"].(map[string]any)
	calls, ok := msg["tool_calls"].([]map[string]any)
	if !ok || len(calls) != 1 {
		t.Fatalf("tool_calls=%#v", msg["tool_calls"])
	}
	if calls[0]["id"] != "call_a" {
		t.Errorf("call id=%v", calls[0]["id"])
	}
	fn := calls[0]["function"].(map[string]any)
	if fn["name"] != "get_weather" || fn["arguments"] != `{"city":"北京"}` {
		t.Errorf("fn=%v", fn)
	}
}

func TestStreamConvertsToOpenAIChunks(t *testing.T) {
	rec := httptest.NewRecorder()
	err := Stream(rec, strings.NewReader(soloSSEFixture))
	if err != nil {
		t.Fatal(err)
	}
	body := rec.Body.String()
	if !strings.Contains(body, `"object":"chat.completion.chunk"`) {
		t.Errorf("missing chunk object: %q", body)
	}
	if !strings.Contains(body, `"content":"中国"`) {
		t.Errorf("missing content delta: %q", body)
	}
	if !strings.Contains(body, `"reasoning_content"`) {
		t.Errorf("missing reasoning delta: %q", body)
	}
	if !strings.Contains(body, "data: [DONE]") {
		t.Errorf("missing [DONE]: %q", body)
	}
	ct := rec.Header().Get("Content-Type")
	if !strings.Contains(ct, "text/event-stream") {
		t.Errorf("content-type=%q", ct)
	}
}

func TestStreamGuaranteesDone(t *testing.T) {
	rec := httptest.NewRecorder()
	// 上游中断（只有 output，无 done）：客户端仍要收到 [DONE] 才能结束解析，
	// 但函数必须把「截断」告诉调用方，否则半句话会被当成成功回复。
	err := Stream(rec, strings.NewReader("event:output\ndata:{\"response\":\"x\",\"reasoning_content\":\"\",\"tool_calls\":null}\n\n"))
	if !errors.Is(err, ErrTruncatedStream) {
		t.Fatalf("want ErrTruncatedStream, got %v", err)
	}
	if !strings.Contains(rec.Body.String(), "data: [DONE]") {
		t.Errorf("missing [DONE]: %q", rec.Body.String())
	}
}

func TestPrepareBodyToolsParametersStringified(t *testing.T) {
	src := `{"model":"glm-5.2","messages":[{"role":"user","content":"hi"}],"tools":[{"type":"function","function":{"name":"get_weather","description":"weather","parameters":{"type":"object","properties":{"city":{"type":"string"}},"required":["city"]}}}]}`
	out := PrepareBody([]byte(src))
	var obj map[string]any
	if err := json.Unmarshal(out, &obj); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	tools, ok := obj["tools"].([]any)
	if !ok || len(tools) == 0 {
		t.Fatalf("tools missing: %#v", obj["tools"])
	}
	tool, _ := tools[0].(map[string]any)
	fn, _ := tool["function"].(map[string]any)
	params, ok := fn["parameters"].(string)
	if !ok {
		t.Fatalf("parameters should be string, got %T", fn["parameters"])
	}
	if !strings.Contains(params, `"city"`) {
		t.Fatalf("parameters content missing: %s", params)
	}
}

func TestPrepareBodyToolsInvalidEntriesDropped(t *testing.T) {
	src := `{"model":"glm-5.2","messages":[{"role":"user","content":"hi"}],"tools":[{"type":"function","function":{"name":"ok","parameters":{"type":"object"}}},{"bad":1}]}`
	out := PrepareBody([]byte(src))
	var obj map[string]any
	_ = json.Unmarshal(out, &obj)
	tools, ok := obj["tools"].([]any)
	if !ok {
		t.Fatalf("tools should exist")
	}
	if len(tools) != 1 {
		t.Fatalf("invalid entry should be dropped, got %d", len(tools))
	}
}

func TestMergeToolCallSOLOFunctionCall(t *testing.T) {
	// 模拟 SOLO 上游的 function_call 格式(流式两段: 先 name,再 arguments)
	toolCalls := map[int]map[string]any{}
	var order []int
	m1 := json.RawMessage(`[{"index":0,"id":"call_x","type":"function","function_call":{"name":"get_weather","arguments":"{\"city\":\"北京\""}}]`)
	m2 := json.RawMessage(`[{"index":0,"id":"","type":"function","function_call":{"name":"","arguments":"}"}}]`)
	mergeToolCallJSON(toolCalls, &order, m1)
	mergeToolCallJSON(toolCalls, &order, m2)
	if len(order) != 1 || order[0] != 0 {
		t.Fatalf("order=%v", order)
	}
	m := toolCalls[0]
	if m["id"] != "call_x" || m["type"] != "function" {
		t.Fatalf("id/type: %#v", m)
	}
	fn, ok := m["function"].(map[string]any)
	if !ok {
		t.Fatalf("function missing: %#v", m)
	}
	if fn["name"] != "get_weather" {
		t.Fatalf("name=%v", fn["name"])
	}
	args, _ := fn["arguments"].(string)
	if args != `{"city":"北京"}` {
		t.Fatalf("arguments=%q", args)
	}
}

func TestMergeToolCallStripsSOLOFields(t *testing.T) {
	toolCalls := map[int]map[string]any{}
	var order []int
	m := json.RawMessage(`[{"index":0,"id":"call_y","type":"function","function_call":{"name":"skill_view","arguments":"{\"name\":\"x\"}","namespace":"trae","partial_arguments":null}}]`)
	mergeToolCallJSON(toolCalls, &order, m)
	fn, _ := toolCalls[0]["function"].(map[string]any)
	if _, has := fn["namespace"]; has {
		t.Error("namespace should be stripped")
	}
	if _, has := fn["partial_arguments"]; has {
		t.Error("partial_arguments should be stripped")
	}
	if fn["name"] != "skill_view" {
		t.Errorf("name=%v", fn["name"])
	}
}

func TestPrepareBodyAssistantToolCallsToFunctionCall(t *testing.T) {
	src := `{"model":"glm-5.2","messages":[
	  {"role":"user","content":"hi"},
	  {"role":"assistant","content":null,"tool_calls":[{"id":"call_x","type":"function","function":{"name":"skill_view","arguments":"{\"name\":\"hermes-agent\"}"}}]},
	  {"role":"tool","tool_call_id":"call_x","content":"skill content"}
	],"tools":[{"type":"function","function":{"name":"skill_view"}}]}`
	out := PrepareBody([]byte(src))
	var obj map[string]any
	_ = json.Unmarshal(out, &obj)
	msgs := obj["messages"].([]any)
	assistant := msgs[1].(map[string]any)
	tcs := assistant["tool_calls"].([]any)
	tc := tcs[0].(map[string]any)
	if _, has := tc["function"]; has {
		t.Error("function should be converted to function_call")
	}
	fc, ok := tc["function_call"].(map[string]any)
	if !ok {
		t.Fatalf("function_call missing: %#v", tc)
	}
	if fc["name"] != "skill_view" {
		t.Errorf("function_call.name=%v", fc["name"])
	}
}

func TestPrepareBodyToolCallWithoutNameDropped(t *testing.T) {
	src := `{"model":"glm-5.2","messages":[
	  {"role":"user","content":"hi"},
	  {"role":"assistant","tool_calls":[{"id":"call_bad","type":"function","function":{"arguments":"{}"}}]}
	]}`
	out := PrepareBody([]byte(src))
	var obj map[string]any
	_ = json.Unmarshal(out, &obj)
	msgs := obj["messages"].([]any)
	assistant := msgs[1].(map[string]any)
	if _, has := assistant["tool_calls"]; has {
		t.Error("tool_call without name should be dropped")
	}
}

// 流式必须首帧带 role=assistant（部分客户端依赖它开气泡）。
func TestStreamFirstChunkHasRole(t *testing.T) {
	rec := httptest.NewRecorder()
	if err := Stream(rec, strings.NewReader(soloSSEFixture)); err != nil {
		t.Fatal(err)
	}
	first := strings.SplitN(strings.TrimSpace(rec.Body.String()), "\n", 2)[0]
	if !strings.Contains(first, `"role":"assistant"`) {
		t.Errorf("first chunk should carry role=assistant: %q", first)
	}
}

// 整个流共用同一个 id 与 created；model 用调用方传入的值。
func TestStreamStableIDCreatedAndModel(t *testing.T) {
	rec := httptest.NewRecorder()
	if err := StreamWithOptions(rec, strings.NewReader(soloSSEFixture), StreamOptions{Model: "glm-5.2"}); err != nil {
		t.Fatal(err)
	}
	body := rec.Body.String()
	ids := map[string]bool{}
	createds := map[string]bool{}
	models := map[string]bool{}
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "data: {") {
			continue
		}
		var m map[string]any
		if json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &m) != nil {
			continue
		}
		if v, ok := m["id"].(string); ok {
			ids[v] = true
		}
		if v, ok := m["created"].(float64); ok {
			createds[fmt.Sprint(v)] = true
		}
		if v, ok := m["model"].(string); ok {
			models[v] = true
		}
	}
	if len(ids) != 1 {
		t.Errorf("id must be stable across chunks, got %d distinct: %v", len(ids), ids)
	}
	if len(createds) != 1 {
		t.Errorf("created must be stable across chunks, got %v", createds)
	}
	if len(models) != 1 || !models["glm-5.2"] {
		t.Errorf("model should be the requested one, got %v", models)
	}
}

// 上游发累计快照时不能重复拼正文（历史上会导致内容翻倍）。
func TestStreamHandlesCumulativeSnapshots(t *testing.T) {
	sse := "event:output\ndata:{\"response\":\"你好\",\"reasoning_content\":\"\",\"tool_calls\":null}\n\n" +
		"event:output\ndata:{\"response\":\"你好世界\",\"reasoning_content\":\"\",\"tool_calls\":null}\n\n" +
		"event:output\ndata:{\"response\":\"你好世界！\",\"reasoning_content\":\"\",\"tool_calls\":null}\n\n" +
		"event:done\ndata:{\"finish_reason\":\"stop\"}\n\n"
	rec := httptest.NewRecorder()
	if err := Stream(rec, strings.NewReader(sse)); err != nil {
		t.Fatal(err)
	}
	var got strings.Builder
	for _, line := range strings.Split(rec.Body.String(), "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "data: {") {
			continue
		}
		var m map[string]any
		if json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &m) != nil {
			continue
		}
		ch := m["choices"].([]any)[0].(map[string]any)
		if c, ok := ch["delta"].(map[string]any)["content"].(string); ok {
			got.WriteString(c)
		}
	}
	if got.String() != "你好世界！" {
		t.Errorf("cumulative snapshot duplicated content: %q", got.String())
	}
}

// 聚合路径同样要吃掉累计快照，且 model 取上游回报值。
func TestAggregateHandlesSnapshotsAndModel(t *testing.T) {
	sse := "event:metadata\ndata:{\"model\":\"glm-5.2\",\"session_id\":\"x\"}\n\n" +
		"event:output\ndata:{\"response\":\"你好\",\"reasoning_content\":\"\"}\n\n" +
		"event:output\ndata:{\"response\":\"你好世界\",\"reasoning_content\":\"\"}\n\n" +
		"event:done\ndata:{\"finish_reason\":\"stop\"}\n\n"
	resp, err := Aggregate(strings.NewReader(sse))
	if err != nil {
		t.Fatal(err)
	}
	if resp["model"] != "glm-5.2" {
		t.Errorf("model=%v", resp["model"])
	}
	msg := resp["choices"].([]any)[0].(map[string]any)["message"].(map[string]any)
	if msg["content"] != "你好世界" {
		t.Errorf("content=%q (snapshot duplicated)", msg["content"])
	}
}

// 上游心跳/排队事件要转成 SSE 注释保活，而不是被静默丢弃。
func TestStreamForwardsHeartbeatAsComment(t *testing.T) {
	sse := "event:heartbeat\ndata:{}\n\n" +
		"event:progress_notice\ndata:{\"text\":\"排队中\"}\n\n" +
		"event:output\ndata:{\"response\":\"hi\",\"reasoning_content\":\"\"}\n\n" +
		"event:done\ndata:{\"finish_reason\":\"stop\"}\n\n"
	rec := httptest.NewRecorder()
	if err := Stream(rec, strings.NewReader(sse)); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(rec.Body.String(), ": keepalive") {
		t.Errorf("heartbeat not forwarded: %q", rec.Body.String())
	}
}

// 上游静默时也必须持续吐保活注释（否则客户端/代理会判定卡死）。
func TestStreamHeartbeatDuringSilence(t *testing.T) {
	pr, pw := io.Pipe()
	rec := httptest.NewRecorder()
	errc := make(chan error, 1)
	go func() {
		errc <- StreamWithOptions(rec, pr, StreamOptions{Heartbeat: 20 * time.Millisecond})
	}()
	// 上游先静默 150ms，再发正文与 done。
	time.Sleep(150 * time.Millisecond)
	io.WriteString(pw, "event:output\ndata:{\"response\":\"hi\",\"reasoning_content\":\"\"}\n\n")
	io.WriteString(pw, "event:done\ndata:{\"finish_reason\":\"stop\"}\n\n")
	pw.Close()
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
	body := rec.Body.String()
	n := strings.Count(body, ": keepalive")
	if n < 3 {
		t.Errorf("expected >=3 keepalives during 150ms silence, got %d", n)
	}
	if !strings.Contains(body, "data: [DONE]") {
		t.Errorf("missing DONE")
	}
}

// plan_item 的思考面要转成 reasoning_content 增量。
func TestStreamPlanItemReasoning(t *testing.T) {
	sse := "event:plan_item\ndata:{\"thought\":\"先想一想\"}\n\n" +
		"event:done\ndata:{\"finish_reason\":\"stop\"}\n\n"
	rec := httptest.NewRecorder()
	if err := Stream(rec, strings.NewReader(sse)); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(rec.Body.String(), `"reasoning_content":"先想一想"`) {
		t.Errorf("plan_item reasoning missing: %q", rec.Body.String())
	}
}

// ThinkingContent 模式：思考要当正文增量下发，并用 think 标签包裹。
func TestStreamThinkingInContent(t *testing.T) {
	sse := "event:output\ndata:{\"response\":\"\",\"reasoning_content\":\"想一\"}\n\n" +
		"event:output\ndata:{\"response\":\"\",\"reasoning_content\":\"想\"}\n\n" +
		"event:output\ndata:{\"response\":\"答案\",\"reasoning_content\":\"\"}\n\n" +
		"event:done\ndata:{\"finish_reason\":\"stop\"}\n\n"
	rec := httptest.NewRecorder()
	if err := StreamWithOptions(rec, strings.NewReader(sse), StreamOptions{Thinking: ThinkingContent}); err != nil {
		t.Fatal(err)
	}
	var got strings.Builder
	for _, line := range strings.Split(rec.Body.String(), "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "data: {") {
			continue
		}
		var m map[string]any
		if json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &m) != nil {
			continue
		}
		ch := m["choices"].([]any)[0].(map[string]any)
		if c, ok := ch["delta"].(map[string]any)["content"].(string); ok {
			got.WriteString(c)
		}
	}
	out := got.String()
	// 必须是完整的 <think>…</think>：缺了尖括号客户端不会折叠，思考会当正文显示。
	if !strings.HasPrefix(out, "<think>") {
		t.Errorf("thinking must open with <think>: %q", out)
	}
	if !strings.Contains(out, "</think>") {
		t.Errorf("thinking must be closed with </think>: %q", out)
	}
	if strings.Count(out, "<think>") != 1 || strings.Count(out, "</think>") != 1 {
		t.Errorf("think tag must appear exactly once: %q", out)
	}
	// 正文必须在 think 块之外
	if i, j := strings.Index(out, "</think>"), strings.Index(out, "答案"); i < 0 || j < i {
		t.Errorf("answer should come after </think>: %q", out)
	}
	if !strings.Contains(out, "想一想") {
		t.Errorf("thinking text lost: %q", out)
	}
}

// ThinkingContent 模式下若上游先给正文、后补思考，思考不能直接混进正文。
func TestStreamThinkingContentLateReasoning(t *testing.T) {
	sse := "event:output\ndata:{\"response\":\"答案\",\"reasoning_content\":\"\"}\n\n" +
		"event:output\ndata:{\"response\":\"\",\"reasoning_content\":\"迟到的思考\"}\n\n" +
		"event:done\ndata:{\"finish_reason\":\"stop\"}\n\n"
	rec := httptest.NewRecorder()
	if err := StreamWithOptions(rec, strings.NewReader(sse), StreamOptions{Thinking: ThinkingContent}); err != nil {
		t.Fatal(err)
	}
	var got strings.Builder
	for _, line := range strings.Split(rec.Body.String(), "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "data: {") {
			continue
		}
		var m map[string]any
		if json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &m) != nil {
			continue
		}
		ch := m["choices"].([]any)[0].(map[string]any)
		if c, ok := ch["delta"].(map[string]any)["content"].(string); ok {
			got.WriteString(c)
		}
	}
	out := got.String()
	// 迟到的思考要么被丢弃、要么合法地包进新的 think 块；绝不允裸文本混入正文。
	if strings.Contains(out, "迟到的思考") && !strings.Contains(out, "<think>\n\n迟到的思考") {
		t.Errorf("late reasoning leaked as raw text: %q", out)
	}
}

// ThinkingNone 模式：思考必须完全丢弃。
func TestStreamThinkingNoneDropsReasoning(t *testing.T) {
	sse := "event:output\ndata:{\"response\":\"\",\"reasoning_content\":\"秘密思考\"}\n\n" +
		"event:output\ndata:{\"response\":\"答案\",\"reasoning_content\":\"\"}\n\n" +
		"event:done\ndata:{\"finish_reason\":\"stop\"}\n\n"
	rec := httptest.NewRecorder()
	if err := StreamWithOptions(rec, strings.NewReader(sse), StreamOptions{Thinking: ThinkingNone}); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(rec.Body.String(), "秘密思考") {
		t.Errorf("ThinkingNone must drop reasoning: %q", rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "答案") {
		t.Errorf("answer missing")
	}
}

// 流式计时：首帧/首字/生成跨度/最大空档必须被采集，且 TPS 用 genMs 做分母。
func TestStreamTimingStats(t *testing.T) {
	sse := "event:output\ndata:{\"response\":\"\",\"reasoning_content\":\"想\"}\n\n" +
		"event:output\ndata:{\"response\":\"答\",\"reasoning_content\":\"\"}\n\n" +
		"event:done\ndata:{\"finish_reason\":\"stop\"}\n\n"
	rec := httptest.NewRecorder()
	w := NewSSEWriter(rec, "m")
	defer w.Close()
	if err := w.Run(strings.NewReader(sse), StreamOptions{Thinking: ThinkingReasoning}); err != nil {
		t.Fatal(err)
	}
	st := w.Stats()
	if !st.HasTiming {
		t.Fatal("no timing captured")
	}
	if st.Frames != 2 {
		t.Errorf("frames=%d want 2", st.Frames)
	}
	if st.ContentChars != 1 || st.ThinkChars != 1 {
		t.Errorf("chars: content=%d think=%d", st.ContentChars, st.ThinkChars)
	}
	if st.FirstFrameMs < 0 || st.FirstTextMs < 0 || st.GenMs < 0 {
		t.Errorf("negative timings: %+v", st)
	}
	// TPS 必须用 genMs；genMs=0 时不能返回 NaN/Inf
	if st.GenMs == 0 {
		if got := st.TPS(100); got != 0 {
			t.Errorf("TPS with zero genMs should be 0, got %v", got)
		}
	}
	if st.TPS(0) != 0 {
		t.Error("TPS with zero tokens should be 0")
	}
}

// ThinkingNone 丢弃思考时，思考字符不能算进吞吐（否则 TPS 虚高）。
func TestStreamTimingExcludesDroppedThinking(t *testing.T) {
	sse := "event:output\ndata:{\"response\":\"\",\"reasoning_content\":\"很长很长的思考\"}\n\n" +
		"event:output\ndata:{\"response\":\"答\",\"reasoning_content\":\"\"}\n\n" +
		"event:done\ndata:{\"finish_reason\":\"stop\"}\n\n"
	rec := httptest.NewRecorder()
	w := NewSSEWriter(rec, "m")
	defer w.Close()
	if err := w.Run(strings.NewReader(sse), StreamOptions{Thinking: ThinkingNone}); err != nil {
		t.Fatal(err)
	}
	st := w.Stats()
	if st.ThinkChars != 0 {
		t.Errorf("dropped thinking must not be counted: think=%d", st.ThinkChars)
	}
	if st.ContentChars != 1 { // rune 计数，不是字节数
		t.Errorf("content chars=%d want 1", st.ContentChars)
	}
}
