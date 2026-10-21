// solosse.go SOLO 自定义 SSE 解析 → OpenAI SSE（流式转换 + 非流式聚合）。
//
// SOLO 事件序列（SPEC §4.6，实测）：
//
//	id:1
//	event:metadata
//	data:{"model":"","session_id":"...","prompt_completion_id":0,...}
//
//	id:2
//	event:timing_cost
//	data:{"name":"llm_raw_chat_v2",...}
//
//	event:output                          ← ×N，核心内容
//	data:{"response":"<content 增量>",
//	      "reasoning_content":"<思考链增量>",
//	      "tool_calls":<null 或工具调用>}
//
//	event:extra_info                       ← 含 reasoning_content 完整版
//	event:token_usage
//	data:{"prompt_tokens":21,"completion_tokens":142,"total_tokens":163,"reasoning_tokens":135}
//
//	event:done
//	data:{"finish_reason":"stop"}
package upstream

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// SOLOEvent 单条 SOLO SSE 事件（归一化）。
type SOLOEvent struct {
	Event        string          // metadata | timing_cost | output | extra_info | plan_item | progress_notice | token_usage | done | error | heartbeat
	Response     string          // output/plan_item: content 增量
	Reasoning    string          // output/plan_item: 思考链增量
	ToolCalls    json.RawMessage // output: 工具调用（null 或对象/数组）
	Usage        map[string]any  // token_usage
	FinishReason string          // done
	ErrorCode    int64           // error
	ErrorMessage string          // error
	// ModelName 上游在部分事件里回报的真实模型名（metadata/extra_info.model）。
	// 用于把 chunk 的 model 字段填对，而不是留空字符串。
	ModelName string
}

// SOLOStreamError 上游 SSE 流内的业务错误（event:error）。非流式聚合时返回，
// 调用方可据此分类冷却账号并轮转。
type SOLOStreamError struct {
	Code int64
	Msg  string
}

func (e *SOLOStreamError) Error() string {
	return fmt.Sprintf("solo error code=%d msg=%s", e.Code, e.Msg)
}

// Kind 将 SSE 流内错误分类。默认 1005 → ErrPlanLimit，其余 → ErrClient；
// 可通过 SetStreamErrorClassifier 注入自定义规则（未注入时行为不变）。
func (e *SOLOStreamError) Kind() ErrKind {
	return classifyStreamError(e.Code, e.Msg)
}

// KindFor 按指定 client 的模型级规则判定流内错误类别。
//
// 为什么需要：`Kind()` 只有进程级钩子，无法感知「模型级规则表」。3003
// （all models failed，实测表示"该账号跑不了这个模型"）若按默认口径归为
// Client，会被当成账号级故障累计到熔断，把整个账号踢出选号池——其它模型
// 也跟着不可用。调用方拿到 client 时应优先用本方法。
func (e *SOLOStreamError) KindFor(c *Client) ErrKind {
	if k := c.streamErrKind(e.Code, e.Msg); k == ErrModelBlocked {
		return k
	}
	return e.Kind()
}

// ParseSOLOLine 解析一条事件（eventName 为 event 行值，dataLine 为 data 行值）。
func ParseSOLOLine(eventName, dataLine string) (*SOLOEvent, error) {
	ev := &SOLOEvent{Event: strings.TrimSpace(eventName)}
	if dataLine == "" {
		return ev, nil
	}
	var raw map[string]any
	if err := json.Unmarshal([]byte(dataLine), &raw); err != nil {
		return nil, err
	}
	if v, ok := raw["model"].(string); ok && v != "" {
		ev.ModelName = v
	}
	switch ev.Event {
	case "output":
		if v, ok := raw["response"].(string); ok {
			ev.Response = v
		}
		if v, ok := raw["reasoning_content"].(string); ok {
			ev.Reasoning = v
		}
		if tc, ok := raw["tool_calls"]; ok {
			ev.ToolCalls, _ = json.Marshal(tc)
		}
	case "plan_item":
		// 新版上游把「思考/计划」拆到 plan_item：thought / reasoning_content / content。
		// 只取思考面，正文由 output 事件给，避免 content 双写。
		if v, ok := raw["reasoning_content"].(string); ok {
			ev.Reasoning = v
		} else if v, ok := raw["thought"].(string); ok {
			ev.Reasoning = v
		}
		if v, ok := raw["tool_calls"]; ok {
			ev.ToolCalls, _ = json.Marshal(v)
		}
	case "heartbeat", "keepalive", "ping", "progress_notice", "request_wait_in_queue":
		// 心跳/队列提示：不产生正文，只用于下游保活（见 streamHooks）。
	case "token_usage":
		ev.Usage = raw
	case "done":
		if v, ok := raw["finish_reason"].(string); ok {
			ev.FinishReason = v
		}
	case "error":
		if v, ok := raw["code"].(float64); ok {
			ev.ErrorCode = int64(v)
		}
		if v, ok := raw["message"].(string); ok {
			ev.ErrorMessage = v
		}
	}
	return ev, nil
}

// sseState 维护一行 SSE 的 event/data 跨行累积。
type sseState struct {
	event string
	data  strings.Builder
}

// reset 清空状态（事件边界触发）。
func (s *sseState) reset() {
	s.event = ""
	s.data.Reset()
}

// scanLine 处理一行；返回该行触发的事件（事件边界时解析并返回）。
func scanLine(st *sseState, line string) *SOLOEvent {
	switch {
	case line == "":
		if st.event == "" {
			st.reset()
			return nil
		}
		ev, err := ParseSOLOLine(st.event, st.data.String())
		st.reset()
		if err != nil {
			return nil
		}
		return ev
	case strings.HasPrefix(line, "event:"):
		st.event = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
	case strings.HasPrefix(line, "data:"):
		st.data.WriteString(strings.TrimPrefix(line, "data:"))
	case strings.HasPrefix(line, ":"):
		// 注释行忽略
	}
	return nil
}

// Aggregate 读取完整 SOLO SSE，聚合 response + reasoning + tool_calls + usage，
// 产出单个 OpenAI chat.completion（非流式）。
func Aggregate(r io.Reader) (map[string]any, error) {
	br := bufio.NewReaderSize(r, 64*1024)
	var (
		id           string
		modelName    string
		content      textAcc
		reasoning    textAcc
		finishReason = "stop"
		usage        map[string]any
		toolCalls    = map[int]map[string]any{}
		toolOrder    []int
		upstreamErr  error
	)
	st := &sseState{}
	for {
		line, err := br.ReadString('\n')
		if err != nil && err != io.EOF {
			return nil, err
		}
		if ev := scanLine(st, strings.TrimRight(line, "\r\n")); ev != nil {
			if ev.ModelName != "" {
				modelName = ev.ModelName
			}
			switch ev.Event {
			case "metadata":
				if id == "" && ev.Usage != nil {
					// metadata 无 id 可用，保留为空，末尾补 chatcmpl。
				}
			case "output", "plan_item":
				// 与非流式一致：上游可能发增量也可能发累计快照，由 textAcc 统一判定。
				content.add(ev.Response)
				reasoning.add(ev.Reasoning)
				mergeToolCallJSON(toolCalls, &toolOrder, ev.ToolCalls)
			case "token_usage":
				usage = ev.Usage
			case "done":
				if ev.FinishReason != "" {
					finishReason = ev.FinishReason
				}
			case "error":
				upstreamErr = &SOLOStreamError{Code: ev.ErrorCode, Msg: ev.ErrorMessage}
			}
		}
		if err == io.EOF {
			break
		}
	}
	if upstreamErr != nil {
		return nil, upstreamErr
	}
	if id == "" {
		id = fmt.Sprintf("chatcmpl-%d", time.Now().UnixNano())
	}
	message := map[string]any{
		"role":    "assistant",
		"content": content.String(),
	}
	if reasoning.String() != "" {
		message["reasoning_content"] = reasoning.String()
	}
	if len(toolOrder) > 0 {
		sortInts(toolOrder)
		calls := make([]map[string]any, 0, len(toolOrder))
		for _, idx := range toolOrder {
			calls = append(calls, toolCalls[idx])
		}
		message["tool_calls"] = calls
	}
	resp := map[string]any{
		"id":      id,
		"object":  "chat.completion",
		"created": time.Now().Unix(),
		"model":   modelName,
		"choices": []any{
			map[string]any{
				"index":         0,
				"message":       message,
				"finish_reason": finishReason,
			},
		},
	}
	if usage != nil {
		resp["usage"] = usage
	}
	return resp, nil
}

// mergeToolCallJSON 把 SOLO output.tool_calls（json.RawMessage，可能 null/对象/数组）
// 合并进 toolCalls（按 index）。
func mergeToolCallJSON(toolCalls map[int]map[string]any, toolOrder *[]int, raw json.RawMessage) {
	if len(raw) == 0 || string(raw) == "null" {
		return
	}
	var arr []map[string]any
	if err := json.Unmarshal(raw, &arr); err != nil {
		var one map[string]any
		if json.Unmarshal(raw, &one) != nil {
			return
		}
		arr = []map[string]any{one}
	}
	for _, call := range arr {
		if call == nil {
			continue
		}
		idx := 0
		if v, ok := call["index"].(float64); ok {
			idx = int(v)
		}
		merged, seen := toolCalls[idx]
		if !seen {
			merged = map[string]any{"index": idx}
			toolCalls[idx] = merged
			*toolOrder = append(*toolOrder, idx)
		}
		mergeToolCallDelta(merged, call)
	}
}

// mergeToolCallDelta 把流式 tool_call 片段合并到累计对象：
// id/type/function.name 直覆盖，function.arguments 拼接。
// 上游 SOLO 用 `function_call` 字段（实测），OpenAI 标准用 `function`；两者都兼容。
func mergeToolCallDelta(merged, delta map[string]any) {
	if v, ok := delta["id"].(string); ok && v != "" {
		merged["id"] = v
	}
	if v, ok := delta["type"].(string); ok && v != "" {
		merged["type"] = v
	}
	df, _ := delta["function"].(map[string]any)
	if df == nil {
		df, _ = delta["function_call"].(map[string]any) // SOLO 专属字段名
	}
	if df == nil {
		return
	}
	// 清理 SOLO 专属字段,只保留标准 OpenAI function 结构(name/arguments)
	delete(df, "namespace")
	delete(df, "partial_arguments")
	mf, _ := merged["function"].(map[string]any)
	if mf == nil {
		mf = map[string]any{}
		merged["function"] = mf
	}
	if v, ok := df["name"].(string); ok && v != "" {
		mf["name"] = v
	}
	if v, ok := df["arguments"].(string); ok && v != "" {
		if prev, _ := mf["arguments"].(string); prev != "" {
			mf["arguments"] = prev + v
		} else {
			mf["arguments"] = v
		}
	}
}

// sortInts 升序排序（避免引 sort 包只为三行）。
func sortInts(a []int) {
	for i := 0; i < len(a)-1; i++ {
		for j := i + 1; j < len(a); j++ {
			if a[j] < a[i] {
				a[i], a[j] = a[j], a[i]
			}
		}
	}
}

// StreamOptions 流式输出的行为开关（零值 = 全部默认）。
type StreamOptions struct {
	// Model 写进每个 chunk 的 model 字段。上游 SOLO 的 metadata 里 model 常为空，
	// 留空会让客户端无法按模型统计/路由，所以由调用方传入请求的模型名。
	Model string
	// OnError 上游 event:error 回调（冷却账号/记流水）。
	OnError func(*SOLOStreamError)
	// OnUsage 收到 token_usage 时回调（记账）。
	OnUsage func(UsageDetail)
	// Heartbeat 空闲保活间隔。上游静默超过该时长就写一行 SSE 注释，
	// 让客户端/中间代理知道连接还活着。0 = 用 DefaultHeartbeat。
	Heartbeat time.Duration
	// Thinking 思考内容呈现方式（见 server 配置 stream.thinking）：
	//   ThinkingReasoning（默认）→ delta.reasoning_content
	//   ThinkingContent         → 包进 think 标签作为 delta.content
	//   ThinkingNone            → 丢弃
	Thinking ThinkingMode
}

// ThinkingMode 思考内容的呈现方式。
type ThinkingMode int

const (
	// ThinkingReasoning 走 delta.reasoning_content（DeepSeek 约定，主流客户端会渲染成思考过程）。
	ThinkingReasoning ThinkingMode = iota
	// ThinkingContent 把思考包进 think 标签后作为正文增量下发。
	//
	// 为什么需要这个模式：上游 SOLO 先思考几十秒才给正文（实测 glm-5.2 思考
	// 2000~9000 字、正文首个字符晚 14~90s）。只渲染 delta.content 的客户端
	// （Cline / Zed / 各类 CLI / 多数网页端）这段时间一片空白，用户观感就是
	// 「卡住了」；trae-cn-relay 之类的实现默认就把压缩后的思考当正文输出，
	// 所以看起来一直在出字。开启本模式即对齐该行为。
	ThinkingContent
	// ThinkingNone 丢弃思考，只下发正文。
	ThinkingNone
)

// 思考块标记（与主流前端约定一致，Cherry Studio / OpenWebUI 等会折叠显示）。
const (
	thinkOpenTag  = " thinking\n\n"
	thinkCloseTag = "\n\n<｜end▁of▁thinking｜>\n\n"
)

// DefaultHeartbeat 默认空闲保活间隔。
//
// 为什么必须有心跳：从「客户端发请求」到「上游吐出第一个字」之间，要经历
// 建连 + 上游排队 + 首包生成，实测可长达数秒甚至 10s+。这段时间若一个字节
// 都不写，客户端会以为连接卡死：部分 SDK 有首包空闲超时，中间代理
// （Nginx proxy_read_timeout、Cloudflare 等）会直接掐断连接。
const DefaultHeartbeat = 1 * time.Second

// SSEWriter 是 OpenAI SSE 输出的独占写入器。
//
// 存在的意义：流式请求要「先开流、再连上游」——先立刻把 200 + SSE 头 + role 帧
// 刷给客户端（约几毫秒），随后才去建上游连接。中间这段空隙由心跳协程持续写
// SSE 注释行保活。若直接把 http.ResponseWriter 交给解析循环，就必须等上游
// 握手完成才能写第一个字节，用户侧表现就是「点了没反应，过一两秒才突然开始」。
//
// 并发：所有写入都经 mu 串行化（心跳协程与解析循环会同时写）。
type SSEWriter struct {
	w  http.ResponseWriter
	fl http.Flusher
	mu sync.Mutex

	id        string
	created   int64
	model     string
	lastWrite time.Time
	closed    bool

	// 正文/思考累计器：识别上游的「累计快照」帧并只下发新增后缀。
	contentSeen   textAcc
	reasoningSeen textAcc

	// thinking 模式与「思考块是否已开」状态（ThinkingContent 模式下维护）。
	thinking    ThinkingMode
	thinkOpened bool
	thinkClosed bool

	hbDone chan struct{}
	hbWG   sync.WaitGroup
}

// NewSSEWriter 设置 SSE 响应头、立刻 flush，并写入首个 role 帧。
// 调用返回后客户端已经能看到一个已打开的空流。
func NewSSEWriter(w http.ResponseWriter, model string) *SSEWriter {
	h := w.Header()
	h.Set("Content-Type", "text/event-stream; charset=utf-8")
	h.Set("Cache-Control", "no-cache, no-transform")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no") // 关 Nginx 缓冲，否则整段回复会攒到最后一次吐出

	s := &SSEWriter{
		w:        w,
		fl:       asFlusher(w),
		id:       fmt.Sprintf("chatcmpl-%d", time.Now().UnixNano()),
		created:  time.Now().Unix(),
		model:    model,
		thinking: ThinkingReasoning,
	}
	// 立刻把响应头刷下去：客户端从「等首字节」变成「等第一个字符」。
	s.flushLocked()
	// 首帧角色：Cline / Zed / Codex 等客户端依赖它开始渲染消息气泡。
	s.writeChunk(map[string]any{"role": "assistant"}, "")
	return s
}

func asFlusher(w http.ResponseWriter) http.Flusher {
	if f, ok := w.(http.Flusher); ok {
		return f
	}
	return nil
}

// Model 返回写入 chunk 的模型名。
func (s *SSEWriter) Model() string { return s.model }

// StartHeartbeat 启动空闲保活协程：只要 hb 内没有写出任何字节，就补一行注释。
// 重复调用安全（第二次起直接忽略）。
func (s *SSEWriter) StartHeartbeat(hb time.Duration) {
	if s == nil {
		return
	}
	if hb <= 0 {
		hb = DefaultHeartbeat
	}
	s.mu.Lock()
	if s.hbDone != nil || s.closed {
		s.mu.Unlock()
		return
	}
	done := make(chan struct{})
	s.hbDone = done
	s.mu.Unlock()

	s.hbWG.Add(1)
	go func() {
		defer s.hbWG.Done()
		t := time.NewTicker(hb)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				s.mu.Lock()
				if !s.closed && time.Since(s.lastWrite) >= hb {
					s.writeRawLocked(": keepalive\n\n")
				}
				s.mu.Unlock()
			}
		}
	}()
}

// Close 停止心跳协程并等待其退出。可重复调用。
func (s *SSEWriter) Close() {
	if s == nil {
		return
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	if s.hbDone != nil {
		close(s.hbDone)
		s.hbDone = nil
	}
	s.mu.Unlock()
	s.hbWG.Wait()
}

// WriteError 在已开的流内报错（响应头已发 200，无法再改状态码）。
// 同时发一条 OpenAI 风格的 error 事件与 [DONE]，客户端不会一直挂着。
func (s *SSEWriter) WriteError(msg string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.writeRawLocked("event: error\ndata: " + jsonEscape(msg) + "\n\n")
	s.writeRawLocked("data: [DONE]\n\n")
}

func (s *SSEWriter) writeRawLocked(payload string) {
	if _, err := io.WriteString(s.w, payload); err != nil {
		return
	}
	s.lastWrite = time.Now()
	if s.fl != nil {
		s.fl.Flush()
	}
}

// flushLocked 只刷响应头（不写正文）。
func (s *SSEWriter) flushLocked() {
	s.lastWrite = time.Now()
	if s.fl != nil {
		s.fl.Flush()
	}
}

func (s *SSEWriter) writeChunk(delta map[string]any, finish string) {
	choice := map[string]any{"index": 0, "delta": delta}
	if finish != "" {
		choice["finish_reason"] = finish
	} else {
		choice["finish_reason"] = nil
	}
	chunk := map[string]any{
		"id":      s.id,
		"object":  "chat.completion.chunk",
		"created": s.created,
		"model":   s.model,
		"choices": []any{choice},
	}
	raw, _ := json.Marshal(chunk)
	s.writeRawLocked("data: " + string(raw) + "\n\n")
}

// WriteChunk 对外暴露的 chunk 写入（server 需要补发 usage 等场景）。
func (s *SSEWriter) WriteChunk(delta map[string]any, finish string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.writeChunk(delta, finish)
}

// Run 消费上游 SOLO SSE 并转成 OpenAI chunk。
// 与旧 Stream* 的区别：响应头与 role 帧已在 NewSSEWriter 写过，
// 因此上游迟迟不吐数据时，心跳仍能生效。
func (s *SSEWriter) Run(r io.Reader, opts StreamOptions) error {
	if s.model == "" {
		s.model = opts.Model
	}
	s.thinking = opts.Thinking
	br := bufio.NewReaderSize(r, 64*1024)
	var pendingUsage map[string]any
	sawDone := false
	st := &sseState{}

	type readResult struct {
		line string
		err  error
	}
	lines := make(chan readResult, 64)
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		for {
			line, err := br.ReadString('\n')
			select {
			case lines <- readResult{line, err}:
			case <-stop:
				return
			}
			if err != nil {
				return
			}
		}
	}()

	for {
		var rr readResult
		select {
		case rr = <-lines:
		case <-stop:
			return nil
		}
		line, err := rr.line, rr.err
		if err != nil && err != io.EOF {
			return err
		}
		if ev := scanLine(st, strings.TrimRight(line, "\r\n")); ev != nil {
			switch ev.Event {
			case "output", "plan_item":
				s.mu.Lock()
				out := s.contentSeen.add(ev.Response)
				think := s.reasoningSeen.add(ev.Reasoning)
				switch s.thinking {
				case ThinkingNone:
					// 丢弃思考，只发正文。
					if out != "" {
						s.writeChunk(map[string]any{"content": out}, "")
					}
				case ThinkingContent:
					// 思考当正文发：空档期立刻有字在动，观感就是「边想边说」。
					// 用 think 标签包起来，支持折叠的客户端仍会收成「思考过程」。
					var buf strings.Builder
					if think != "" {
						if !s.thinkOpened {
							buf.WriteString(thinkOpenTag)
							s.thinkOpened = true
						}
						buf.WriteString(think)
					}
					if out != "" {
						if s.thinkOpened && !s.thinkClosed {
							buf.WriteString(thinkCloseTag)
							s.thinkClosed = true
						}
						buf.WriteString(out)
					}
					if buf.Len() > 0 {
						s.writeChunk(map[string]any{"content": buf.String()}, "")
					}
				default: // ThinkingReasoning
					delta := map[string]any{}
					if out != "" {
						delta["content"] = out
					}
					if think != "" {
						delta["reasoning_content"] = think
					}
					if len(delta) > 0 {
						s.writeChunk(delta, "")
					}
				}
				s.mu.Unlock()
				if len(ev.ToolCalls) > 0 && string(ev.ToolCalls) != "null" {
					if tc, ok := normalizeStreamToolCalls(ev.ToolCalls); ok {
						s.WriteChunk(map[string]any{"tool_calls": tc}, "")
					}
				}
			case "token_usage":
				pendingUsage = ev.Usage
				if opts.OnUsage != nil {
					if d, ok := UsageDetailFromEvent(ev); ok {
						opts.OnUsage(d)
					}
				}
			case "heartbeat", "keepalive", "ping", "progress_notice", "request_wait_in_queue":
				// 上游心跳/排队提示：转成下游保活注释，别让客户端以为断流。
				s.mu.Lock()
				s.writeRawLocked(": keepalive\n\n")
				s.mu.Unlock()
			case "done":
				s.mu.Lock()
				if s.thinking == ThinkingContent && s.thinkOpened && !s.thinkClosed {
					s.writeChunk(map[string]any{"content": thinkCloseTag}, "")
					s.thinkClosed = true
				}
				if pendingUsage != nil {
					usageChunk := map[string]any{
						"id":      s.id,
						"object":  "chat.completion.chunk",
						"created": s.created,
						"model":   s.model,
						"choices": []any{map[string]any{
							"index": 0, "delta": map[string]any{}, "finish_reason": ev.FinishReason,
						}},
						"usage": pendingUsage,
					}
					raw, _ := json.Marshal(usageChunk)
					s.writeRawLocked("data: " + string(raw) + "\n\n")
					pendingUsage = nil
				} else {
					s.writeChunk(map[string]any{}, ev.FinishReason)
				}
				s.writeRawLocked("data: [DONE]\n\n")
				s.mu.Unlock()
				sawDone = true
			case "error":
				se := &SOLOStreamError{Code: ev.ErrorCode, Msg: ev.ErrorMessage}
				if opts.OnError != nil {
					opts.OnError(se)
				}
				s.WriteError(fmt.Sprintf("solo error code=%d msg=%s", ev.ErrorCode, ev.ErrorMessage))
				sawDone = true
			}
		}
		if err == io.EOF {
			break
		}
	}
	if !sawDone {
		// 幂等兜底：上游中断（无 done）仍写 [DONE]，避免客户端一直等。
		s.mu.Lock()
		s.writeRawLocked("data: [DONE]\n\n")
		s.mu.Unlock()
	}
	return nil
}

// Stream 流式转换：SOLO SSE → OpenAI SSE chunk，逐条 flush，保证至少一个 [DONE]。
// 调用方必须先设置过 status 200；本函数自设 SSE headers。
func Stream(w http.ResponseWriter, r io.Reader) error {
	return StreamWithOptions(w, r, StreamOptions{})
}

// StreamWithError 同 Stream，额外在遇到上游 event:error 时回调 onErr（非 nil），
// 供调用方冷却账号/记录日志；错误信息同时注入 SSE 事件流。
func StreamWithError(w http.ResponseWriter, r io.Reader, onErr func(*SOLOStreamError)) error {
	return StreamWithOptions(w, r, StreamOptions{OnError: onErr})
}

// StreamWithHooks 同 StreamWithError，额外在收到 token_usage 事件时回调 onUsage。
func StreamWithHooks(w http.ResponseWriter, r io.Reader, onErr func(*SOLOStreamError), onUsage func(UsageDetail)) error {
	return StreamWithOptions(w, r, StreamOptions{OnError: onErr, OnUsage: onUsage})
}

// StreamWithOptions 一步到位版本（自动开流 + 心跳 + 解析）。
// server 流式主路径用 NewSSEWriter + Run 两段式，以便在连上游之前就把流开出去。
func StreamWithOptions(w http.ResponseWriter, r io.Reader, opts StreamOptions) error {
	s := NewSSEWriter(w, opts.Model)
	s.StartHeartbeat(opts.Heartbeat)
	defer s.Close()
	return s.Run(r, opts)
}

// textAcc 累积上游文本并回答「这次该增量下发什么」。
//
// 上游有两种下发方式：
//   - 增量（实测主流路径）：data.response 是本次新增的片段
//   - 累计快照（历史版本 / 重放帧）：data.response 是「到目前为止的完整文本」
//
// 默认按**增量**处理（这符合实测的上游行为）；只有当出现「新片段以已累计内容
// 开头且更长」这种快照的铁证时，才把该流切换成快照模式。反转过来的默认值
// （先假设快照）会把合法的短增量误判成旧快照丢掉——思考链的每帧经常只有
// 1~2 个字，恰好是已累计内容的前缀。
type textAcc struct {
	raw  strings.Builder
	mode accMode
}

type accMode int

const (
	accDelta    accMode = iota // 增量（默认）
	accSnapshot                // 累计快照
)

// add 返回本次应下发的增量片段（空串 = 无需下发）。
func (a *textAcc) add(piece string) string {
	if piece == "" {
		return ""
	}
	cur := a.raw.String()
	if cur == "" {
		a.raw.WriteString(piece)
		return piece
	}
	switch a.mode {
	case accSnapshot:
		switch {
		case piece == cur:
			return ""
		case strings.HasPrefix(piece, cur):
			out := piece[len(cur):]
			a.raw.WriteString(out)
			return out
		case strings.HasPrefix(cur, piece):
			return "" // 乱序到达的旧快照
		default:
			// 快照被上游改写（少见）：整体替换，差额按新增下发。
			a.raw.Reset()
			a.raw.WriteString(piece)
			return piece
		}
	default: // accDelta
		// 快照铁证：新片段以全部已累计内容开头且更长 → 后续按快照处理。
		if len(piece) > len(cur) && strings.HasPrefix(piece, cur) {
			a.mode = accSnapshot
			out := piece[len(cur):]
			a.raw.WriteString(out)
			return out
		}
		a.raw.WriteString(piece)
		return piece
	}
}

// String 返回已下发的完整文本。
func (a *textAcc) String() string { return a.raw.String() }

// normalizeStreamToolCalls 把 SOLO tool_calls 归一化成 OpenAI 结构：
// function_call → function，并剥掉 namespace / partial_arguments 等上游专属字段。
func normalizeStreamToolCalls(raw json.RawMessage) ([]map[string]any, bool) {
	var tc []map[string]any
	if err := json.Unmarshal(raw, &tc); err != nil {
		var one map[string]any
		if err := json.Unmarshal(raw, &one); err != nil {
			return nil, false
		}
		tc = []map[string]any{one}
	}
	if len(tc) == 0 {
		return nil, false
	}
	for _, call := range tc {
		if fc, ok := call["function_call"].(map[string]any); ok {
			call["function"] = fc
			delete(call, "function_call")
		}
		if fn, ok := call["function"].(map[string]any); ok {
			delete(fn, "namespace")
			delete(fn, "partial_arguments")
		}
	}
	return tc, true
}

func jsonEscape(s string) string {
	raw, _ := json.Marshal(s)
	return string(raw)
}
