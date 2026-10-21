// compat.go 补齐 OpenAI 兼容面：legacy /v1/completions、/v1/responses、/v1/embeddings。
//
// 为什么需要这个文件：上游只有 SOLO 的 chat 流，但客户端生态并不统一——
// 老 SDK / 补全插件只认 /v1/completions，新版官方 SDK 与 Agent 框架只认
// /v1/responses，还有一些库会顺手探测 /v1/embeddings。缺这些端点时，
// 客户端报的是「404 找不到路径」，用户会以为网关没起来。
//
// 设计原则：**只做协议翻译，不复制业务逻辑**。所有请求最终都转成一次内部
// 的 chatCompletions 调用（选号/换号/冷却/usage 记账/请求流水全复用），
// 再把 chat 的输出翻译回目标协议。复制一份 chat 逻辑必然在下次修 bug 时漏掉一半。
package server

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// maxLegacyPrompts legacy /v1/completions 批量 prompt 上限。
// 每个 prompt 都是一次独立的上游调用（占一个账号名额），放开会让单个
// HTTP 请求轻易打满整个号池；8 足够覆盖补全批量场景。
const maxLegacyPrompts = 8

// compatID 生成 OpenAI 风格的资源 id（cmpl- / resp_ / msg_ / rs_）。
// 用随机数而不是时间戳：id 会被客户端当幂等键/日志关联键，多实例部署时
// 时间戳会撞号，而且会泄露服务器时钟。
func compatID(prefix string) string {
	b := make([]byte, 12)
	if _, err := rand.Read(b); err != nil {
		return prefix + "unknown"
	}
	return prefix + hex.EncodeToString(b)
}

// readCompatBody 读请求体（与 chat 路径同一个 8MB 上限）。
func readCompatBody(r *http.Request) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes+1))
	if err != nil {
		return nil, err
	}
	if len(body) > maxBodyBytes {
		return nil, fmt.Errorf("request body exceeds 8MB limit")
	}
	return body, nil
}

// subChatRequest 构造一次「内部 chat 调用」的请求。
//
// 必须 Clone 原请求：chat 路径依赖 ctx（客户端断连要立刻中止上游）、
// 依赖 Header（鉴权已经在上层做过，但 X-Request-ID 等要串起来）。
// Body 换成我们改写后的 JSON，ContentLength 同步，否则 chat 里的
// LimitReader 读到 0 就以为空 body。
func subChatRequest(r *http.Request, body []byte) *http.Request {
	sub := r.Clone(r.Context())
	sub.Body = io.NopCloser(bytes.NewReader(body))
	sub.ContentLength = int64(len(body))
	sub.Header = r.Header.Clone()
	sub.Header.Set("Content-Type", "application/json")
	return sub
}

// captureChat 以内部子请求调用 chatCompletions，接住它的原始输出（JSON）。
//
// 只用于**非流式**翻译：流式必须真流式转发（见 sseTranslateWriter），
// 用 recorder 会把整段回复缓冲到内存，客户端又变成「等十几秒然后一次性刷出」，
// 正是本项目之前修掉的「不丝滑」问题。
func (h *Handler) captureChat(w http.ResponseWriter, r *http.Request, body []byte) (status int, ctype string, payload []byte) {
	rec := newBufferedWriter(w)
	h.chatCompletions(rec, subChatRequest(r, body))
	return rec.status, rec.ctype, rec.body.Bytes()
}

// bufferedWriter 极简 ResponseWriter 桩：接住状态码/Content-Type/正文。
// 不用 httptest.ResponseRecorder 是为了让非测试构建也不必引入该包
// （它本身没问题，但这里只需要 4 个字段）。
type bufferedWriter struct {
	hdr    http.Header
	status int
	ctype  string
	body   bytes.Buffer
}

func newBufferedWriter(parent http.ResponseWriter) *bufferedWriter {
	bw := &bufferedWriter{hdr: http.Header{}, status: http.StatusOK}
	// 复用上层已经定好的 X-Request-ID，让「客户端拿到的 ID」与「流水里的 ID」一致。
	if id := parent.Header().Get("X-Request-ID"); id != "" {
		bw.hdr.Set("X-Request-ID", id)
	}
	return bw
}

func (b *bufferedWriter) Header() http.Header { return b.hdr }

func (b *bufferedWriter) WriteHeader(code int) {
	if b.status == http.StatusOK && b.ctype == "" {
		b.status = code
		b.ctype = b.hdr.Get("Content-Type")
	}
}

func (b *bufferedWriter) Write(p []byte) (int, error) {
	if b.ctype == "" {
		b.ctype = b.hdr.Get("Content-Type")
	}
	return b.body.Write(p)
}

// Flush 实现 http.Flusher 空操作：缓冲模式下不需要真正 flush
// （但必须实现，否则 SSEWriter 的 asFlusher 会拿到 nil 而退化成无 flush）。
func (b *bufferedWriter) Flush() {}

// sseTranslateWriter 把 chat 的 SSE 流即时翻译成另一种 SSE 协议。
//
// 为什么不用 recorder：翻译必须是流式的。它在 Write 里按行切分（只处理
// 以 \n 结尾的完整行，残行留到下次），把 data: 负载交给协议转换器，
// 并透传 Flush —— 少了 Flush，客户端要等到整段回复结束才看到内容。
//
// 并发：上游 SSEWriter 的心跳协程会与解析协程同时写（注释帧 + 正文帧），
// 所以内部必须加锁；Write/Flush 都持锁，避免半行被两个 goroutine 交错。
type sseTranslateWriter struct {
	dst    http.ResponseWriter
	handle func(payload []byte) [][]byte // data: 负载 → 要写出的字节块（nil = 丢弃）

	mu       sync.Mutex
	buf      []byte
	sawFirst bool
	jsonMode bool // 首个字节不是 SSE → chat 走的是非流式错误路径，原样透传
	flushErr bool
}

func newSSETranslateWriter(dst http.ResponseWriter, handle func([]byte) [][]byte) *sseTranslateWriter {
	return &sseTranslateWriter{dst: dst, handle: handle}
}

func (t *sseTranslateWriter) Header() http.Header { return t.dst.Header() }

func (t *sseTranslateWriter) WriteHeader(code int) { t.dst.WriteHeader(code) }

func (t *sseTranslateWriter) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.sawFirst {
		t.sawFirst = true
		// chat 只有在「上游已接受请求」之后才开始写 SSE；在那之前出错会走
		// writeJSON（首个字节是 '{'）。这种情况原样透传，客户端拿到标准
		// OpenAI 错误体，比翻译成半个 SSE 帧更有用。
		if len(p) > 0 && p[0] != 'd' && p[0] != ':' && p[0] != '\n' && p[0] != '\r' {
			t.jsonMode = true
		}
	}
	if t.jsonMode {
		n, err := t.dst.Write(p)
		t.flushLocked()
		return n, err
	}
	t.buf = append(t.buf, p...)
	for {
		i := bytes.IndexByte(t.buf, '\n')
		if i < 0 {
			break
		}
		line := t.buf[:i]
		t.buf = t.buf[i+1:]
		t.handleLineLocked(line)
	}
	t.flushLocked()
	return len(p), nil
}

// Flush 透传给下游：SSE 的「丝滑」全靠它。
func (t *sseTranslateWriter) Flush() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.flushLocked()
}

func (t *sseTranslateWriter) flushLocked() {
	if f, ok := t.dst.(http.Flusher); ok {
		f.Flush()
	}
}

func (t *sseTranslateWriter) handleLineLocked(line []byte) {
	line = bytes.TrimRight(line, "\r")
	if len(line) == 0 {
		return
	}
	if line[0] == ':' {
		// 心跳注释：保留，否则长思考期客户端/反代会把连接判成空闲断开。
		t.writeRawLocked([]byte(": keepalive\n\n"))
		return
	}
	const p = "data:"
	if !bytes.HasPrefix(line, []byte(p)) {
		return // event: 行等 —— chat 不发，忽略
	}
	payload := bytes.TrimSpace(line[len(p):])
	for _, chunk := range t.handle(payload) {
		t.writeRawLocked(chunk)
	}
}

func (t *sseTranslateWriter) writeRawLocked(b []byte) {
	if len(b) == 0 {
		return
	}
	if _, err := t.dst.Write(b); err != nil {
		t.flushErr = true
	}
	t.flushLocked()
}

// finish 收尾：把残留的半行（上游意外断开）丢掉，并在需要时补齐终止帧。
// 协议转换器自己负责「只发一次终止帧」。
// passthrough 报告 chat 是否走了非流式（JSON 错误）路径。
// 用方法而不是直读字段：调用方在另一个 goroutine 结束后读取，
// 加锁能明确表达「这个字段由 t.mu 保护」，避免日后被误当无锁字段改坏。
func (t *sseTranslateWriter) passthrough() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.jsonMode
}

func (t *sseTranslateWriter) finish() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.buf = nil
}

// ---------------------------------------------------------------------------
// POST /v1/completions —— legacy text completion
// ---------------------------------------------------------------------------

// extractPrompts 解析 legacy prompt 字段。
//
// 支持：string、[]string（批量）。
// 明确拒绝：[]int / [][]int（token ids）——我们没有分词器，把 token id 当
// 字符串发给上游只会得到乱码回复，比明确报 400 更糟。
func extractPrompts(v any) ([]string, error) {
	switch p := v.(type) {
	case string:
		if strings.TrimSpace(p) == "" {
			return nil, fmt.Errorf("prompt must not be empty")
		}
		return []string{p}, nil
	case []any:
		if len(p) == 0 {
			return nil, fmt.Errorf("prompt array must not be empty")
		}
		out := make([]string, 0, len(p))
		for i, item := range p {
			switch it := item.(type) {
			case string:
				out = append(out, it)
			case float64, int, int64:
				return nil, fmt.Errorf("prompt[%d]: token-id prompts are not supported "+
					"(the upstream has no detokenizer; send text instead)", i)
			case []any:
				return nil, fmt.Errorf("prompt[%d]: nested token-id arrays are not supported "+
					"(send text instead)", i)
			default:
				return nil, fmt.Errorf("prompt[%d]: unsupported element type %T", i, item)
			}
		}
		return out, nil
	default:
		return nil, fmt.Errorf("prompt is required (string or array of strings)")
	}
}

// legacyAllowed legacy 请求里我们真正会转发的字段。
// 其余字段（echo/suffix/best_of/logprobs/logit_bias/n/...）显式忽略并打日志：
// 静默丢弃会让「设了 best_of 却没变化」变成无法排查的谜。
var legacyAllowed = map[string]bool{
	"model": true, "prompt": true, "stream": true, "max_tokens": true,
	"temperature": true, "top_p": true, "stop": true, "user": true,
	"presence_penalty": true, "frequency_penalty": true, "stream_options": true,
	"max_completion_tokens": true, // 会映射到 max_tokens，不算忽略
}

func paramStr(obj map[string]any, key string) string {
	s, _ := obj[key].(string)
	return strings.TrimSpace(s)
}

func paramBool(obj map[string]any, key string) bool {
	b, _ := obj[key].(bool)
	return b
}

// logIgnoredFields 记录被忽略的字段（debug 可见，便于排查「参数没生效」）。
func logIgnoredFields(endpoint string, obj map[string]any, allowed map[string]bool) {
	var ignored []string
	for k := range obj {
		if !allowed[k] {
			ignored = append(ignored, k)
		}
	}
	if len(ignored) == 0 {
		return
	}
	sort.Strings(ignored)
	log.Printf("[compat] %s 忽略不支持的字段: %s", endpoint, strings.Join(ignored, ","))
}

// legacyChatBody 把一次 legacy completion 请求翻译成 chat 请求体。
func legacyChatBody(model, prompt string, stream bool, src map[string]any) map[string]any {
	body := map[string]any{
		"model":    model,
		"messages": []map[string]any{{"role": "user", "content": prompt}},
	}
	if stream {
		body["stream"] = true
		// 要 include_usage 才能拿到 usage；翻译层再用它填最后一帧。
		body["stream_options"] = map[string]any{"include_usage": true}
	}
	for _, k := range []string{"max_tokens", "temperature", "top_p", "stop", "user",
		"presence_penalty", "frequency_penalty"} {
		if v, ok := src[k]; ok {
			body[k] = v
		}
	}
	// OpenAI 新字段：max_completion_tokens 优先（与 payload.go 的口径一致）。
	if v, ok := src["max_completion_tokens"]; ok {
		body["max_tokens"] = v
	}
	return body
}

func (h *Handler) legacyCompletions(w http.ResponseWriter, r *http.Request) {
	raw, err := readCompatBody(r)
	if err != nil {
		writeOpenAIError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	var src map[string]any
	if err := json.Unmarshal(raw, &src); err != nil {
		writeOpenAIError(w, http.StatusBadRequest, "invalid_request", "request body is not valid JSON")
		return
	}
	logIgnoredFields("/v1/completions", src, legacyAllowed)

	model := paramStr(src, "model")
	prompts, perr := extractPrompts(src["prompt"])
	if perr != nil {
		writeOpenAIError(w, http.StatusBadRequest, "invalid_request", perr.Error())
		return
	}
	if len(prompts) > maxLegacyPrompts {
		writeOpenAIError(w, http.StatusBadRequest, "invalid_request",
			fmt.Sprintf("at most %d prompts per request (got %d)", maxLegacyPrompts, len(prompts)))
		return
	}
	stream := paramBool(src, "stream")
	if stream {
		if len(prompts) != 1 {
			// 批量 + 流式：需要多路并发流合并并按 index 排序，收益低、易错。
			// 明确拒绝，别让客户端拿到只含第一个 prompt 的半成品流。
			writeOpenAIError(w, http.StatusBadRequest, "invalid_request",
				"streaming with a prompt array is not supported; send one prompt per request")
			return
		}
		h.streamLegacy(w, r, model, prompts[0], src)
		return
	}
	h.bufferedLegacy(w, r, model, prompts, src)
}

// bufferedLegacy 非流式：逐 prompt 调 chat，再合并成 text_completion。
//
// 并发度按**账号数**钳制（每个 prompt 至少独占一个号的在途名额）：
//   - 串行：单个 chat 调用含上游排队+生成，实测几秒到几十秒，8 个会拖到分钟级；
//   - 全并发：1~2 个号的池会被自己的批量请求占满，子请求直接
//     「no_healthy_account」503 —— 这不是上游的问题，是我们自己造的拥塞。
//
// 取 min(prompts, max(1, 账号总数))：既拿到并行收益，又不会自我打死。
func (h *Handler) bufferedLegacy(w http.ResponseWriter, r *http.Request, model string, prompts []string, src map[string]any) {
	type res struct {
		content, reasoning, finish string
		usage                      map[string]any
		status                     int
		errRaw                     []byte
		ctype                      string
	}
	out := make([]res, len(prompts))
	limit := len(prompts)
	if h.cfg.Pool != nil {
		if n := h.cfg.Pool.Counts().Total; n > 0 && n < limit {
			limit = n
		}
	}
	if limit < 1 {
		limit = 1
	}
	sem := make(chan struct{}, limit)
	var wg sync.WaitGroup
	for i, p := range prompts {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int, prompt string) {
			defer wg.Done()
			defer func() { <-sem }()
			body, _ := json.Marshal(legacyChatBody(model, prompt, false, src))
			status, ctype, payload := h.captureChat(w, r, body)
			out[i] = res{status: status, ctype: ctype}
			if status != http.StatusOK {
				out[i].errRaw = payload
				return
			}
			content, reasoning, usage, finish, cerr := chatResult(payload)
			if cerr != nil {
				out[i].status = http.StatusBadGateway
				out[i].errRaw = payload
				return
			}
			out[i].content, out[i].reasoning, out[i].usage, out[i].finish = content, reasoning, usage, finish
		}(i, p)
	}
	wg.Wait()

	// 任一子请求失败就整体失败：返回部分结果会让客户端以为「其他 prompt 真的空回复」。
	for _, o := range out {
		if o.status != http.StatusOK {
			if len(o.errRaw) > 0 && strings.Contains(o.ctype, "json") {
				// 已经是 OpenAI 错误体，原样回（含它自己的 status）。
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(o.status)
				_, _ = w.Write(o.errRaw)
				return
			}
			writeOpenAIError(w, o.status, "upstream_error", "chat completion failed")
			return
		}
	}

	choices := make([]any, 0, len(out))
	total := map[string]int64{"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
	for i, o := range out {
		finish := o.finish
		if finish == "" {
			finish = "stop"
		}
		choices = append(choices, map[string]any{
			"text":          o.content,
			"index":         i,
			"logprobs":      nil,
			"finish_reason": finish,
		})
		for _, k := range []string{"prompt_tokens", "completion_tokens", "total_tokens"} {
			if v, ok := numFromAny(o.usage[k]); ok {
				total[k] += v
			}
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"id":      compatID("cmpl-"),
		"object":  "text_completion",
		"created": time.Now().Unix(),
		// model 必须回显客户端传的原始名（与 chat 路径「别名无损」策略一致）。
		"model":   model,
		"choices": choices,
		"usage": map[string]any{
			"prompt_tokens":     total["prompt_tokens"],
			"completion_tokens": total["completion_tokens"],
			"total_tokens":      total["total_tokens"],
		},
	})
}

// numFromAny 把 JSON 解出来的数字（float64 / int64）归一成 int64。
func numFromAny(v any) (int64, bool) {
	switch n := v.(type) {
	case float64:
		return int64(n), true
	case int64:
		return n, true
	case int:
		return int64(n), true
	case json.Number:
		if i, err := n.Int64(); err == nil {
			return i, true
		}
	}
	return 0, false
}

// chatResult 从 chat.completion JSON 里取出正文/思考/usage/finish_reason。
func chatResult(raw []byte) (content, reasoning string, usage map[string]any, finish string, err error) {
	var m map[string]any
	if uerr := json.Unmarshal(raw, &m); uerr != nil {
		return "", "", nil, "", fmt.Errorf("chat response is not JSON: %w", uerr)
	}
	if e, ok := m["error"]; ok {
		b, _ := json.Marshal(e)
		return "", "", nil, "", fmt.Errorf("chat error: %s", string(b))
	}
	if chs, ok := m["choices"].([]any); ok && len(chs) > 0 {
		if c, ok := chs[0].(map[string]any); ok {
			finish, _ = c["finish_reason"].(string)
			if msg, ok := c["message"].(map[string]any); ok {
				content, _ = msg["content"].(string)
				reasoning, _ = msg["reasoning_content"].(string)
			}
		}
	}
	usage, _ = m["usage"].(map[string]any)
	return content, reasoning, usage, finish, nil
}

// legacyStream 把 chat SSE 帧翻译成 legacy text_completion SSE 帧。
//
// 思考内容（reasoning_content）在 legacy 协议里没有对应字段，直接丢弃：
// 硬塞进 text 会让补全客户端把思考当成答案续写。
type legacyStream struct {
	mu      sync.Mutex
	id      string
	created int64
	model   string
	usage   map[string]any
	done    bool
	emitted bool
}

func (l *legacyStream) frame(text, finish string, withUsage bool) []byte {
	choice := map[string]any{"text": text, "index": 0, "logprobs": nil}
	if finish == "" {
		choice["finish_reason"] = nil
	} else {
		choice["finish_reason"] = finish
	}
	payload := map[string]any{
		"id":      l.id,
		"object":  "text_completion",
		"created": l.created,
		"model":   l.model,
		"choices": []any{choice},
	}
	if withUsage && l.usage != nil {
		payload["usage"] = l.usage
	}
	raw, _ := json.Marshal(payload)
	return append(append([]byte("data: "), raw...), '\n', '\n')
}

// payload 实现 sseTranslateWriter 的回调。
func (l *legacyStream) payload(p []byte) [][]byte {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.done {
		return nil
	}
	if bytes.Equal(p, []byte("[DONE]")) {
		l.done = true
		out := [][]byte{l.frame("", "stop", true)}
		return append(out, []byte("data: [DONE]\n\n"))
	}
	var chunk struct {
		Choices []struct {
			Delta map[string]any `json:"delta"`
		} `json:"choices"`
		Usage map[string]any `json:"usage"`
	}
	if err := json.Unmarshal(p, &chunk); err != nil {
		return nil
	}
	if chunk.Usage != nil {
		l.usage = chunk.Usage
	}
	var out [][]byte
	for _, c := range chunk.Choices {
		if c.Delta == nil {
			continue
		}
		if text, ok := c.Delta["content"].(string); ok && text != "" {
			out = append(out, l.frame(text, "", false))
			l.emitted = true
		}
	}
	return out
}

// streamLegacy 流式翻译：把子请求直接写进翻译型 ResponseWriter。
func (h *Handler) streamLegacy(w http.ResponseWriter, r *http.Request, model, prompt string, src map[string]any) {
	ls := &legacyStream{id: compatID("cmpl-"), created: time.Now().Unix(), model: model}
	tw := newSSETranslateWriter(w, ls.payload)
	body, _ := json.Marshal(legacyChatBody(model, prompt, true, src))
	h.chatCompletions(tw, subChatRequest(r, body))
	tw.finish()
	// 一条正文都没翻译出来（例如上游只给了思考、或整条流空）：补一个终止帧，
	// 否则会按「流开了但没 [DONE]」被客户端判成连接异常。
	ls.mu.Lock()
	need := !ls.done
	ls.mu.Unlock()
	if need {
		if _, err := w.Write(append(ls.frame("", "stop", true), []byte("data: [DONE]\n\n")...)); err != nil {
			return
		}
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
	}
}

// ---------------------------------------------------------------------------
// POST /v1/responses —— Responses API 最小可用子集
// ---------------------------------------------------------------------------

var responsesAllowed = map[string]bool{
	"model": true, "input": true, "instructions": true, "stream": true,
	"max_output_tokens": true, "temperature": true, "top_p": true, "user": true,
}

// flattenResponseContent 把 Responses 的 content 归一成纯文本。
//
// Responses 的内容是分片数组：[{"type":"input_text","text":"..."}, ...]，
// chat 只认字符串；不发数组上游会 4xx。多种 type 名（input_text/text/output_text）
// 都接受，因为不同 SDK 版本写法不一致。
func flattenResponseContent(v any) (string, error) {
	switch c := v.(type) {
	case nil:
		return "", nil
	case string:
		return c, nil
	case []any:
		var b strings.Builder
		for i, part := range c {
			m, ok := part.(map[string]any)
			if !ok {
				return "", fmt.Errorf("content[%d]: unsupported element type %T", i, part)
			}
			t, _ := m["type"].(string)
			switch t {
			case "input_text", "text", "output_text", "summary_text":
				s, _ := m["text"].(string)
				b.WriteString(s)
			default:
				return "", fmt.Errorf("content[%d]: unsupported content type %q "+
					"(only text parts are supported; images/files are not)", i, t)
			}
		}
		return b.String(), nil
	default:
		return "", fmt.Errorf("content: unsupported type %T", v)
	}
}

// responsesToMessages 把 instructions + input 翻译成 chat messages。
func responsesToMessages(req map[string]any) ([]map[string]any, error) {
	var msgs []map[string]any
	if ins, ok := req["instructions"].(string); ok && strings.TrimSpace(ins) != "" {
		msgs = append(msgs, map[string]any{"role": "system", "content": ins})
	}
	switch in := req["input"].(type) {
	case string:
		if strings.TrimSpace(in) == "" {
			return nil, fmt.Errorf("input must not be empty")
		}
		msgs = append(msgs, map[string]any{"role": "user", "content": in})
	case []any:
		for i, item := range in {
			switch it := item.(type) {
			case string:
				msgs = append(msgs, map[string]any{"role": "user", "content": it})
			case map[string]any:
				if t, _ := it["type"].(string); t != "" && t != "message" {
					return nil, fmt.Errorf("input[%d]: unsupported item type %q "+
						"(only message items are supported)", i, t)
				}
				role, _ := it["role"].(string)
				if role == "" {
					role = "user"
				}
				text, err := flattenResponseContent(it["content"])
				if err != nil {
					return nil, fmt.Errorf("input[%d]: %w", i, err)
				}
				msgs = append(msgs, map[string]any{"role": role, "content": text})
			default:
				return nil, fmt.Errorf("input[%d]: unsupported element type %T", i, item)
			}
		}
	default:
		return nil, fmt.Errorf("input is required (string or array of messages)")
	}
	if len(msgs) == 0 {
		return nil, fmt.Errorf("input produced no messages")
	}
	return msgs, nil
}

// responseUsage 把 chat usage 映射成 Responses usage 形状。
func responseUsage(usage map[string]any) map[string]any {
	if usage == nil {
		return nil
	}
	in, _ := numFromAny(usage["prompt_tokens"])
	outTok, _ := numFromAny(usage["completion_tokens"])
	tot, _ := numFromAny(usage["total_tokens"])
	if tot == 0 {
		tot = in + outTok
	}
	// 思考 token 在 OpenAI 里走 output_tokens_details.reasoning_tokens。
	reason := int64(0)
	if d, ok := usage["completion_tokens_details"].(map[string]any); ok {
		if v, ok := numFromAny(d["reasoning_tokens"]); ok {
			reason = v
		}
	}
	return map[string]any{
		"input_tokens":  in,
		"output_tokens": outTok,
		"total_tokens":  tot,
		"output_tokens_details": map[string]any{
			"reasoning_tokens": reason,
		},
	}
}

// buildResponseObject 组装 Responses API 的 response 对象。
func buildResponseObject(id, model string, created int64, status string,
	output []any, usage map[string]any) map[string]any {
	obj := map[string]any{
		"id":         id,
		"object":     "response",
		"created_at": created,
		// 回显客户端传的模型名（不因别名/内部映射而改变）。
		"model":               model,
		"status":              status,
		"output":              output,
		"parallel_tool_calls": true,
		"tool_choice":         "auto",
		"tools":               []any{},
	}
	if usage != nil {
		obj["usage"] = usage
	}
	return obj
}

// messageItem 组装一条 assistant message output item。
func messageItem(msgID, text, status string) map[string]any {
	return map[string]any{
		"type":   "message",
		"id":     msgID,
		"status": status,
		"role":   "assistant",
		"content": []any{map[string]any{
			"type": "output_text", "text": text, "annotations": []any{},
		}},
	}
}

func (h *Handler) responses(w http.ResponseWriter, r *http.Request) {
	raw, err := readCompatBody(r)
	if err != nil {
		writeOpenAIError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	var src map[string]any
	if err := json.Unmarshal(raw, &src); err != nil {
		writeOpenAIError(w, http.StatusBadRequest, "invalid_request", "request body is not valid JSON")
		return
	}
	logIgnoredFields("/v1/responses", src, responsesAllowed)

	model := paramStr(src, "model")
	msgs, merr := responsesToMessages(src)
	if merr != nil {
		writeOpenAIError(w, http.StatusBadRequest, "invalid_request", merr.Error())
		return
	}
	stream := paramBool(src, "stream")
	chatBody := map[string]any{"model": model, "messages": msgs}
	// max_output_tokens → max_tokens（上游只认后者）。
	if v, ok := src["max_output_tokens"]; ok {
		chatBody["max_tokens"] = v
	}
	for _, k := range []string{"temperature", "top_p", "user"} {
		if v, ok := src[k]; ok {
			chatBody[k] = v
		}
	}
	if stream {
		chatBody["stream"] = true
		chatBody["stream_options"] = map[string]any{"include_usage": true}
	}
	if stream {
		h.streamResponses(w, r, model, chatBody)
		return
	}

	body, _ := json.Marshal(chatBody)
	status, ctype, payload := h.captureChat(w, r, body)
	if status != http.StatusOK {
		// chat 的错误体已经是 OpenAI error schema，原样透传（含状态码）。
		cType := ctype
		if cType == "" {
			cType = "application/json"
		}
		w.Header().Set("Content-Type", cType)
		w.WriteHeader(status)
		_, _ = w.Write(payload)
		return
	}
	content, reasoning, usage, _, cerr := chatResult(payload)
	if cerr != nil {
		writeOpenAIError(w, http.StatusBadGateway, "upstream_parse", cerr.Error())
		return
	}
	created := time.Now().Unix()
	id := compatID("resp_")
	output := make([]any, 0, 2)
	if reasoning != "" {
		// 思考作为 reasoning item 放在最前面（Responses 规范的顺序）。
		output = append(output, map[string]any{
			"type": "reasoning", "id": compatID("rs_"), "status": "completed",
			"summary": []any{map[string]any{"type": "summary_text", "text": reasoning}},
		})
	}
	output = append(output, messageItem(compatID("msg_"), content, "completed"))
	resp := buildResponseObject(id, model, created, "completed", output, responseUsage(usage))
	// output_text 是便捷字段：官方 SDK 的 .output_text 直接读它，
	// 缺了会让调用方拿到空字符串（很常见的踩坑点）。
	resp["output_text"] = content
	writeJSON(w, http.StatusOK, resp)
}

// responsesStream 把 chat SSE 翻译成 Responses 事件序列。
type responsesStream struct {
	mu      sync.Mutex
	id      string
	model   string
	created int64
	seq     int
	nextIdx int

	rsID, msgID     string
	rsIdx, msgIdx   int
	rsOpen, msgOpen bool
	rsDone, msgDone bool
	text, reasoning strings.Builder
	usage           map[string]any
	done            bool
	// headSent 标记 response.created / response.in_progress 是否已发。
	// 惰性发送（等第一帧真实数据）而不是提前发：若 chat 在开流前就失败，
	// 它会写一个 JSON 错误体（4xx/5xx），此时流还没「开」，客户端能拿到
	// 正常的状态码与错误 JSON，而不是一个半截 SSE 流。
	headSent bool
}

func (s *responsesStream) event(name string, payload map[string]any) []byte {
	payload["type"] = name
	payload["sequence_number"] = s.seq
	s.seq++
	raw, _ := json.Marshal(payload)
	out := append([]byte("event: "), name...)
	out = append(out, '\n')
	out = append(out, "data: "...)
	out = append(out, raw...)
	return append(out, '\n', '\n')
}

// payload 实现 sseTranslateWriter 的回调。
func (s *responsesStream) payload(p []byte) [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.done {
		return nil
	}
	out := s.prologue()
	if bytes.Equal(p, []byte("[DONE]")) {
		// Responses 规范不用 [DONE]；只补 response.completed 收尾。
		s.done = true
		return append(out, s.complete()...)
	}
	var chunk struct {
		Choices []struct {
			Delta map[string]any `json:"delta"`
		} `json:"choices"`
		Usage map[string]any `json:"usage"`
	}
	if err := json.Unmarshal(p, &chunk); err != nil {
		return nil
	}
	if chunk.Usage != nil {
		s.usage = chunk.Usage
	}
	for _, c := range chunk.Choices {
		if c.Delta == nil {
			continue
		}
		// 思考先来：先建 reasoning item，再建 message item，保证 output 顺序正确。
		if think, ok := c.Delta["reasoning_content"].(string); ok && think != "" {
			out = append(out, s.ensureReasoning()...)
			s.reasoning.WriteString(think)
			out = append(out, s.event("response.reasoning_summary_text.delta", map[string]any{
				"item_id": s.rsID, "output_index": s.rsIdx, "summary_index": 0, "delta": think,
			}))
		}
		if text, ok := c.Delta["content"].(string); ok && text != "" {
			out = append(out, s.ensureMessage()...)
			s.text.WriteString(text)
			out = append(out, s.event("response.output_text.delta", map[string]any{
				"item_id": s.msgID, "output_index": s.msgIdx, "content_index": 0, "delta": text,
			}))
		}
	}
	return out
}

// prologue 首次写数据前补上 created / in_progress（Responses 规范要求它们先于
// 任何增量事件出现，客户端据此建立 response 对象）。
func (s *responsesStream) prologue() [][]byte {
	if s.headSent {
		return nil
	}
	s.headSent = true
	return [][]byte{
		s.event("response.created", map[string]any{
			"response": buildResponseObject(s.id, s.model, s.created, "in_progress", []any{}, nil),
		}),
		s.event("response.in_progress", map[string]any{
			"response": buildResponseObject(s.id, s.model, s.created, "in_progress", []any{}, nil),
		}),
	}
}

func (s *responsesStream) ensureReasoning() [][]byte {
	if s.rsOpen {
		return nil
	}
	s.rsOpen = true
	s.rsID = compatID("rs_")
	s.rsIdx = s.nextIdx
	s.nextIdx++
	return [][]byte{
		s.event("response.output_item.added", map[string]any{
			"output_index": s.rsIdx,
			"item": map[string]any{"type": "reasoning", "id": s.rsID, "status": "in_progress",
				"summary": []any{}},
		}),
		s.event("response.reasoning_summary_part.added", map[string]any{
			"item_id": s.rsID, "output_index": s.rsIdx, "summary_index": 0,
			"part": map[string]any{"type": "summary_text", "text": ""},
		}),
	}
}

func (s *responsesStream) ensureMessage() [][]byte {
	if s.msgOpen {
		return nil
	}
	s.msgOpen = true
	s.msgID = compatID("msg_")
	s.msgIdx = s.nextIdx
	s.nextIdx++
	return [][]byte{
		s.event("response.output_item.added", map[string]any{
			"output_index": s.msgIdx,
			"item": map[string]any{"type": "message", "id": s.msgID, "status": "in_progress",
				"role": "assistant", "content": []any{}},
		}),
		s.event("response.content_part.added", map[string]any{
			"item_id": s.msgID, "output_index": s.msgIdx, "content_index": 0,
			"part": map[string]any{"type": "output_text", "text": "", "annotations": []any{}},
		}),
	}
}

// complete 收尾：补齐 done 事件与 response.completed。
func (s *responsesStream) complete() [][]byte {
	out := s.prologue()
	text, think := s.text.String(), s.reasoning.String()
	if s.rsOpen && !s.rsDone {
		s.rsDone = true
		out = append(out,
			s.event("response.reasoning_summary_text.done", map[string]any{
				"item_id": s.rsID, "output_index": s.rsIdx, "summary_index": 0, "text": think,
			}),
			s.event("response.output_item.done", map[string]any{
				"output_index": s.rsIdx,
				"item": map[string]any{"type": "reasoning", "id": s.rsID, "status": "completed",
					"summary": []any{map[string]any{"type": "summary_text", "text": think}}},
			}),
		)
	}
	// 内容为空也要给一个 message item：客户端按 output[0].content 取结果，
	// 空 output 数组会让它们抛数组越界。
	if !s.msgOpen {
		out = append(out, s.ensureMessage()...)
	}
	if !s.msgDone {
		s.msgDone = true
		out = append(out,
			s.event("response.output_text.done", map[string]any{
				"item_id": s.msgID, "output_index": s.msgIdx, "content_index": 0, "text": text,
			}),
			s.event("response.content_part.done", map[string]any{
				"item_id": s.msgID, "output_index": s.msgIdx, "content_index": 0,
				"part": map[string]any{"type": "output_text", "text": text, "annotations": []any{}},
			}),
			s.event("response.output_item.done", map[string]any{
				"output_index": s.msgIdx,
				"item":         messageItem(s.msgID, text, "completed"),
			}),
		)
	}
	output := make([]any, s.nextIdx)
	if s.rsOpen {
		output[s.rsIdx] = map[string]any{"type": "reasoning", "id": s.rsID, "status": "completed",
			"summary": []any{map[string]any{"type": "summary_text", "text": think}}}
	}
	if s.msgOpen {
		output[s.msgIdx] = messageItem(s.msgID, text, "completed")
	}
	compact := make([]any, 0, len(output))
	for _, item := range output {
		if item != nil {
			compact = append(compact, item)
		}
	}
	resp := buildResponseObject(s.id, s.model, s.created, "completed", compact, responseUsage(s.usage))
	resp["output_text"] = text
	out = append(out, s.event("response.completed", map[string]any{"response": resp}))
	return out
}

func (h *Handler) streamResponses(w http.ResponseWriter, r *http.Request, model string, chatBody map[string]any) {
	rs := &responsesStream{id: compatID("resp_"), model: model, created: time.Now().Unix()}
	tw := newSSETranslateWriter(w, rs.payload)
	body, _ := json.Marshal(chatBody)
	h.chatCompletions(tw, subChatRequest(r, body))
	tw.finish()
	// 若 chat 走的是非流式错误路径（jsonMode 透传），这里不能再补事件。
	rs.mu.Lock()
	need := !rs.done
	rs.mu.Unlock()
	jsonMode := tw.passthrough()
	if need && !jsonMode {
		for _, chunk := range rs.payload([]byte("[DONE]")) {
			if _, err := w.Write(chunk); err != nil {
				return
			}
		}
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
	}
}

// ---------------------------------------------------------------------------
// POST /v1/embeddings —— 明确不支持
// ---------------------------------------------------------------------------

// embeddingsUnsupported 返回 501 而不是 404。
//
// 返回 404 会让调用方以为「路径写错了/版本不对」，从而去改 base_url、试别的
// 路径，浪费时间；501 明确表达「端点存在但本网关/上游没有这个能力」。
func (h *Handler) embeddingsUnsupported(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusNotImplemented)
	_, _ = w.Write([]byte(`{"error":{"message":"embeddings are not supported by the upstream ` +
		`(TRAE SOLO has no /embeddings endpoint)","type":"invalid_request_error",` +
		`"code":"not_implemented","param":null}}`))
}
