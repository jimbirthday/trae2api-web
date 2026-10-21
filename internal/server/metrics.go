// metrics.go Prometheus /metrics 导出。
//
// 为什么需要这个文件：面板能看总量，但值班/告警需要「机器可抓」的时间序列。
// 手写文本格式而不是引第三方库，是为了守住「零依赖」这条硬约束——
// Prometheus exposition format 本身只是几行文本，没必要为此背上 SDK。
//
// 性能：/metrics 会被每 15s 抓一次，且抓取方常有超时（默认 10s）。所有数据都
// 来自内存里的原子计数与已算好的快照（reqlog.Metrics / usage.Snapshot），
// 不读文件、不打上游、不做全量遍历以外的重活。
//
// 安全：**不鉴权**是刻意的。Prometheus 的 scrape_config 不支持自定义
// Authorization 之外的灵活头，绝大多数部署都直接匿名抓 /metrics。
// 代价是指标会暴露账号数量与流量规模，因此 README 明确要求：
// 生产环境必须用反向代理做 IP 白名单（或只在内网暴露该路径）。
package server

import (
	"bytes"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// metricsBufPool 复用输出缓冲。单次 /metrics 只有几 KB，
// 但在 15s 抓取 + 多实例下复用可以省掉绝大部分分配。
var metricsBufPool = sync.Pool{New: func() any { return new(bytes.Buffer) }}

// promEscape 转义 Prometheus 标签值：反斜杠、双引号、换行必须转义，
// 否则一个含引号的模型名（上游确实有 custom_model_1M 这类自由命名）会让
// 整个 exposition 解析失败——Prometheus 侧表现为「整个 target down」。
func promEscape(s string) string {
	if !strings.ContainsAny(s, "\\\"\n") {
		return s
	}
	var b strings.Builder
	b.Grow(len(s) + 8)
	for _, r := range s {
		switch r {
		case '\\':
			b.WriteString(`\\`)
		case '"':
			b.WriteString(`\"`)
		case '\n':
			b.WriteString(`\n`)
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

// promWriter 极简 exposition 写入器：保证 # HELP / # TYPE 只写一次。
type promWriter struct {
	buf  *bytes.Buffer
	seen map[string]bool
}

func (p *promWriter) header(name, typ, help string) {
	if p.seen[name] {
		return
	}
	p.seen[name] = true
	p.buf.WriteString("# HELP ")
	p.buf.WriteString(name)
	p.buf.WriteByte(' ')
	// HELP 里的换行也会破坏格式，统一压成空格。
	p.buf.WriteString(strings.ReplaceAll(help, "\n", " "))
	p.buf.WriteByte('\n')
	p.buf.WriteString("# TYPE ")
	p.buf.WriteString(name)
	p.buf.WriteByte(' ')
	p.buf.WriteString(typ)
	p.buf.WriteByte('\n')
}

// metric 写一条无标签样本。
func (p *promWriter) metric(name, typ, help string, v float64) {
	p.header(name, typ, help)
	p.buf.WriteString(name)
	p.buf.WriteByte(' ')
	p.buf.WriteString(formatPromFloat(v))
	p.buf.WriteByte('\n')
}

// labeled 写一条带标签样本。
func (p *promWriter) labeled(name, typ, help, labelKey, labelVal string, v float64) {
	p.header(name, typ, help)
	p.buf.WriteString(name)
	p.buf.WriteString(`{`)
	p.buf.WriteString(labelKey)
	p.buf.WriteString(`="`)
	p.buf.WriteString(promEscape(labelVal))
	p.buf.WriteString(`"} `)
	p.buf.WriteString(formatPromFloat(v))
	p.buf.WriteByte('\n')
}

// labeled2 写一条双标签样本。
func (p *promWriter) labeled2(name, typ, help, k1, v1, k2, v2 string, v float64) {
	p.header(name, typ, help)
	p.buf.WriteString(name)
	p.buf.WriteString(`{`)
	p.buf.WriteString(k1)
	p.buf.WriteString(`="`)
	p.buf.WriteString(promEscape(v1))
	p.buf.WriteString(`",`)
	p.buf.WriteString(k2)
	p.buf.WriteString(`="`)
	p.buf.WriteString(promEscape(v2))
	p.buf.WriteString(`"} `)
	p.buf.WriteString(formatPromFloat(v))
	p.buf.WriteByte('\n')
}

// formatPromFloat 用最紧凑的写法输出浮点（整数不带小数点，与 Prometheus 惯例一致）。
func formatPromFloat(v float64) string {
	if v == float64(int64(v)) && v < 1e15 && v > -1e15 {
		return strconv.FormatInt(int64(v), 10)
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}

// startedAt 进程启动时间，用于 uptime。放在包级变量：Handler 可能被测试
// 创建多个，但 uptime 语义是「进程」，不是「Handler」。
var startedAt = time.Now()

// metrics GET /metrics
func (h *Handler) metrics(w http.ResponseWriter, r *http.Request) {
	buf := metricsBufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer metricsBufPool.Put(buf)

	p := &promWriter{buf: buf, seen: map[string]bool{}}

	p.metric("trae2web_up", "gauge", "总是 1，用于存活探测", 1)
	p.metric("trae2web_uptime_seconds", "gauge", "进程启动至今的秒数",
		time.Since(startedAt).Seconds())

	// 账号池：一次性快照，避免每个 state 各拿一次锁。
	if h.cfg.Pool != nil {
		c := h.cfg.Pool.Counts()
		p.labeled("trae2web_accounts", "gauge", "账号数量（按状态）", "state", "total", float64(c.Total))
		p.labeled("trae2web_accounts", "gauge", "账号数量（按状态）", "state", "healthy", float64(c.Healthy))
		p.labeled("trae2web_accounts", "gauge", "账号数量（按状态）", "state", "cooling", float64(c.Cooling))
		p.labeled("trae2web_accounts", "gauge", "账号数量（按状态）", "state", "disabled", float64(c.Disabled))
		p.labeled("trae2web_accounts", "gauge", "账号数量（按状态）", "state", "below_floor", float64(c.BelowFloor))
	}

	if rl := h.cfg.RequestLog; rl != nil {
		m := rl.Metrics()
		p.metric("trae2web_in_flight", "gauge", "当前进行中的请求数", float64(m.InFlight))
		p.labeled("trae2web_requests_total", "counter", "请求累计数（按结果）", "outcome", "success", float64(m.Succeeded))
		p.labeled("trae2web_requests_total", "counter", "请求累计数（按结果）", "outcome", "failure", float64(m.Failed))
		p.labeled("trae2web_requests_total", "counter", "请求累计数（按结果）", "outcome", "client_error", float64(m.ClientErrors))
		p.labeled("trae2web_requests_total", "counter", "请求累计数（按结果）", "outcome", "canceled", float64(m.Canceled))
		p.metric("trae2web_request_duration_ms_avg", "gauge", "平均请求耗时（毫秒）", m.AvgLatencyMs)
		p.metric("trae2web_success_rate", "gauge", "成功率（0..1）", m.SuccessRate)
		p.metric("trae2web_rotations_total", "counter", "换号累计次数", float64(m.Rotates))
		p.labeled("trae2web_tokens_total", "counter", "token 累计数", "kind", "prompt", float64(m.PromptTokens))
		p.labeled("trae2web_tokens_total", "counter", "token 累计数", "kind", "completion", float64(m.ComplTokens))
		p.labeled("trae2web_tokens_total", "counter", "token 累计数", "kind", "total", float64(m.TotalTokens))
		p.metric("trae2web_archive_write_dropped_total", "counter",
			"归档写入被丢弃的累计次数（队列满，不阻塞业务）", float64(m.DroppedWrites))
	}

	// 按模型：用量快照里已算好加权 TPS / 均值首字。
	if u := h.cfg.Usage; u != nil {
		snap := u.Snapshot()
		models := make([]string, 0, len(snap.Models))
		byName := make(map[string]int, len(snap.Models))
		for i, m := range snap.Models {
			models = append(models, m.Model)
			byName[m.Model] = i
		}
		// 排序让输出稳定：Prometheus 不要求，但 diff/人工核对时非常有用
		// （否则每次抓取的样本顺序都不同，看起来像一直在变）。
		sort.Strings(models)
		for _, name := range models {
			st := snap.Models[byName[name]]
			p.labeled("trae2web_model_requests_total", "counter",
				"按模型的请求累计数", "model", name, float64(st.Requests))
			p.labeled2("trae2web_model_tokens_total", "counter", "按模型的 token 累计数",
				"model", name, "kind", "prompt", float64(st.PromptTokens))
			p.labeled2("trae2web_model_tokens_total", "counter", "按模型的 token 累计数",
				"model", name, "kind", "completion", float64(st.CompletionTokens))
			p.labeled2("trae2web_model_tokens_total", "counter", "按模型的 token 累计数",
				"model", name, "kind", "cache_read", float64(st.CacheReadTokens))
			p.labeled("trae2web_model_tps", "gauge",
				"加权输出吞吐（completion_tokens / 生成时长）", "model", name, st.AvgTPS)
			p.labeled("trae2web_model_ttft_ms_avg", "gauge",
				"平均首字耗时（毫秒）", "model", name, st.AvgTTFTMs)
		}
	}

	if h.cfg.Session != nil {
		p.metric("trae2web_sticky_sessions", "gauge", "当前粘性会话绑定数",
			float64(h.cfg.Session.Count()))
	}

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	// 禁止中间缓存（反代/CDN）缓存指标：抓到的会是上一轮的旧值。
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(buf.Bytes())
}
