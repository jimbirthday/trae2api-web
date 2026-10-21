// helpers.go admin 子包共用的小工具。
package server

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"html"
	"io"
	"net/http"
	"os"
	"strings"
)

// decodeBodyOptional 读 body（限 1MB）并 JSON 解码到 v；body 为空时返回 nil 不报错。
// 用于 login start/cancel 等可选 body 的接口。
func decodeBodyOptional(r *http.Request, v any) error {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		return err
	}
	if len(body) == 0 {
		return nil
	}
	return json.Unmarshal(body, v)
}

// mkdirAll 包装 os.MkdirAll（便于 mock；当前直接转 os.MkdirAll）。
func mkdirAll(dir string) error {
	if dir == "" {
		return nil
	}
	return os.MkdirAll(dir, 0o755)
}

// authorizeRender 渲染 /authorize 回调后的浏览器可见页面（非 JSON）。
// 复用 admin.html 的深色基调，但极简：一个标题 + 一段说明 + 自动关闭尝试。
func authorizeRender(w http.ResponseWriter, status int, title, detail string) {
	// title/detail 可能含昵称、上游错误文本、query 参数 —— 一律转义后再拼进 HTML，
	// 否则回调页会成为反射型/存储型 XSS 的落点。
	title = html.EscapeString(title)
	detail = html.EscapeString(detail)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	// 回调 URL 的 query 里带着 refreshToken/userInfo：绝不能通过 Referer 泄漏给第三方。
	w.Header().Set("Referrer-Policy", "no-referrer")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("X-Frame-Options", "DENY")
	w.WriteHeader(status)
	// 极简内联 HTML，无外部依赖；样式 token 与 admin.html 接近。
	html := `<!DOCTYPE html><html lang="zh-CN"><head><meta charset="utf-8">` +
		`<meta name="viewport" content="width=device-width,initial-scale=1">` +
		`<title>trae2api-web 登录</title><style>` +
		`body{background:#0f1115;color:#e6e9ef;font-family:"Segoe UI","Microsoft YaHei",system-ui,sans-serif;` +
		`display:flex;align-items:center;justify-content:center;min-height:100vh;margin:0}` +
		`.box{max-width:480px;padding:32px;text-align:center}` +
		`h1{font-size:20px;font-weight:600;margin:0 0 12px}` +
		`p{color:#8a93a6;line-height:1.6;margin:0 0 8px;word-break:break-all}` +
		`</style></head><body><div class="box"><h1>` + title + `</h1><p>` + detail + `</p>` +
		`<p style="margin-top:20px;font-size:12px">窗口可关闭并返回 trae2api-web 控制台。</p>` +
		`<script>try{setTimeout(function(){window.close()},3000);}catch(e){}</script>` +
		`</div></body></html>`
	_, _ = w.Write([]byte(html))
}

// newRequestID 生成 16 hex 的请求 ID（无第三方依赖）。
func newRequestID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "req-unknown"
	}
	return hex.EncodeToString(b)
}

// adminClientIP 取管理端请求的来源 IP：优先 X-Forwarded-For 首段，其次 X-Real-IP。
// 只用于限流与审计日志，不做信任边界判定（真正的边界是 Bearer/cookie 鉴权）。
// 与 chat 路径的 chatClientIP 刻意分开：面板模块不依赖业务模块的内部工具函数。
func adminClientIP(r *http.Request) string {
	if v := r.Header.Get("X-Forwarded-For"); v != "" {
		if i := strings.IndexByte(v, ','); i >= 0 {
			v = v[:i]
		}
		if v = strings.TrimSpace(v); v != "" {
			return v
		}
	}
	if v := strings.TrimSpace(r.Header.Get("X-Real-IP")); v != "" {
		return v
	}
	host := r.RemoteAddr
	if i := strings.LastIndexByte(host, ':'); i > 0 {
		host = host[:i]
	}
	return strings.Trim(host, "[]")
}
