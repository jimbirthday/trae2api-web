// 面板鉴权门禁：/admin 页面与 /admin/api/* 都必须持有有效凭证才能访问。
//
// 两种凭证等价，都校验同一个 TW2A_API_KEY：
//
//  1. Authorization: Bearer <key>  —— 脚本/curl 用
//  2. Cookie tw2a_admin=<derived>  —— 浏览器用，由 POST /admin/api/session 设置
//
// Cookie 里存的**不是明文 key**，而是 sha256(key|salt) 的派生值：即使 cookie
// 泄漏也无法反推 key，且 key 轮换后旧 cookie 自动失效。Cookie 设为 HttpOnly
// （JS 读不到）+ SameSite=Strict（阻断跨站请求携带）+ Path=/admin。
//
// TW2A_API_KEY 为空时不设门禁（本地无 key 场景保持可用）。
package server

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	adminCookieName = "tw2a_admin"
	adminCookiePath = "/admin"
	// adminCookieMaxAge 会话有效期（秒）。改 key 会立刻让所有旧 cookie 失效。
	adminCookieMaxAge = 7 * 24 * 3600
)

// adminCookieValue 由 API Key 派生 cookie 值（不可逆）。
func (h *Handler) adminCookieValue() string {
	sum := sha256.Sum256([]byte(h.cfg.APIKey + "|tw2a-admin-v1"))
	return hex.EncodeToString(sum[:])
}

// adminAuthorized 校验请求是否持有有效凭证（Bearer 或 cookie）。
func (h *Handler) adminAuthorized(r *http.Request) bool {
	if h.cfg.APIKey == "" {
		return true // 未配置 key：不设门禁
	}
	if h.bearerOK(r) {
		return true
	}
	if c, err := r.Cookie(adminCookieName); err == nil && c.Value != "" {
		return subtle.ConstantTimeCompare([]byte(c.Value), []byte(h.adminCookieValue())) == 1
	}
	return false
}

// bearerOK 校验 Authorization: Bearer <key>。
func (h *Handler) bearerOK(r *http.Request) bool {
	authz := r.Header.Get("Authorization")
	const prefix = "Bearer "
	if len(authz) < len(prefix) || !strings.EqualFold(authz[:len(prefix)], prefix) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(authz[len(prefix):]), []byte(h.cfg.APIKey)) == 1
}

// adminSession POST /admin/api/session：用 key 换 cookie（浏览器登录面板）。
// 本身不能挂在 withAdminAuth 后面（否则先有鸡还是先有蛋）。
func (h *Handler) adminSession(w http.ResponseWriter, r *http.Request) {
	if h.cfg.APIKey == "" {
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "note": "未配置 API Key，无需登录"})
		return
	}
	var req struct {
		Key string `json:"key"`
	}
	body, _ := io.ReadAll(io.LimitReader(r.Body, 8192))
	_ = json.Unmarshal(body, &req)
	if req.Key == "" {
		req.Key = r.URL.Query().Get("key")
	}
	// 防爆破：同一来源连续失败 5 次锁 60 秒。key 是唯一凭证，必须限速。
	ip := adminClientIP(r)
	if wait, locked := h.loginLimiter.blocked(ip); locked {
		w.Header().Set("Retry-After", strconv.Itoa(int(wait.Seconds())+1))
		writeOpenAIError(w, http.StatusTooManyRequests, "rate_limit_exceeded",
			"登录失败次数过多，请 "+strconv.Itoa(int(wait.Seconds())+1)+" 秒后重试")
		return
	}
	if subtle.ConstantTimeCompare([]byte(strings.TrimSpace(req.Key)), []byte(h.cfg.APIKey)) != 1 {
		h.loginLimiter.fail(ip)
		writeOpenAIError(w, http.StatusUnauthorized, "invalid_api_key", "key 不正确")
		return
	}
	h.loginLimiter.reset(ip)
	http.SetCookie(w, &http.Cookie{
		Name:     adminCookieName,
		Value:    h.adminCookieValue(),
		Path:     adminCookiePath,
		MaxAge:   adminCookieMaxAge,
		HttpOnly: true,
		Secure:   r.TLS != nil,
		SameSite: http.SameSiteStrictMode,
	})
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// adminLogout DELETE /admin/api/session：清除 cookie。
func (h *Handler) adminLogout(w http.ResponseWriter, r *http.Request) {
	http.SetCookie(w, &http.Cookie{
		Name:     adminCookieName,
		Value:    "",
		Path:     adminCookiePath,
		MaxAge:   -1,
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
	})
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// adminLoginHTML 未登录时返回的极简登录页。不引用任何外部资源，
// 也不包含面板的任何数据，避免未授权信息泄漏。
func adminLoginHTML(w http.ResponseWriter, status int, errMsg string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	errBlock := ""
	if errMsg != "" {
		errBlock = `<p class="err">` + htmlEscape(errMsg) + `</p>`
	}
	page := `<!DOCTYPE html><html lang="zh-CN"><head><meta charset="utf-8">` +
		`<meta name="viewport" content="width=device-width,initial-scale=1">` +
		`<title>trae2api-web 登录</title><style>` +
		`:root{--bg:#0f1115;--panel:#171a21;--line:#252a35;--txt:#e6e9ef;--dim:#8a93a6;--acc:#4c8dff;--bad:#ff6b6b}` +
		`*{box-sizing:border-box}` +
		`body{background:var(--bg);color:var(--txt);margin:0;min-height:100vh;display:flex;` +
		`align-items:center;justify-content:center;font-family:"Segoe UI","Microsoft YaHei",system-ui,sans-serif}` +
		`form{background:var(--panel);border:1px solid var(--line);border-radius:12px;padding:28px;width:340px}` +
		`h1{font-size:17px;font-weight:600;margin:0 0 6px}` +
		`p.sub{color:var(--dim);font-size:12px;margin:0 0 18px;line-height:1.6}` +
		`input{width:100%;padding:10px 12px;background:#0f1115;border:1px solid var(--line);` +
		`border-radius:8px;color:var(--txt);font-size:13px;outline:none}` +
		`input:focus{border-color:var(--acc)}` +
		`button{width:100%;margin-top:12px;padding:10px;background:var(--acc);border:0;border-radius:8px;` +
		`color:#fff;font-size:13px;font-weight:600;cursor:pointer}` +
		`button:disabled{opacity:.6;cursor:default}` +
		`p.err{color:var(--bad);font-size:12px;margin:10px 0 0}` +
		`label.rem{display:flex;align-items:center;gap:6px;color:var(--dim);font-size:12px;margin-top:10px}` +
		`label.rem input{width:auto;margin:0}` +
		`</style></head><body><form id="f">` +
		`<h1>trae2api-web 控制台</h1>` +
		`<p class="sub">需要 TW2A_API_KEY 才能查看面板。</p>` +
		`<input id="k" type="password" placeholder="输入 API Key" autofocus autocomplete="current-password">` +
		`<label class="rem"><input id="rm" type="checkbox"> 记住（存本地，明文）</label>` +
		`<button id="b" type="submit">进入</button>` + errBlock +
		`</form><script>` +
		`document.getElementById('f').addEventListener('submit',async function(e){` +
		`e.preventDefault();var b=document.getElementById('b');var k=document.getElementById('k').value.trim();` +
		`if(!k){return}b.disabled=true;b.textContent='验证中...';` +
		`try{var r=await fetch('/admin/api/session',{method:'POST',headers:{'Content-Type':'application/json'},` +
		`body:JSON.stringify({key:k})});` +
		// 默认只写 sessionStorage（关标签页即失效）。勾选「记住」才落 localStorage：
		// API Key 是明文存本地，默认持久化会把凭据长期留在磁盘上，与 XSS 叠加风险更大。
		`if(r.ok){sessionStorage.setItem('tw2a_admin_key',k);` +
		`if(document.getElementById('rm').checked){localStorage.setItem('tw2a_admin_key',k)}` +
		`else{localStorage.removeItem('tw2a_admin_key')}location.reload();return}` +
		`var d=await r.json().catch(function(){return{}});show((d.error&&d.error.message)||'Key 不正确');` +
		`}catch(x){show('网络错误：'+x.message)}` +
		`function show(m){b.disabled=false;b.textContent='进入';var p=document.querySelector('.err');` +
		`if(!p){p=document.createElement('p');p.className='err';document.getElementById('f').appendChild(p)}p.textContent=m}` +
		`});` +
		// 回填：localStorage 里已有 key 说明上次勾了「记住」，把复选框与输入框都还原。
		`(function(){var d=localStorage.getItem('tw2a_admin_key');if(d){` +
		`document.getElementById('rm').checked=true;document.getElementById('k').value=d}else{` +
		`var s=sessionStorage.getItem('tw2a_admin_key');if(s){document.getElementById('k').value=s}}})();` +
		`</script></body></html>`
	_, _ = w.Write([]byte(page))
}

func htmlEscape(s string) string {
	r := strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;", `"`, "&quot;", "'", "&#39;")
	return r.Replace(s)
}

// ---------------------------------------------------------------------------
// 登录爆破限流（内存实现：单实例足够；多实例部署应换成共享存储）
// ---------------------------------------------------------------------------

const (
	loginFailLimit   = 5                // 连续失败阈值
	loginFailLockFor = 60 * time.Second // 锁定时长
	loginFailTTL     = 10 * time.Minute // 计数条目过期（防 map 无限增长）
)

type loginFailEntry struct {
	fails  int
	until  time.Time
	lastAt time.Time
}

type loginLimiter struct {
	mu      sync.Mutex
	entries map[string]*loginFailEntry
}

// blocked 报告来源是否处于锁定中，并返回剩余等待时间。
func (l *loginLimiter) blocked(ip string) (time.Duration, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	e := l.entries[ip]
	if e == nil {
		return 0, false
	}
	if now := time.Now(); now.Before(e.until) {
		return e.until.Sub(now), true
	}
	return 0, false
}

// fail 记一次失败；达到阈值后进入锁定窗口。
func (l *loginLimiter) fail(ip string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.entries == nil {
		l.entries = make(map[string]*loginFailEntry)
	}
	now := time.Now()
	e := l.entries[ip]
	if e == nil || now.Sub(e.lastAt) > loginFailTTL {
		e = &loginFailEntry{}
		l.entries[ip] = e
	}
	e.fails++
	e.lastAt = now
	if e.fails >= loginFailLimit {
		e.until = now.Add(loginFailLockFor)
		e.fails = 0
	}
	l.sweepLocked(now)
}

// reset 登录成功后清空该来源的失败计数。
func (l *loginLimiter) reset(ip string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.entries, ip)
}

// sweepLocked 清理过期条目（每 10 分钟量级，条目数很少，开销可忽略）。
func (l *loginLimiter) sweepLocked(now time.Time) {
	if len(l.entries) < 256 {
		return
	}
	for k, e := range l.entries {
		if now.Sub(e.lastAt) > loginFailTTL && now.After(e.until) {
			delete(l.entries, k)
		}
	}
}
