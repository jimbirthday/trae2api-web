// redact.go 出站错误文案脱敏。
//
// 为什么放在「写响应」的最底层而不是每个调用点：
// 上游错误几乎都是 net/http 的 *url.Error，Error() 形如
//
//	Post "https://trae-api-cn.mchost.guru/api/agent/v3/llm_utils_chat": ...
//
// 只要有任何一处忘记包一层，客户端就会看到内网/上游域名。调用点有十几处且
// 以后还会新增，靠人工 review 不可持续；统一在 writeJSON 前的最后一道口子
// 做替换，才能保证「新增代码默认是安全的」。
package server

import (
	"regexp"
	"strings"
)

var (
	// URL：http(s)://host[:port]/path?query —— 整段替换，连 query 里的签名一起去掉。
	reRedactURL = regexp.MustCompile(`(?i)\bhttps?://[^\s"'<>\)\]]+`)
	// 裸 host:port（如 trae-api-cn.mchost.guru:443），没有 scheme 的情况。
	reRedactHostPort = regexp.MustCompile(`(?i)\b[a-z0-9][a-z0-9.-]*\.(?:guru|cn|com|net|io|internal|local)(?::\d{1,5})?\b`)
	// net/http 的 `Post "…"` / `Get "…"` 前缀，替换 URL 后留着会很怪。
	reRedactMethodPrefix = regexp.MustCompile(`(?i)\b(?:Post|Get|Put|Delete|Head|Do)\s+"<upstream>"`)
	// 裸 IP：`dial tcp 10.255.255.1:443` 这类错误里没有 scheme 也没有域名，
	// 前两条正则都抓不到。内网 IP 同样是实现细节。
	//
	// 刻意分成两条而不是一条宽松的「任意四段数字」：上游版本号/模型 id 里常有
	// `1.0.0.1229` 这种形态，宽松匹配会把错误文案改成 `version <upstream>`，
	// 反而让运维看不懂。所以内网段允许不带端口，其它 IP 必须带端口才算地址。
	reRedactPrivateIP  = regexp.MustCompile(`\b(?:10|127|0)\.\d{1,3}\.\d{1,3}\.\d{1,3}\b|\b192\.168\.\d{1,3}\.\d{1,3}\b|\b172\.(?:1[6-9]|2\d|3[01])\.\d{1,3}\.\d{1,3}\b|\b169\.254\.\d{1,3}\.\d{1,3}\b`)
	reRedactIPWithPort = regexp.MustCompile(`\b\d{1,3}(?:\.\d{1,3}){3}:\d{1,5}\b`)
	reRedactIPv6       = regexp.MustCompile(`\[[0-9a-fA-F:]{2,}\](?::\d{1,5})?`)
)

// redactUpstream 抹掉文本里可能出现的上游地址/主机名。
//
// 只做「去掉实现细节」，不做截断（截断仍由 sanitizeErrMsg 负责）；对不含地址
// 的普通文案是恒等变换，因此可以无脑套在每一条出站消息上。
//
// 注意：**只能用在错误文案上**，绝不能套在模型输出正文上 —— 用户让模型写的
// 内容里出现 URL 是正常的，脱敏会把正常回复改坏。
func redactUpstream(s string) string {
	if s == "" {
		return s
	}
	// 顺序有讲究：先替 URL（把整段 path/query 吃掉），再删 `Post "<upstream>"`
	// 前缀，否则前缀正则匹配不到还是原样的 URL。
	s = reRedactURL.ReplaceAllString(s, "<upstream>")
	s = reRedactMethodPrefix.ReplaceAllString(s, "")
	s = reRedactHostPort.ReplaceAllString(s, "<upstream>")
	s = reRedactIPv6.ReplaceAllString(s, "<upstream>")
	s = reRedactIPWithPort.ReplaceAllString(s, "<upstream>")
	s = reRedactPrivateIP.ReplaceAllString(s, "<upstream>")
	// 抹掉地址后常留下悬空的分隔符（`Post "<upstream>": dial ...` → `: dial ...`），
	// 读起来像坏数据。只清理**开头**的分隔符，正文里的冒号有语义不能动。
	s = strings.TrimLeft(s, ":,;- ")
	return s
}
