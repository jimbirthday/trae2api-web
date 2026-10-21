// Package logfmt 统一日志里的 uid 截断、账号标签与显示宽度对齐。
//
// 约定：
//   - uid 统一截 8 位：日志行只留 uid 前 8 位，全量 uid 可从 data/state.json 查。
//   - 账号标签形如 "昵称(a1b2c3d4)"：昵称认人、uid8 供 grep。
//
// 本包不引入日志库，也不引第三方依赖，只提供纯字符串 helper：
// Truncate / UID8 / Label / DisplayWidth / Pad / ShortUA。
package logfmt

import (
	"strings"
	"unicode/utf8"
)

// Truncate 截断字符串到 n 字节上限（先 TrimSpace），切点落在多字节字符中间时
// 回退到 UTF-8 rune 边界——错误 body 多为中文，按字节切会出半截序列乱码。
// 短于 n 原样返回；n<=0 返回空串。
func Truncate(s string, n int) string {
	if n <= 0 {
		return ""
	}
	s = strings.TrimSpace(s)
	if len(s) > n {
		// s[n] 是切点后的首字节：是 rune 的后续字节（continuation）说明切点落在
		// 多字节字符中间，逐字节回退到 rune 边界（该字符整个让出）。
		for n > 0 && !utf8.RuneStart(s[n]) {
			n--
		}
		return s[:n]
	}
	return s
}

// UID8 返回 uid 的前 8 位；空 uid 返回 "-"。
func UID8(uid string) string {
	if uid == "" {
		return "-"
	}
	if len(uid) > 8 {
		return uid[:8]
	}
	return uid
}

// Label 返回日志里的账号标签，形如 "示例昵称甲(a1b2c3d4)"；昵称为空时退回 "a1b2c3d4"。
// uid 与 nick 同时为空时返回 "-"（与 UID8 口径一致）。
func Label(uid, nick string) string {
	short := UID8(uid)
	nick = strings.TrimSpace(nick)
	if nick == "" {
		return short
	}
	return nick + "(" + short + ")"
}

// DisplayWidth 返回 s 的终端显示列宽：CJK / 全角 / emoji 记 2 列，其余记 1 列。
//
// 账号昵称是用户自定的中文（"猫" 是 3 字节但占 2 列），用字节数做表格对齐会错位。
// 标准库没有显示宽度函数，本仓库不引 go-runewidth，故内置这份常见宽字符区段判定。
func DisplayWidth(s string) int {
	w := 0
	for _, r := range s {
		w += runeWidth(r)
	}
	return w
}

// Pad 把 s 右补空格到 width 显示列宽；已超宽或 width<=0 时原样返回（不截断）。
func Pad(s string, width int) string {
	if width <= 0 {
		return s
	}
	if d := width - DisplayWidth(s); d > 0 {
		return s + strings.Repeat(" ", d)
	}
	return s
}

// maxShortUALen ShortUA 的返回上限（足够放下 "curl/8.4.0"、"python-requests/2.31.0"）。
const maxShortUALen = 40

// shortUAEngines UA 里只说明渲染引擎、不说明"是什么客户端"的通用 token。
var shortUAEngines = map[string]bool{
	"mozilla": true, "applewebkit": true, "gecko": true, "khtml": true,
	"like": true, "safari": true, "compatible": true, "msie": true, "trident": true,
}

// ShortUA 从 User-Agent 提取便于人眼识别的客户端标签（"curl/8.4.0"、"Chrome/120.0.0.0"）。
// 规则：取第一个形如 name/version 且 name 不是渲染引擎的 token；没有则回落整串前
// maxShortUALen 字节（纯产品名 UA，如 "node"）。空 UA 返回空串。
func ShortUA(ua string) string {
	ua = strings.TrimSpace(ua)
	if ua == "" {
		return ""
	}
	for _, tok := range strings.Fields(ua) {
		tok = strings.Trim(tok, "(),;")
		name, _, ok := strings.Cut(tok, "/")
		if !ok || name == "" {
			continue
		}
		if shortUAEngines[strings.ToLower(name)] {
			continue
		}
		return Truncate(tok, maxShortUALen)
	}
	return Truncate(ua, maxShortUALen)
}

// runeWidth 单个 rune 的显示列宽。区段判定取自 Unicode East Asian Width 的
// Wide/Fullwidth 集合，只保留实际会用到的段。
func runeWidth(r rune) int {
	switch {
	case r == 0:
		return 0
	case r < 0x20 || (r >= 0x7f && r < 0xa0):
		// 控制字符（含 DEL/C1）不占位。
		return 0
	case r < 0x1100:
		return 1
	case r <= 0x115f: // Hangul Jamo 初声
		return 2
	case r == 0x2329 || r == 0x232a:
		return 2
	case r >= 0x2e80 && r <= 0xa4cf && r != 0x303f: // CJK 部首…Yi（303f 是窄字符）
		return 2
	case r >= 0xac00 && r <= 0xd7a3: // Hangul 音节
		return 2
	case r >= 0xf900 && r <= 0xfaff: // CJK 兼容表意
		return 2
	case r >= 0xfe30 && r <= 0xfe6f: // CJK 兼容形式
		return 2
	case r >= 0xff00 && r <= 0xff60: // 全角 ASCII
		return 2
	case r >= 0xffe0 && r <= 0xffe6: // 全角符号
		return 2
	case r >= 0x1f300 && r <= 0x1f9ff: // emoji
		return 2
	case r >= 0x20000 && r <= 0x3fffd: // CJK 扩展 B 及以后
		return 2
	}
	return 1
}
