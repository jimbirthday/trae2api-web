// usage.go 把上游 SOLO 的 token_usage 归一化成 OpenAI 标准 usage 结构。
//
// 上游给的字段名是 Anthropic 风格（cache_read_input_tokens / cache_creation_input_tokens /
// reasoning_tokens）且夹杂大量内部计数器（*_total / cluster / name）。OpenAI 标准只认：
//
//	{"prompt_tokens":N,"completion_tokens":N,"total_tokens":N,
//	 "prompt_tokens_details":{"cached_tokens":N},
//	 "completion_tokens_details":{"reasoning_tokens":N}}
//
// 直接把上游结构透出去会有两个后果：严格按 schema 校验的 SDK 会报未知字段，
// 而按 OpenAI 字段名读缓存的客户端（多数网关、计费统计）永远读到 0。
package upstream

// normalizeUsage 把上游 usage map 转成 OpenAI 标准结构。
// 输入为 nil 时返回 nil（调用方据此决定是否下发 usage）。
func normalizeUsage(raw map[string]any) map[string]any {
	if raw == nil {
		return nil
	}
	num := func(k string) (int64, bool) { return numField(raw, k) }

	prompt, hasPrompt := num("prompt_tokens")
	completion, hasCompletion := num("completion_tokens")
	total, hasTotal := num("total_tokens")
	if !hasPrompt && !hasCompletion && !hasTotal {
		return nil // 一条用量信息都没有：不要下发全 0 的假数据
	}
	if !hasTotal || total == 0 {
		total = prompt + completion
	}

	out := map[string]any{
		"prompt_tokens":     prompt,
		"completion_tokens": completion,
		"total_tokens":      total,
	}

	// 缓存命中：OpenAI 放在 prompt_tokens_details.cached_tokens。
	// prompt_tokens 已包含命中部分，所以 cached_tokens 不得重复计入 total。
	if cached, ok := num("cache_read_input_tokens"); ok && cached > 0 {
		out["prompt_tokens_details"] = map[string]any{"cached_tokens": cached}
	}
	// 思考链 token 已含在 completion_tokens 内，只做明细展示。
	if reasoning, ok := num("reasoning_tokens"); ok && reasoning > 0 {
		out["completion_tokens_details"] = map[string]any{"reasoning_tokens": reasoning}
	}
	return out
}
