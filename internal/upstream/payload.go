// payload.go OpenAI → SOLO llm_utils_chat 请求体改写。
package upstream

import (
	"encoding/json"
	"log"
	"strings"
)

// PrepareBody 单 pass 改写；无法解析时原样返回。
//
//	OpenAI: {model, messages, stream, tools, tool_choice, ...}
//	SOLO:   {messages, function:"solo_work_lite", stream:true,
//	         config_name:<model>, model:<model>}
//
// 改写规则（SPEC §4.4）：
//  1. messages: content 字符串 → [{"type":"text","text":...}]；已是数组 → 透传
//  2. stream: 强制 true（非流式由服务端聚合）
//  3. model → config_name + model
//  4. function: 固定 "solo_work_lite"
//  5. tools/tool_choice: 归一化（"none" 删 tools；auto/required 保留；function 提取 name）
func PrepareBody(src []byte) []byte {
	if len(src) == 0 {
		return src
	}
	var obj map[string]any
	if err := json.Unmarshal(src, &obj); err != nil {
		return src
	}
	obj["stream"] = true
	obj["function"] = Function

	// max_completion_tokens 是 OpenAI 新字段（o1/reasoning 模型），上游只认
	// max_tokens。两者都给时以 max_completion_tokens 为准（OpenAI 语义：新字段
	// 覆盖旧字段）。保留 max_tokens 会让上游按旧值截断，导致「同样的 max_tokens
	// 在别家网关能用、这里提前 stop」。
	if v, ok := obj["max_completion_tokens"]; ok {
		obj["max_tokens"] = v // 新字段覆盖旧字段
		delete(obj, "max_completion_tokens")
	}
	// stream_options 是 OpenAI 的下游语义（是否回传 usage），由本服务处理；
	// 上游 SOLO 不认这个字段，转发过去可能被判非法参数，必须剥掉。
	delete(obj, "stream_options")
	// 同理：这些是 OpenAI 侧的参数，上游 llm_utils_chat 不接受。
	delete(obj, "stream_options_include_usage")

	if msgs, ok := obj["messages"].([]any); ok {
		for _, mi := range msgs {
			m, ok := mi.(map[string]any)
			if !ok {
				continue
			}
			content, present := m["content"]
			role, _ := m["role"].(string)

			// assistant 消息回传 tool_calls: OpenAI function → 上游 function_call
			if role == "assistant" {
				if tcs, ok := m["tool_calls"].([]any); ok {
					kept := make([]any, 0, len(tcs))
					for _, tci := range tcs {
						tc, ok := tci.(map[string]any)
						if !ok {
							continue
						}
						// OpenAI: function{name, arguments} → 上游 SOLO: function_call{name, arguments}
						if fn, ok := tc["function"].(map[string]any); ok {
							tc["function_call"] = fn
							delete(tc, "function")
						}
						// 上游要求 FunctionCall.Name 必填: 无 name 的 tool_call 剔除
						if fc, ok := tc["function_call"].(map[string]any); ok {
							name, _ := fc["name"].(string)
							if strings.TrimSpace(name) == "" {
								continue
							}
						}
						kept = append(kept, tc)
					}
					if len(kept) == 0 {
						delete(m, "tool_calls")
					} else {
						m["tool_calls"] = kept
					}
				}
			}

			if !present || content == nil {
				continue // 无 content 的消息(如纯 tool_calls assistant 已转换完)保留字段但跳过 content 改写
			}
			switch c := content.(type) {
			case string:
				m["content"] = []any{
					map[string]any{"type": "text", "text": c},
				}
			default:
				// 已是数组 → 透传（兼容多模态，未实测，保守透传）
			}
		}
	}

	model, _ := obj["model"].(string)
	model = strings.TrimSpace(model)
	if model == "" {
		model = DefaultConfigName
	}
	obj["config_name"] = model
	obj["model"] = model

	normalizeToolChoice(obj)
	normalizeTools(obj)
	dropUnsupported(obj)
	out, err := json.Marshal(obj)
	if err != nil {
		return src
	}
	return out
}

// DefaultConfigName 默认模型（glm-5.2，实测可用）。
const DefaultConfigName = "glm-5.2"

// normalizeToolChoice 按上游 Go struct（string 类型）改写 OpenAI tool_choice。
//   - "none" / {"type":"none"} → 删 tool_choice + 删 tools/functions
//   - {"type":"auto"/"required"} → 字符串 "auto"/"required"
//   - {"type":"function","function":{"name":"x"}} → 字符串 "x"
//   - 其他对象/非标量 → 删 tool_choice
func normalizeToolChoice(obj map[string]any) {
	suppress := func() {
		delete(obj, "tools")
		delete(obj, "functions")
	}
	tc, present := obj["tool_choice"]
	if !present {
		return
	}
	switch v := tc.(type) {
	case string:
		if strings.EqualFold(strings.TrimSpace(v), "none") {
			delete(obj, "tool_choice")
			suppress()
		}
	case map[string]any:
		typ, _ := v["type"].(string)
		typ = strings.ToLower(strings.TrimSpace(typ))
		switch typ {
		case "none":
			delete(obj, "tool_choice")
			suppress()
		case "auto", "required":
			obj["tool_choice"] = typ
		case "function":
			name := ""
			if fn, ok := v["function"].(map[string]any); ok {
				name, _ = fn["name"].(string)
			}
			if name == "" {
				name, _ = v["name"].(string)
			}
			if name = strings.TrimSpace(name); name != "" {
				obj["tool_choice"] = name
			} else {
				obj["tool_choice"] = "auto"
			}
		default:
			delete(obj, "tool_choice")
		}
	default:
		delete(obj, "tool_choice")
	}
}

// normalizeTools 把 OpenAI tools 转为 SOLO 上游格式。
// 实测上游 Go struct: FunctionDefinition.tools[].function.parameters 是 string 类型
// （OpenAI 标准是 object）→ 需把 parameters 对象序列化为 JSON 字符串。
// 同时 tools 条目若不是 map 或缺 function，整体剔除（避免上游反序列化失败）。
func normalizeTools(obj map[string]any) {
	raw, present := obj["tools"]
	if !present {
		return
	}
	list, ok := raw.([]any)
	if !ok || len(list) == 0 {
		return
	}
	out := make([]any, 0, len(list))
	for _, item := range list {
		t, ok := item.(map[string]any)
		if !ok {
			continue
		}
		fn, ok := t["function"].(map[string]any)
		if !ok {
			continue
		}
		if params, ok := fn["parameters"]; ok {
			if paramsMap, isMap := params.(map[string]any); isMap {
				if s, err := json.Marshal(paramsMap); err == nil {
					fn["parameters"] = string(s)
				}
			}
		}
		out = append(out, t)
	}
	if len(out) == 0 {
		delete(obj, "tools")
		return
	}
	obj["tools"] = out
}

// unsupportedFields 上游 llm_utils_chat 不接受、且我们也没做映射的 OpenAI 参数。
//
// 为什么选择「丢弃而不是透传」：SOLO 的 Go struct 对未知字段是严格解码，
// 多一个字段整条请求直接 4xx（实测），客户端会看到莫名其妙的参数错误。
// 为什么选择「丢弃而不是假装支持」：这些字段语义（多候选、对数概率、随机种子、
// 强制并行工具调用、结构化输出）在上游完全没有对应实现，静默假装支持会让
// 客户端拿到不符合 request 语义的结果。折中是：丢弃 + 打一行日志，让使用者
// 知道「这次请求的这几个参数没生效」，而不是无声无息地偏离。
var unsupportedFields = []string{
	"n", "logprobs", "top_logprobs", "logit_bias", "seed",
	"parallel_tool_calls", "response_format", "service_tier",
	"store", "metadata", "moderation", "prediction", "audio", "web_search_options",
}

// dropUnsupported 删除上游不支持的可选参数，并记录被忽略的字段名。
func dropUnsupported(obj map[string]any) {
	var dropped []string
	for _, k := range unsupportedFields {
		if _, ok := obj[k]; ok {
			delete(obj, k)
			dropped = append(dropped, k)
		}
	}
	if len(dropped) > 0 {
		// 用标准 log 而非 fmt：这行是运维排查依据，必须进容器日志。
		log.Printf("INFO: [upstream] ignored unsupported params: %v", dropped)
	}
}
