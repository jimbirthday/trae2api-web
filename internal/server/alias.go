// alias.go 客户端模型别名 → 上游 config_name 的一级映射。
//
// 为什么需要这个文件：客户端（IDE 插件、桌面端）会把模型名写死在配置里，
// 而上游 SOLO 的 config_name 是随版本变化的内部名（custom_model_x / glm-5.2）。
// 有了别名表，用户可以在面板里把 "my-gpt" 指到 "glm-5.2"，不必改客户端。
package server

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

// resolveModelAlias 按 Config.ModelAliases 做**一次**（且仅一次）名称映射。
//
// 三条设计约束，缺一条都会出事故：
//  1. 不递归：只跳一级。否则 A→B→C 链式解析会让「改了别名却看不出为什么
//     请求跑到别的模型上」，而 A→B→A 这类环在递归实现里会死循环。
//  2. 静态表优先：客户端传的名字如果本身就是合法模型 id（glm-5.2 等），
//     一律不走别名。别名是为了给上游模型起别名，绝不能反过来把真实模型名
//     改写掉——那会让「显式指定模型」失效。
//  3. 只影响转发目标，不影响响应里的 model：响应仍回显客户端原始名
//     （chat.go 的 providerName 已保证），否则客户端按 model 分支的逻辑会错乱。
func (h *Handler) resolveModelAlias(model string) string {
	model = strings.TrimSpace(model)
	aliases := h.aliasesSnapshot()
	if model == "" || len(aliases) == 0 {
		return model
	}
	if _, ok := staticModelMatch(model); ok {
		return model
	}
	if v, ok := aliases[model]; ok {
		if s := strings.TrimSpace(v); s != "" {
			return s
		}
	}
	// 大小写不敏感兜底：OpenAI 客户端常把模型名规范化后再发（如 MY-GPT）。
	// 遍历顺序不影响结果——同一个键只可能大小写折叠出一次命中。
	for k, v := range aliases {
		if strings.EqualFold(k, model) {
			if s := strings.TrimSpace(v); s != "" {
				return s
			}
			break
		}
	}
	return model
}

// aliasTargets 返回别名表里「不是合法静态 id」的目标列表，用于 /v1/models
// 之外的诊断（面板/日志排查「别名指向了一个不存在的模型」）。
func (h *Handler) aliasTargets() []string {
	aliases := h.aliasesSnapshot()
	if len(aliases) == 0 {
		return nil
	}
	out := make([]string, 0, len(aliases))
	seen := map[string]bool{}
	for _, v := range aliases {
		v = strings.TrimSpace(v)
		if v == "" || seen[v] {
			continue
		}
		seen[v] = true
		if _, ok := staticModelMatch(v); !ok {
			out = append(out, v)
		}
	}
	return out
}

// aliasEnvKey 别名表的兜底来源：TW2A_MODEL_ALIASES="my-gpt=glm-5.2,fast=kimi-k3"。
//
// 为什么还要 env：config.json 的读取在 cmd/server（本文件无权改动），而别名
// 又必须能落地配置。env 让功能立刻可用，且与其它 TW2A_* 开关风格一致。
// 优先级：Config.ModelAliases（程序内注入）> env。
func aliasEnvKey() string { return "TW2A_MODEL_ALIASES" }

// loadModelAliasesEnv 解析 "alias=target,alias2=target2"。
// 非法片段只打日志、跳过，绝不让一个手误的逗号导致服务起不来。
func loadModelAliasesEnv() map[string]string {
	raw := strings.TrimSpace(os.Getenv(aliasEnvKey()))
	if raw == "" {
		return nil
	}
	out := map[string]string{}
	for _, pair := range strings.Split(raw, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		k, v, ok := strings.Cut(pair, "=")
		k, v = strings.TrimSpace(k), strings.TrimSpace(v)
		if !ok || k == "" || v == "" {
			log.Printf("[alias] 忽略非法别名项 %q（期望 alias=model）", pair)
			continue
		}
		if strings.EqualFold(k, v) {
			// 自映射是环的最简形式：留着只会在 resolveModelAlias 里白跑一趟。
			continue
		}
		out[k] = v
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// ---------------------------------------------------------------------------
// 别名的三条来源与面板读写
// ---------------------------------------------------------------------------
//
// 优先级（高 → 低）：Config.ModelAliases（程序注入）> config.json 的
// model_aliases > TW2A_MODEL_ALIASES。
//
// 为什么要自己读 config.json：cmd/server 的配置结构体（本 Agent 无权改动）
// 没有 model_aliases 字段，配置不会自动透传进来。别名是纯路由层面的东西，
// 由 server 包自己加载既能立刻可用，也不必让 cmd 层为它增加一个字段。

// aliasesMu 保护 h.cfg.ModelAliases 的运行时替换（面板保存后立即生效）。
// 不能只靠「启动时读一次」：改完别名要重启服务才能验证，体验太差，
// 而且面板显示的值会和实际生效的值不一致。
func (h *Handler) aliasesSnapshot() map[string]string {
	h.aliasesMu.RLock()
	defer h.aliasesMu.RUnlock()
	if len(h.cfg.ModelAliases) == 0 {
		return nil
	}
	out := make(map[string]string, len(h.cfg.ModelAliases))
	for k, v := range h.cfg.ModelAliases {
		out[k] = v
	}
	return out
}

// setAliases 替换运行时的别名表（面板保存路径调用）。
func (h *Handler) setAliases(m map[string]string) {
	h.aliasesMu.Lock()
	defer h.aliasesMu.Unlock()
	h.cfg.ModelAliases = m
}

// aliasStorePath 别名表的**持久化兜底文件**（data/model_aliases.json）。
//
// 为什么需要它：Docker 部署时 config.json 常常没被挂载（compose 默认只挂
// auths/ 与 data/），此时写 config.json 只落在容器可写层——容器一重建
// 别名就没了，但面板还显示「已保存」。把副本写进 data/ 目录（该目录默认
// 挂载到宿主）才能真正持久化。
func (h *Handler) aliasStorePath() string { return aliasStorePathFor(h.cfg.ConfigPath) }

// aliasStorePathFor 纯函数版本：NewHandler 构造期还没有 Handler 实例可用，
// 但同样需要算出兜底文件路径来加载别名。
func aliasStorePathFor(configPath string) string {
	base := strings.TrimSpace(configPath)
	if base == "" {
		return ""
	}
	return filepath.Join(filepath.Dir(base), "data", "model_aliases.json")
}

// loadModelAliasesStore 从兜底文件读别名表；文件不存在属正常情况。
func loadModelAliasesStore(path string) map[string]string {
	if strings.TrimSpace(path) == "" {
		return nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var doc struct {
		Aliases map[string]string `json:"aliases"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		log.Printf("[alias] 解析 %s 失败：%v", path, err)
		return nil
	}
	out := map[string]string{}
	for k, v := range doc.Aliases {
		k, v = strings.TrimSpace(k), strings.TrimSpace(v)
		if k == "" || v == "" || strings.EqualFold(k, v) {
			continue
		}
		out[k] = v
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// loadModelAliasesConfig 从 config.json 读 model_aliases。
// 解析失败只打日志：配置里一个坏字段不该让服务起不来（别名是可选的加速项）。
func loadModelAliasesConfig(path string) map[string]string {
	if strings.TrimSpace(path) == "" {
		return nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var doc struct {
		ModelAliases map[string]string `json:"model_aliases"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		log.Printf("[alias] 解析 %s 的 model_aliases 失败：%v", path, err)
		return nil
	}
	out := map[string]string{}
	for k, v := range doc.ModelAliases {
		k, v = strings.TrimSpace(k), strings.TrimSpace(v)
		if k == "" || v == "" || strings.EqualFold(k, v) {
			continue
		}
		out[k] = v
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// normalizeAliases 校验并规范化面板提交的别名表。
// 拒绝三类会静默坏掉请求的输入：空名、空目标、自映射（自己指向自己）。
func normalizeAliases(in map[string]string) (map[string]string, error) {
	out := make(map[string]string, len(in))
	for k, v := range in {
		k, v = strings.TrimSpace(k), strings.TrimSpace(v)
		if k == "" {
			return nil, fmt.Errorf("别名不能为空")
		}
		if v == "" {
			return nil, fmt.Errorf("别名 %q 的目标模型不能为空", k)
		}
		if strings.EqualFold(k, v) {
			return nil, fmt.Errorf("别名 %q 指向自己，会形成无效映射", k)
		}
		if strings.ContainsAny(k, " \t\n") || strings.ContainsAny(v, " \t\n") {
			return nil, fmt.Errorf("别名 %q → %q 不能包含空白字符", k, v)
		}
		out[k] = v
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

// adminAliases GET/POST /admin/api/aliases
//
// GET 回显当前生效的别名表与来源（便于判断「为什么我改的不生效」：
// 可能是 env 覆盖了文件）。POST 写入 config.json 并立即生效。
func (h *Handler) adminAliases(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		writeJSON(w, http.StatusOK, map[string]any{
			"aliases":     h.aliasesSnapshot(),
			"env_var":     aliasEnvKey(),
			"config_path": h.cfg.ConfigPath,
			"sources": map[string]any{
				"config_file": loadModelAliasesConfig(h.cfg.ConfigPath) != nil,
				"data_store":  loadModelAliasesStore(h.aliasStorePath()) != nil,
				"env":         loadModelAliasesEnv() != nil,
			},
			"data_store_path": h.aliasStorePath(),
			// 目标不在静态表里的别名：多半是拼错了模型名，或指向了只在动态表
			// 里出现的模型（上游新增）。面板显示出来，省得用户猜「为什么没生效」。
			"unknown_targets": h.aliasTargets(),
			"note":            "只做一级映射（不递归）；若客户端传的模型名本身就是真实模型 id，则不生效。",
		})
		return
	}
	var req struct {
		Aliases map[string]string `json:"aliases"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
		writeOpenAIError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	clean, err := normalizeAliases(req.Aliases)
	if err != nil {
		writeOpenAIError(w, http.StatusBadRequest, "invalid_config", err.Error())
		return
	}
	if err := h.persistAliases(clean); err != nil {
		writeOpenAIError(w, http.StatusInternalServerError, "write_failed", err.Error())
		return
	}
	h.setAliases(clean)
	log.Printf("[alias] 面板更新别名表：%d 项", len(clean))
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "aliases": clean})
}

// persistAliases 把别名表合并进 config.json（保留其它字段）。
//
// 写盘沿用与配置保存相同的安全序列：备份 → 临时文件 → rename → 回读校验。
// 少了回读校验，一个坏的转义就可能留下「下次起不来」的配置文件。
func (h *Handler) persistAliases(aliases map[string]string) error {
	path := h.cfg.ConfigPath
	if strings.TrimSpace(path) == "" {
		// 没配 ConfigPath（纯 env 部署）时只更新运行时，不报错：
		// 让「改完立刻能用」这件事不依赖是否挂了配置文件。
		log.Printf("[alias] 未配置 config_path，别名仅在本次进程内生效（重启后需重设或改用 %s）", aliasEnvKey())
		return nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("读取 %s 失败：%w", path, err)
	}
	doc := map[string]any{}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &doc); err != nil {
			return fmt.Errorf("解析 %s 失败：%w", path, err)
		}
	}
	if len(aliases) == 0 {
		delete(doc, "model_aliases")
	} else {
		doc["model_aliases"] = aliases
	}
	out, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return err
	}
	// 兜底副本先写：data/ 目录默认挂载到宿主，是容器重建后唯一还活着的路径。
	// 它写失败不算致命（宿主裸跑时 data/ 可能不存在，由服务启动时创建）。
	storePath, storeErr := h.writeAliasStore(aliases)
	if err := os.WriteFile(path+".bak", raw, 0o600); err != nil {
		return fmt.Errorf("备份失败：%w", err)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, out, 0o600); err != nil {
		return fmt.Errorf("写入临时文件失败：%w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("替换失败：%w", err)
	}
	// 回读校验：新内容必须仍是合法 JSON，否则立刻回滚。
	var check map[string]any
	if err := json.Unmarshal(out, &check); err != nil {
		_ = os.WriteFile(path, raw, 0o600)
		return fmt.Errorf("新配置校验失败已回滚：%w", err)
	}
	if storeErr != nil {
		// config.json 写成功、兜底副本失败：目标机器上 config.json 可能没被挂载，
		// 重启后别名会丢。明确告警而不是假装成功。
		log.Printf("WARN: [alias] 兜底副本写入失败（%s）：%v；若 config.json 未挂载到宿主，重启后别名将丢失", storePath, storeErr)
	}
	return nil
}

// writeAliasStore 写 data/model_aliases.json（atomic + 0600）。
func (h *Handler) writeAliasStore(aliases map[string]string) (string, error) {
	path := h.aliasStorePath()
	if path == "" {
		return "", nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return path, err
	}
	out, err := json.MarshalIndent(map[string]any{"aliases": aliases}, "", "  ")
	if err != nil {
		return path, err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, out, 0o600); err != nil {
		return path, err
	}
	if err := os.Rename(tmp, path); err != nil {
		return path, err
	}
	return path, nil
}
