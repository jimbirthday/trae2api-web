// /admin/api/usage：按模型的用量统计（token / 缓存率 / 每百万 token 消耗积分）。
//
// 两类指标口径不同，接口显式区分，避免误读：
//   - token / 缓存率：全量精确（每个请求的 token_usage 事件）
//   - 积分/百万：采样估算（账号级计数器前后差值），带样本数供判断可信度
package server

import (
	"net/http"
	"sort"
	"strings"
	"time"

	"trae2api-web/internal/usage"
)

// adminUsage GET /admin/api/usage?page=&page_size=&q=
//
// 模型通常只有十几个，但用量数据会随时间累积（模型名带版本号后很容易上百），
// 因此这里也对模型列表分页；totals 始终是「全量合计」，不随分页变化。
func (h *Handler) adminUsage(w http.ResponseWriter, r *http.Request) {
	if h.cfg.Usage == nil {
		writeJSON(w, http.StatusOK, map[string]any{
			"enabled": false,
			"note":    "用量统计未启用（config.usage.enabled=false）",
		})
		return
	}
	q := r.URL.Query()
	page := clampInt(q.Get("page"), 1, 1, 1<<20)
	pageSize := clampInt(q.Get("page_size"), 50, 1, 500)
	sortKey := strings.ToLower(strings.TrimSpace(q.Get("sort")))
	keyword := strings.ToLower(strings.TrimSpace(q.Get("q")))

	snap := h.cfg.Usage.Snapshot()
	models := snap.Models
	if keyword != "" {
		kept := make([]usage.ModelStat, 0, len(models))
		for _, m := range models {
			if strings.Contains(strings.ToLower(m.Model), keyword) {
				kept = append(kept, m)
			}
		}
		models = kept
	}
	switch sortKey {
	case "tokens":
		sort.SliceStable(models, func(i, j int) bool { return models[i].TotalTokens > models[j].TotalTokens })
	case "cache":
		sort.SliceStable(models, func(i, j int) bool { return models[i].CacheRatePct > models[j].CacheRatePct })
	case "credits":
		sort.SliceStable(models, func(i, j int) bool { return models[i].CreditsPerMillion > models[j].CreditsPerMillion })
	case "tps":
		sort.SliceStable(models, func(i, j int) bool { return models[i].AvgTPS > models[j].AvgTPS })
	case "model":
		sort.SliceStable(models, func(i, j int) bool { return models[i].Model < models[j].Model })
	}
	total := len(models)
	start := (page - 1) * pageSize
	if start > total {
		start = total
	}
	end := start + pageSize
	if end > total {
		end = total
	}
	paged := snap
	paged.Models = models[start:end]

	writeJSON(w, http.StatusOK, map[string]any{
		"enabled":   true,
		"snapshot":  paged,
		"total":     total,
		"page":      page,
		"page_size": pageSize,
		"pages":     (total + pageSize - 1) / pageSize,
		// 采样是否启用（关闭时 credits_per_million 恒为 0，前端显示 "-"）
		"credit_sampling_enabled": h.cfg.CreditSampler != nil,
		// 采样方式：window=窗口归集（按 token 比例分摊）。窗口内首个请求后
		// 需等一个窗口期才有首个样本，因此刚启动时该列会是 "-"。
		"credit_sampling_method": "window",
		"fetched_at":             time.Now().Format("2006-01-02 15:04:05"),
	})
}
