// /admin/api/usage：按模型的用量统计（token / 缓存率 / 每百万 token 消耗积分）。
//
// 两类指标口径不同，接口显式区分，避免误读：
//   - token / 缓存率：全量精确（每个请求的 token_usage 事件）
//   - 积分/百万：采样估算（账号级计数器前后差值），带样本数供判断可信度
package server

import (
	"net/http"
	"time"
)

// adminUsage GET /admin/api/usage：按模型聚合的用量与成本估算。
func (h *Handler) adminUsage(w http.ResponseWriter, r *http.Request) {
	if h.cfg.Usage == nil {
		writeJSON(w, http.StatusOK, map[string]any{
			"enabled": false,
			"note":    "用量统计未启用（config.usage.enabled=false）",
		})
		return
	}
	snap := h.cfg.Usage.Snapshot()
	writeJSON(w, http.StatusOK, map[string]any{
		"enabled":  true,
		"snapshot": snap,
		// 采样是否启用（关闭时 credits_per_million 恒为 0，前端显示 "-"）
		"credit_sampling_enabled": h.cfg.CreditSampler != nil,
		// 采样方式：window=窗口归集（按 token 比例分摊）。窗口内首个请求后
		// 需等一个窗口期才有首个样本，因此刚启动时该列会是 "-"。
		"credit_sampling_method": "window",
		"fetched_at":             time.Now().Format("2006-01-02 15:04:05"),
	})
}
