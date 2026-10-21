<div align="center">

<img src="./docs/logo.svg" alt="trae2api-web logo" width="110" height="110" />


# trae2api-web

**TRAE SOLO 逆向工程服务 · OpenAI 兼容 API · 可视化账号管理面板**

[![Go Version](https://img.shields.io/badge/Go-1.22+-00ADD8?style=flat-square&logo=go&logoColor=white)](https://golang.org/)
[![Docker Ready](https://img.shields.io/badge/Docker-Ready-2496ED?style=flat-square&logo=docker&logoColor=white)](https://www.docker.com/)
[![OpenAI Compatible](https://img.shields.io/badge/API-OpenAI%20Compatible-412991?style=flat-square&logo=openai&logoColor=white)](https://platform.openai.com/)
[![Web Admin](https://img.shields.io/badge/Web%20Admin-Built--in-10b981?style=flat-square)](http://localhost:8000/admin)
[![License](https://img.shields.io/badge/License-MIT-blue?style=flat-square)](LICENSE)

</div>

---

> 基于上游 [https://github.com/Sliverkiss/traework2api](https://github.com/Sliverkiss/traework2api) 改进。
> 
## 概述

`trae2api-web` 是一个将 TRAE SOLO 对话通道包装为标准 OpenAI 协议（`/v1/chat/completions` 与 `/v1/models`）的高性能反向代理服务。基于纯 Go 标准库构建，具备极低资源消耗与高并发处理能力。

本项目内置了轻量级 Web 控制台，支持多账号凭证管理、额度监控、自动化轮换保活以及开箱即用的一键网页登录闭环。

## 核心特性

- **OpenAI 协议兼容**：提供标准 `/v1/chat/completions`（支持流式 Streaming 与非流式）与 `/v1/models` 端点，无缝接入 NextChat、Chatbox、Claude Code、Cline 等客户端。
- **可视化 Web 管理控制台**：内置轻量 Web 界面（`GET /admin`），实时展示账号配额（剩余/已用/总量）、签到状态与健康度，支持多账号并发查询与自动刷新。
- **凭证全生命周期管理**：提供 Web 凭证导入、软启停开关、昵称修改、删除及一键 Web 登录闭环（无需手动抓包或提取 Token）。
- **多账号智能调度池**：加权选号（余额 × 闲置补偿 × 手动权重 × 连败降权）、Top5 加权随机打散、单账号在途租约限流、账号级 + 模型级两级冷却、软限流指数退避、熔断与连败降权、会话粘性、超时/断连不误罚号。
- **自动化运维与保活**：每日定时自动签到，并在 Token 过期前 24 小时自动预刷新与原子落盘，保障长周期稳定可用。
- **新模型支持**：同步支持 glm-5.3、glm-5.2 等新版模型调度，适配最新协议版本。
- **丝滑的流式输出**：响应头 + `role` 帧在**毫秒级**下发（不等上游握手），段内逐帧 flush，跨 chunk 恒定 `id`/`created`/`model`；上游思考期由心跳 SSE 注释保活，思考内容可按 `stream.thinking` 选择走 `reasoning_content` 或包 `think` 标签当正文输出。
- **纯净轻量**：纯 Go 标准库开发，零第三方运行时依赖，静态编译产物小巧，内存占用极低。

## 快速开始

### Docker Compose 部署（推荐）

1. **准备密钥**

```bash
cp .env.example .env
# 编辑 .env，把 TW2A_API_KEY 改成自己的随机串（面板登录 + API 调用都用它）
```

2. **从源码构建并启动（默认监听 8000）**

```bash
mkdir -p auths data
docker compose up -d --build
```

> 镜像内含默认 `config.json`（端口 8000、目录 `/app/auths`、`/app/data`）；
> 想用宿主的 `config.json` 覆盖，把 `docker-compose.yml` 里
> `./config.json:/app/config.json:ro` 那行取消注释即可。
> 换宿主端口：`TW2A_PORT=8080 docker compose up -d`（容器内恒为 8000）。

3. **接口健康检查与模型验证**

```bash
# 健康检查
curl http://127.0.0.1:8000/healthz

# 查看可用模型列表
curl http://127.0.0.1:8000/v1/models

# 查看账号池状态
curl http://127.0.0.1:8000/status
```

### 本地直接运行

环境要求：Go 1.22+

```bash
# 设置访问密钥
export TW2A_API_KEY="your_secure_api_key"

# 编译并启动服务
go build -o trae2api-web ./cmd/server
./trae2api-web
```

## Web 管理面板

服务启动后，访问 `http://127.0.0.1:8000/admin` 即可进入可视化管理后台：

- **账号总览**：实时查看各账号的剩余积分、配额总量、已用积分、权益包数及签到状态。
- **Web 登录闭环**：在面板点击发起登录，浏览器完成验证后回调自动回传至服务并完成 Token 换取与热加载，无需手动复制凭证。
- **凭证管理**：支持粘贴 JSON 凭证或回调链接直接导入账号，支持随时启停软开关、修改备注昵称或删除失效账号。
  **批量导入**支持三种来源，可粘贴文本也可直接选择 `.json` 文件（支持多选）：
  1. 本仓导出包 `{"format":"trae2api-web-accounts","accounts":[{"auth":{...},"account":{...}}]}`（导出→导入闭环）
  2. 中转面板导出包 `{"format":"trae-cn-relay-accounts","accounts":[{user_id,label,token,refresh_token,expired_at,host}]}`
  3. 浏览器登录态导出 `{"accounts":[{"name","token","refreshToken","cookies":[{"name","value","host_key"}]}]}`
     —— 无 `user_id` 时从 JWT 的 `data.id` 解析 uid，无 `expired_at` 时取 JWT 的 `exp`；
     `enabled:false` 的账号导入后保持停用；`cookies` 会原样存档到凭证文件（当前链路仍走
     `Cloud-IDE-JWT` + `X-Uid` 请求头，cookie 不参与出站请求）。
  也接受顶层数组 `[{...},{...}]`、以及多个 JSON 文档首尾相接（手工拼接的导出文件），
  逐条返回 `created/updated/error`，单条失败不影响其余。
- **安全脱敏**：前端展示严格脱敏（仅显示前缀与长度），写操作（导入、删除、修改）均受 `TW2A_API_KEY` 保护。

## API 调用示例

### 对话补全 (Chat Completions)

```bash
curl -X POST http://127.0.0.1:8000/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${TW2A_API_KEY}" \
  -d '{
    "model": "glm-5.2",
    "messages": [
      {
        "role": "user",
        "content": "请用简短的一句话介绍你自己。"
      }
    ],
    "stream": false
  }'
```

## 运维脚本

项目根目录下提供了便捷的 CLI 运维工具：

```bash
# 全账号批量签到与 Token 保活
./signin.sh

# 账号积分与使用情况报表
./credit.sh

# 输出 JSON 格式报表
./credit.sh -json

# 查看指定 UID 账号
./credit.sh <UID>
```

## 调度与运维

### 选号算法

每次请求独立选号，候选必须同时满足：`未被禁用 && 软开关开启 && 未暂停 && 不在账号级冷却 && 不在该模型级冷却 && 在途数未满`。

权重按以下因子相乘后排序取前 5 名，再在前 5 名内加权随机抽取（避免热点账号被反复命中）：

| 因子 | 说明 |
|---|---|
| 余额因子 | `log1p(credits/100)`，积分越多权重越高；credits=0 时给一个小的正基数，保证仍可被选中 |
| 闲置补偿 | `1 + min(闲置小时 × 0.5, 2)`，长期未用的账号优先被拉起来 |
| 手动权重 | 面板/配置文件里 1-100，1 = 不干预 |
| 连败降权 | 连续失败达阈值后权重 × 0.2，持续一个可配窗口 |

另有硬性门槛 `pool.credit_floor`（默认 0 = 关闭）：余额低于该值的账号直接不参与选号，
也不被粘性路由命中。用于把「快跑完的号」提前摘出流量，等签到/月初重置回血，
避免打到 4008 配额超限才被动冷却。

### 失败处置

| 上游表现 | 处置 | 作用域 |
|---|---|---|
| `code:1005` / plan 权益不足（含流内 `event:error`） | 长冷却（默认 12h） | 账号 |
| HTTP 429 | 短冷却，`60s × 2^(streak-1)`，封顶 30m | 账号 |
| HTTP 404 | 短冷却，不计错误次数 | 账号 |
| HTTP 401 / session 失效 | 硬禁用，需面板「revive」或重新登录 | 账号 |
| 5xx / 传输错误 / 连续失败 | 累计计数，达阈值触发熔断（10m 起翻倍，封顶 1h） | 账号 |
| 模型级错误（可配规则命中） | 只锁该模型，其他模型不受影响 | **模型** |
| 上游首字节超时 / 客户端断连 | **不换号、不罚号**，直接结束本次请求 | — |

**实测结论**：TRAE 上游确实存在模型级错误——对当前账号不可用的模型，SSE 流内返回
`event: error` + `code: 3003` / `message: "all models failed"`。该错误只表示「这个号跑不了这个模型」，
与账号本身是否健康无关。默认配置已内置该规则：

```json
{
  "upstream": {
    "model_error_rules": [
      { "code": 3003, "kind": "model_unavailable" }
    ]
  }
}
```

效果：命中后**只锁该模型 10 分钟**，账号本身不进入冷却（`cooling=false`），其他模型照常可用。
规则可随时在 `config.json` 里增删，无需改代码：

```json
{ "code": 1234, "contains": ["model"], "kind": "model_unavailable" }
{ "status": 403, "contains": ["not available"], "kind": "rate_limit" }
```

规则语义：`code != 0` 要求响应体出现该业务码；`status != 0` 要求 HTTP 状态相等；`contains` 需全部子串命中（大小写不敏感）。
流内 `event:error` 没有 JSON body 可匹配子串，只按 `code` 判定，所以**配 `code` 优先**。

### 会话粘性

同一会话（按请求体的 `user` 字段 → 首条 user 消息内容 → 全量 messages 指纹依次派生）优先命中同一账号，
TTL 默认 30 分钟滚动续期。命中账号若因冷却/在途满/模型限额/余额保底不可路由，会自动解绑并回落普通轮换。

实现上刻意分成三步：`session.Lookup`（**只查**已有绑定，不分配）→ `pool` 加权选号 → 成功后
`session.Bind` 回写。这样新会话的首次选号仍走完整的加权算法，余额/闲置/手动权重/降权都生效；
若用「未命中就按 hash 直接分配」的写法，粘性会把加权选号整个架空。
TRAE 是无状态全量上下文，粘性不解决上下文拼接，但能避免同一会话在账号间跳变导致的上游判定漂移。

### 在途租约

每个账号默认最多 2 个并发在途请求（`pool.max_in_flight`），超出时选号直接跳过，
防止单个号被并发打爆。`0` 表示不限制。

### 请求流水

`reqlog` 记录每请求的 uid / 模型 / 状态 / 结果 / 延迟 / tokens / 换号次数，
内存保留最近 512 条，可选按天归档到 `data/logs/requests-YYYYMMDD-N.jsonl`（默认 16MiB/文件、保留 7 天）。
面板「请求流水」页可按 uid / 模型 / 结果过滤并分页。**不记录提示词、响应正文与凭证。**

归档读取走**文件尾部倒读**（按 64KiB 块从末尾往前扫，凑够一页就停），不再整文件解析；
无过滤条件时的总数用「按 (文件, 大小) 缓存的行数」直接给出，带过滤条件时不做全量扫描、总数显示为未知。
20 万行（约 60MiB）归档下取一页 50 条的实测：**386ms → 0.10ms**（`go test -bench BenchmarkTailVsFullScan`）。

### 面板分页与性能

四个页面都是服务端分页，前端只渲染当前页：

| 页面 | 接口 | 默认每页 | 说明 |
|---|---|---|---|
| 账号管理 | `GET /admin/api/accounts?page=&page_size=&q=&status=` | 20 | `q` 搜 uid/昵称/备注，`status`=ok/cooling/paused/off/disabled |
| 额度监控 | `GET /admin/api/credits?page=&page_size=&refresh=1` | 30 | **服务端缓存 60s**；`refresh=1` 强制穿透；上游并发上限 6 |
| 请求流水 | `GET /admin/api/requests?offset=&limit=&uid=&model=&outcome=` | 50 | `offset`=跳过最新 N 条；返回 `total` 与 `total_exact` |
| 用量统计 | `GET /admin/api/usage?page=&page_size=&sort=&q=` | 50 | `sort`=requests/model/tokens/cache/credits；合计行恒为全量口径 |

额度页为什么必须缓存：每个账号一次刷新要打上游两次（`EntUsage` + `CheckinStatus`），
N 个账号就是 2N 次上游调用。改前「切 tab 就全量重查」，5 个号切一次要 ~3s；
现在切 tab / 翻页走缓存（毫秒级），只有点「实时查询上游」才穿透，且同一时刻只允许一份刷新在跑。

### 面板运维动作

`/admin` 面板支持：启用/停用、暂停/恢复选号、revive（解除 session 禁用）、刷新 token、
刷新额度、改调度权重与备注、批量启停、全部签到 + 刷额度、查看脱敏凭证、请求流水与指标。

另有两个常用动作：

- **测试连通性**（`POST /admin/api/accounts/{uid}/test?model=xxx`）：用指定账号打一次最小对话，
  直接回答「这个号现在能不能接单」。该请求**不经选号、不进冷却、不计连败**，不污染调度状态。
- **导出全部凭证**（`GET /admin/api/accounts/export`）：导出**含明文 token**的全量凭证，
  供备份/迁移，返回格式与本仓库 `auths/trae-*.json` 同构（可直接重新导入）。
  该接口强制要求 Bearer，响应带 `Cache-Control: no-store`；输出请勿外传。

## 配置项参考

所有配置项均可通过环境变量或 `config.json` 进行调整：

| 环境变量 | 默认值 | 说明 |
|---|---|---|
| `TW2A_API_KEY` | (必填) | 服务鉴权密钥，用于 API 调用与控制台写操作 |
| `TW2A_LISTEN` | `:8000` | 主服务监听地址及端口 |
| `TW2A_PORT` | `8000` | （仅 docker compose）宿主暴露端口 |
| `TW2A_AUTH_DIR` | `./auths` | 账号凭证存储目录 |
| `TW2A_STATE_FILE` | `./data/state.json` | 账号池状态持久化文件 |
| `TW2A_DEFAULT_MODEL` | `glm-5.2` | 默认回退请求模型 |
| `TW2A_CALLBACK_PORT` | `18080` | 本地 OAuth 回调端口（设为 0 可关闭） |
| `TW2A_TIMEOUT_SECONDS` | `120` | 上游请求超时时长（秒） |
| `TW2A_ERR_THRESHOLD` | `3` | 触发冷却前的连续错误次数 |
| `TW2A_ERR_COOLDOWN` | `10m` | 熔断/错误冷却基数 |
| `TW2A_SOFT_RATE_MAX` | `30m` | 软限流指数退避封顶 |
| `TW2A_MAX_IN_FLIGHT` | `2` | 单账号最大在途请求数（0 = 不限） |
| `TW2A_BREAKER_THRESHOLD` | `3` | 连续失败触发熔断的阈值 |
| `TW2A_BREAKER_COOLDOWN` | `10m` | 熔断基数（连续触发翻倍） |
| `TW2A_CREDIT_FLOOR` | `0` | 余额保底：`credits` 低于该值的号不参与选号/粘性路由（0 = 关闭） |
| `TW2A_SESSION_ENABLED` | `true` | 是否启用会话粘性 |
| `TW2A_SESSION_TTL` | `30m` | 会话绑定 TTL |
| `TW2A_REQLOG_ENABLED` | `true` | 是否记录请求流水 |
| `TW2A_REQLOG_DIR` | `./data/logs` | 请求流水归档目录 |
| `TW2A_REQLOG_RETAIN_DAYS` | `7` | 归档保留天数 |
| `TW2A_STREAM_THINKING` | `content` | 思考内容呈现：`reasoning`（走 `reasoning_content`）/ `content`（包 `think` 标签当正文，空档期立刻有字）/ `none`（丢弃） |
| `TW2A_STREAM_HEARTBEAT` | `1s` | 空闲保活间隔（上游静默时写 SSE 注释，防代理掐断）；`0`/`off` 关闭 |
| `TW2A_USAGE_ENABLED` | `true` | 是否统计按模型用量（token/缓存/积分采样） |
| `TW2A_USAGE_FILE` | `./data/usage.json` | 用量统计持久化文件 |
| `TW2A_CREDIT_SAMPLING` | `30s` | 积分采样窗口（`0` = 关闭积分采样） |

### 用量统计

面板「用量统计」页（`GET /admin/api/usage`）按模型给出：

| 指标 | 口径 | 可信度 |
|---|---|---|
| 请求数 / 输入输出 token | 每个请求的 `event:token_usage` 累加 | **全量精确** |
| 缓存命中 token / 缓存率 | `cache_read_input_tokens / prompt_tokens` | **全量精确** |
| **积分/百万 token** | 窗口积分增量 ÷ 同窗口 token 量，按 token 比例分摊到模型，取多次样本**中位数** | 采样估算，看样本数 |

积分为什么只能估算：TRAE 对话流只返回 token，**不返回积分**；积分只能从
`/trae/api/v2/pay/ide_user_ent_usage` 的 `usage_summary.consumed_amount` 读到，
而那是**账号级累计值且异步结算**。实测两种失真：结算滞后时差值为 0；补结算时
一次差值并进多笔，出现远高于真实值的离群（`kimi-k3` 真实 69~100 积分/百万，
却出现过单次 562）。因此不能按单请求取差值，改为**窗口归集**：

```
窗口开始 → 读计数器基线
  期间每个完成请求记录 (model, tokens)
窗口到期 → 再读计数器，得 Δcredit
          按 token 比例摊到各模型：credit_m = Δcredit × tokens_m / tokens_total
```

分子分母同源同窗口，异步结算只推迟入账时点，不改变窗口内总量对应关系，比值因此无偏；
再对多窗口样本取中位数抗残余离群。窗口大小由 `usage.credit_sampling` 控制（默认 30s，
`0` = 关闭积分采样），**窗口未满时该列显示 `-`**。

已知局限：`consumed_amount` 聚合的是 entitlement 额度包（含 `work_credits`），
不是 SOLO 通道的 `ide_credits`（见 `docs/RESEARCH.md` §3）。因此该比值适合做
**成本横向对比与趋势观察**，不等于「SOLO 通道真实扣费」。

### 面板访问控制

面板与全部读写接口都需要 `TW2A_API_KEY`：

- **浏览器**：访问 `/admin` 未登录时只返回登录页（不含任何控制台内容），
  输入 key 后 `POST /admin/api/session` 换取 `HttpOnly` + `SameSite=Strict` +
  `Path=/admin` 的会话 cookie。cookie 值是 `sha256(key|salt)` 派生值，**不含明文 key**，
  且轮换 key 会让所有旧 cookie 立即失效。点「退出登录」清除。
- **脚本/客户端**：`Authorization: Bearer <key>`，与 cookie 等价。

`TW2A_API_KEY` 为空时门禁自动放开（本地无鉴权场景），生产务必设置。

## 安全声明

- **本地存储与脱敏**：所有账号凭证仅保存于本地 `auths/` 目录，控制台及日志中所有 Token 均严格脱敏输出。
- **环境隔离**：凭证文件、状态数据及环境配置文件默认加入 `.gitignore`，防止误提交泄漏。
- **面板鉴权**：`/admin` 页面与全部 `/admin/api/*` 接口均需 API Key（Bearer 或
  HttpOnly 会话 cookie），未登录只能看到登录页；cookie 不存明文 key，轮换 key 即失效。

## 开源协议

本项目基于 [MIT License](LICENSE) 许可发布。
