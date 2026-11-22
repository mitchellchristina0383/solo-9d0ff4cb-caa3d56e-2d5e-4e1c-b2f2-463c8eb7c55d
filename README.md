# 链路追踪尾部采样演练台（Tail-Based Sampling Workbench）

一个**零第三方依赖**（仅用 Python 3.11 标准库：`http.server` + `sqlite3` + 原生 JS）的本地 Web
演练台，用来演示分布式追踪中「尾部采样（tail-based sampling）」的完整决策过程。

## 它解决什么问题

值班员可以：

1. 手工（或通过演示按钮）提交带时间戳的 trace / span；
2. 配置**收集窗口**、**慢请求阈值**和**每个服务的保留配额**；
3. 观察系统在窗口结束后按**完整链路**（而不是单个 span）决定保留还是丢弃；
4. 观察**错误 / 超时链路优先**、慢请求其次、普通链路最后的排序；
5. 验证跨多个服务的链路必须**同时满足所有相关服务配额**——任一服务配额耗尽，
   整条链路（全部 span）一起丢弃，**绝不部分导出**；
6. 在**宽限期**内投递迟到 span，观察尚未导出的决策如何被翻转；已导出的链路
   **不会重复导出**，最终保留结果也不会突破任何配额；
7. 页面逐条展示每条链路的**保留 / 丢弃 / 等待原因**以及各服务**配额占用**；
8. 预览由**增量评估**驱动：轮询未变更的窗口直接复用上次计算结果；新 span 或
   配额调整只重算通过共享服务**直接或间接**连在一起的链路组；迟到 span 把两组
   独立链路连通时，按原优先级合并重算；每次预览都与同一份已提交数据的全量重算
   **完全一致**；
9. 所有状态持久化在 SQLite，**重启后可继续未完成的窗口**，重启 / 清空 / 换窗后
   的预览一律以 SQLite 当前数据全量重建。

## 目录结构

```
.
├── app/
│   ├── __init__.py
│   ├── server.py            # HTTP 路由、窗口生命周期、API、静态文件服务、缓存管理
│   ├── sampler.py           # 纯函数：链路聚合 + 优先级 + 配额分配决策
│   ├── incremental.py       # 增量评估缓存：共享服务分组、按组重算、结果复用
│   ├── storage.py           # SQLite 持久层（窗口/配额/span/导出记录）
│   ├── demo.py              # 四个一键演示场景
│   └── static/              # 单页前端（index.html / app.js / styles.css）
├── tests/
│   ├── test_sampler.py      # 15 项端到端单测（假时钟，确定性）
│   ├── test_incremental.py  # 9 项增量评估单测（复用/合并/重启/随机一致性）
│   └── smoke_http.py        # 对真实运行服务的 31 项 HTTP 冒烟检查
├── run.py                   # 入口（自带 sys.path 处理，支持 python -I）
├── Dockerfile               # python:3.11-slim，无需 pip install
├── docker-compose.yml       # 一条命令起服务，SQLite 挂在命名卷
└── requirements.txt         # 故意为空：运行期零第三方依赖
```

## 用 Docker Compose 启动

```bash
docker compose up --build
# 打开浏览器
#   http://localhost:8080
```

数据写入命名卷 `tracing-data`（容器内 `/data/tracing.db`）。停止再启动
（`docker compose down` 后重新 `up`，只要不删卷）后，未关闭的窗口、已收
span、配额和导出记录都会恢复。

不使用 Docker 直接本地运行：

```bash
DB_PATH=./data/tracing.db PORT=8080 python3 -I run.py
```

## 采样规则（与代码一一对应）

| 规则 | 实现位置 |
| --- | --- |
| 按 `trace_id` 把乱序 span 聚合成完整链路，延迟按时间戳重算 | `sampler.build_traces` / `Trace.latency_ms` |
| 优先级：错误/超时 tier0 → 慢请求 tier1 → 普通 tier2，同 tier 按开始时间 | `sampler.evaluate_window` 排序键 |
| 每条保留链路占用其**每个**经过服务的 1 个配额槽 | 同上，接受时原子扣减全部相关服务 |
| 任一服务槽位不足 → 整条链路 `drop_quota`，不导出任何 span | `blocking` 分支 |
| 配额是硬上限：错误链路在配额满时一样丢弃，原因里明确说明 | 同上（高优先丢弃时附加说明文案） |
| 宽限期内迟到 span 可改变**暂定**决策；关闭后才最终落库 | `tentative_evaluation` + `_finalize_locked` |
| 已导出链路幂等：重新结束窗口不产生重复导出，且仍占用配额 | `exports` 表主键 `(window_id, trace_id)` + 已导出槽位恢复 |
| 重启恢复未完成窗口 | 全部状态在 SQLite；`AppState` 重建后 `advance()` 自动补关闭 |
| 轮询未变更窗口：复用上次计算，不重算、不扫 span | `incremental.WindowCache`（无脏组件即整体复用） |
| 新 span / 调配额：只重算共享服务连通组，其余链路沿用原结果 | `WindowCache.add_span` / `set_quota` 按组件脏化 |
| 迟到 span 连通两组独立链路：按原优先级合并重算 | `WindowCache._merge` + 组件整体重估 |
| 增量预览 ≡ 同一份已提交数据的全量重算 | 组件间不共享服务 ⇒ 配额互不影响（见下节证明） |
| 清空 / 换窗 / 重启后以 SQLite 当前数据生成预览 | 缓存即弃，`WindowCache.from_storage` 全量重建 |

## 增量评估原理（为什么按组重算与全量重算完全一致）

把「链路 ↔ 服务」看成二分图：链路经过某服务即连一条边。一个服务的配额
只会被经过它的链路消耗，因此**不同连通分量里的链路互不影响**彼此的
保留/丢弃决定、判定原因和配额占用。于是：

* 每个连通分量（页面上的「组」）用同一个 `sampler.evaluate_window`
  独立评估，结果与全窗口全量重算**逐链路一致**；
* 轮询时若没有脏组件，整份计算结果（含每条链路的展示数据）直接复用；
* 新 span 只脏化它所在的组件；若它带的服务属于另一个组件，两个组合并
  为一组整体重算（排序键不变：tier → 开始时间 → trace_id，即原优先级）；
* 配额调整只脏化包含该服务的组件；没有任何链路经过的服务不影响决策；
* 窗口关闭时按「已导出槽位恢复」语义整体重估一次并落库，之后不可变；
* 缓存只是 SQLite 已提交数据的派生物：清空、换窗、重启（含模拟重启）
  即弃，下次预览从 SQLite 全量重建。

页面「当前窗口状态」底部会显示本次预览的来源（全量重建 / 增量重算 N 组 /
复用）和共享服务分组；`GET /api/debug/cache` 可查看各组件成员与累计统计。

窗口三个阶段（页面顶部有进度条）：

- `collecting` 收集中：正常接收 span，展示「暂定」保留/丢弃；
- `grace` 宽限期：仍可接收时间戳合法的迟到 span，决策可能被翻转；
- `closed` 已关闭并导出：拒绝新 span，导出记录固定，不可重复导出。

## 四个一键演示场景

| 按钮 | 演练点 |
| --- | --- |
| 乱序 span | s3→s1→s2 乱序投递，窗口结束后按时间戳重建完整链路并重算延迟 |
| 宽限期迟到错误 | 普通链路在宽限期内收到一个迟到的错误 span，优先级翻转并抢到配额 |
| 配额耗尽 | payment/orders 配额仅 1，错误链路抢占唯一保留位，其余整条丢弃 |
| 跨服务配额联动 | 链路经过配额为 0 的 inventory，整条丢弃；不经过它的链路正常保留 |

页面上还有「立即结束并导出」「模拟进程重启（从 SQLite 恢复）」「清空全部数据」。

## 自测

```bash
# 15 项确定性单测（假时钟驱动 collecting→grace→closed 全流程）
python3 -I tests/test_sampler.py

# 9 项增量评估单测（未变复用 / 无关链路复用 / 两组合并 / 重启重建 /
# 关闭重估可见性 / 160 步随机操作逐步比对全量重算）
python3 -I tests/test_incremental.py

# HTTP 冒烟（先起服务）
DB_PATH=/tmp/smoke.db PORT=8091 python3 -I run.py &
python3 -I tests/smoke_http.py http://127.0.0.1:8091
```

覆盖：乱序 span、迟到错误翻转、配额硬上限、跨服务原子性、重启恢复、
幂等导出、关闭后拒 span、重复 span 幂等、默认配额、慢请求优先、运行中
调配额、输入校验、演示场景装载、零宽限边界；以及增量评估的：轮询复用、
无关链路复用、迟到 span 合并两组、重启/清空/换窗后全量重建、随机操作
下每次预览与全量重算完全一致。

## 主要 API

```
POST /api/windows                     创建窗口（collect_ms/grace_ms/slow_ms/default_quota/quotas）
POST /api/windows/<id>/quotas         运行中调整某服务配额
POST /api/windows/<id>/finalize       立即结束窗口并导出（幂等）
POST /api/spans                       提交 span（时间戳由调用方给）
GET  /api/window/current              当前/最近窗口的完整快照（含每条链路原因、配额占用与增量信息）
GET  /api/windows/<id>                任意窗口详情（含已关闭窗口）
GET  /api/windows                     历史窗口
GET  /api/debug/cache                 增量缓存：共享服务分组、每组链路、累计统计
POST /api/demo/<name>                 演示场景：out-of-order | late-error | quota-exhausted | multi-service
POST /api/simulate-restart            关闭并重开 Storage，模拟进程重启
POST /api/reset                       清空全部数据
```

快照中新增的字段：`incremental`（本次预览来源 full_build / incremental /
reuse、重算与复用的链路数、合并次数）和 `groups`（共享服务连通组及其
成员链路），页面「当前窗口状态」底部同步展示。
