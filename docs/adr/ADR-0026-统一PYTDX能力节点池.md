# ADR-0026：统一 PYTDX 能力节点池

- 状态：Accepted
- 日期：2026-08-11
- 关联 Issue：#39
- 取代：ADR-0024 中显式 endpoint 配置与禁止运行时节点发现的决定
- 澄清：ADR-0024 的未复权 Daily Bar、Raw lineage、有限连接 failover、单批次固定 endpoint 与缺口可见语义继续有效
- 2026-08-25 澄清：刷新间隔固定在 Worker 任务目录中为 1 小时，不接受环境变量覆盖

## 背景

远程 Daily Bar、集合竞价五档和收盘五档当前分别保留
`PYTDX_DAILY_BAR_ENDPOINTS`、Daily Bar pool、`PYTDX_HQ_HOST` 和 HQ pool 等配置与
fallback。两个 Provider 重复解析同形状文件，systemd 启动检查又只识别显式 Daily Bar
endpoint，导致可用池存在时 Worker 仍可能拒绝启动。公共 TDX 节点能力并不一致：节点可能
只支持实时 quote、只覆盖沪深某一市场，或缺少北交所日线，因此单一“可连接”状态不足以
支持确定性路由。

## 决定

1. Daily Bar 与五档行情只读取 `PYTDX_POOL_PATH` 指向的
   `pytdx.endpoint_pool.v1` 文件，并按 capability 选择节点。
2. Worker 取得全局 Scheduler advisory lock 后执行启动刷新，随后每 1 小时通过
   APScheduler 刷新。刷新间隔由受控代码任务目录固定，不通过 `.env` 配置。不得使用 cron、
   Windows Task Scheduler 或其他 OS 级刷新任务。
3. 刷新从 pytdx 内置候选目录执行有界并发探测。每个节点显式记录 `quote`、
   `daily_bar_sse`、`daily_bar_szse`、`daily_bar_bse` 四项布尔能力及探测延迟。
4. 新池只有在至少各有一个 quote、SSE 日 K 和 SZSE 日 K 节点时才能发布；BSE 能力独立
   记录，不作为发布硬门禁。BSE 无可用节点时保留显式缺口。
5. 发布使用同目录临时文件、flush、fsync 和原子替换。刷新失败不得覆盖最后一个有效池；
   新旧池均无效时 Worker 拒绝启动。
6. 删除 `PYTDX_DAILY_BAR_ENDPOINTS`、`PYTDX_DAILY_BAR_POOL_PATH`、
   `PYTDX_HQ_HOST`、`PYTDX_HQ_PORT`、`PYTDX_HQ_POOL_PATH` 及其解析和 fallback 代码。
7. `PYTDX_VIPDOC_PATH` 保留。Daily Bar 继续先读取本地 `.day` 文件；本地文件缺失时才按
   SSE、SZSE 或 BSE capability 使用远程节点池。
8. Daily Bar 为不同市场维护独立固定会话；五档采集会话只选择 `quote=true` 节点。连接
   阶段允许有限 failover，读取开始后不在同一批次中途切换 endpoint。
9. 每个成功 IngestionRun 仍只有一个实际 Provider 和 endpoint。endpoint 继续只写入 Raw
   request metadata，不进入 Domain Record、Core 或公共 API。
10. 节点池刷新登记为内部 Operations workflow `pytdx_pool_refresh`，不创建 IngestionRun，
    不发布到 PostgREST、FastAPI 或 Agent 工具契约。

## 节点池契约

```json
{
  "schema_version": "pytdx.endpoint_pool.v1",
  "refreshed_at": "2026-08-11T10:00:00+08:00",
  "nodes": [
    {
      "host": "203.0.113.10",
      "port": 7709,
      "latency_ms": 86,
      "capabilities": {
        "quote": true,
        "daily_bar_sse": true,
        "daily_bar_szse": true,
        "daily_bar_bse": false
      }
    }
  ]
}
```

读取器必须拒绝未知版本、无时区刷新时间、非法端口、重复 endpoint、缺失或非布尔
capability。节点按 `(latency_ms, host, port)` 稳定排序。池文件是可重建的本地运行数据，
不得提交 Git，也不属于 Raw market data。

## 后果与限制

- 节点维护从人工配置转为 Worker 内部受控刷新，运维只配置池路径，不配置刷新周期。
- Worker 启动依赖一个满足门禁的新池或 last-good 池；网络全面不可用且没有 last-good 时
  会明确失败，而不是以无可用 Provider 的状态继续运行。
- 节点能力是协议探测结果，不是市场事实，不进入数据库领域模型。
- 公共节点没有 SLA。节点池提高选择确定性和恢复能力，但不承诺 BSE 覆盖，也不允许使用
  BaoStock 或 AKShare 填补普通 Daily Bar 缺口。
- 池刷新、Provider 消费和部署检查共享一个严格解析实现，避免多份容错语义漂移。

## 验收

- Fake client 测试覆盖严格解析、能力筛选、有界探测、稳定排序、原子发布和 last-good。
- Worker 测试覆盖取得全局锁后启动刷新、无有效池拒绝启动和每 1 小时 interval job。
- Daily Bar 测试覆盖 vipdoc 本地优先、按市场 capability、连接阶段 failover、请求阶段不切换、
  BSE 显式缺口和 endpoint lineage。
- 五档测试覆盖只选择 quote 节点、固定采集会话、Decimal 与手转股语义不变。
- migration 约束全部受控 workflow code，并保持公共 API 契约不变。
- 活跃源码、部署模板和运行手册不再引用旧 endpoint 配置；完整 Ruff、mypy、pytest 与隔离
  PostgreSQL integration gate 通过。
