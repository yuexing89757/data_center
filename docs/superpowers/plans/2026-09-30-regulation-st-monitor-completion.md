# 异动监控 ST 事实与连续发布 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 以逐日 ST 来源事实替代缺失的历史名称判定，让异动监控从 2026-07-06 连续发布可解释的价格结果；真正缺失的行情/换手仍显示未知。

**Architecture:** 复用 Tushare HTTP Provider、Raw/IngestionRun、Worker 监管工作流和已部署的有界 API。新增每日完整 ST 集合事实，监控装配按精确交易日读取并将其血缘加入输入身份；算法版本升为 v2，旧 v1 结果保持不可变。

**Tech Stack:** Python 3.12、uv、SQLAlchemy、PostgreSQL、APScheduler、pytest；不加依赖。

**Spec:** `docs/superpowers/specs/2026-09-30-tushare-st-history-design.md`；ADR-0061（Accepted）；Issue #85；原监控 Issue #69 与 ADR-0060。

## Global Constraints

- 只覆盖沪深主板/创业板监管监控；BSE 日 K 路由提案 #86 不进入本计划。
- `stock_st(trade_date=YYYYMMDD)` 单日完整成功才可推断名单外为非 ST。空、重复、错日、非法代码、达到 1000 行上限均失败且不发布。
- `core.daily_bar.is_st` 未知值和当前证券名称不能倒填历史；名称只展示。缺 ST 批次为 `INSUFFICIENT_DATA`。
- 保留不可变 Raw、实际 Tushare 来源和单一采集批次；重放必须验证 Raw 并复用 manifest。
- 监控 v2 必须从 2026-07-06 连续重算；未补齐换手率不能填 0，但不阻断具有完整价格事实的规则。
- 不新建公开 ST API、操作系统任务或依赖；FastAPI 仍只通过有界 `api_v1` RPC 只读查询。
- 本地测试用隔离可丢弃 PostgreSQL；生产迁移仅通过受保护工作流。部署、补采、补算前核对准确目标和待迁移文件。
- 现有工作区其他修改属于用户，不纳入本计划提交。

## Review Focus

1. Tushare 返回 0 或恰好 1000 行：拒绝发布，不能把全市场当非 ST；Task 1 测试。
2. 某日 ST 名单更正：旧 Raw/监控批次不改，新 ingestion_id 改变输入哈希，后继链须重算；Task 2、3 测试。
3. 历史名称为空且 ST 集合完整：非 ST 股票仍可计算；缺集合时保持不足；Task 3 测试。
4. 当日已知非 ST，但下一交易日 ST 状态尚未知：次日结果须注明条件假设，不给不受约束的确定结论；Task 3 测试。
5. 历史换手缺口与 12 只缺日收益：价格规则独立可用，相关规则/股票仍显式部分覆盖；Task 4 预检核对。

---

### Task 1：Tushare 单日 ST 完整快照与 Raw 解码

**Files:** `src/market_data_center/domain/ingestion.py`、`src/market_data_center/domain/records.py`、`src/market_data_center/providers/contracts.py`、`src/market_data_center/providers/tushare.py`、`tests/test_tushare_provider.py`。

**Interfaces:** `DatasetCode.REGULATION_ST_SNAPSHOT = "regulation_st_snapshot"`；`TushareProvider.fetch_regulation_st_snapshot(trade_date: date) -> ProviderBatch[RegulationStSnapshotRecord]`；record 保存 `trade_date`, `symbols`, `source_code`。`normalize_tushare_raw` 接受 schema `tushare.regulation_st_snapshot.v1`。

- [ ] 先加失败测试：Mock `stock_st` 返回两个不同标准代码并保留原始行；错日、重复代码、非法代码、空、1000 行、上游错误均拒绝；Raw normalizer 得到与在线一致的 record。
- [ ] 运行 `uv run pytest tests/test_tushare_provider.py -q -p no:cacheprovider`，确认新增用例因能力缺失而失败。
- [ ] 增加记录及 `stock_st` 请求（字段 `ts_code,name,trade_date,type,type_name`），用现有 `_rows` 和 `_source_symbol` 的逆映射规则；完整性检查在共享 normalizer 中执行，在线/Raw replay 同口径。只保存记录中的标准代码集合，不以名称判断 ST。
- [ ] 重跑同一测试文件；再跑 `uv run mypy src` 验证 ProviderRecord 联合类型无遗漏。
- [ ] 仅提交本任务文件，不混入工作区其他改动。

### Task 2：迁移、原子发布与 Raw replay

**Files:** 新建 `supabase/migrations/20260930000100_create_regulation_st_day_snapshot.sql`；修改 `src/market_data_center/pipeline.py`、`src/market_data_center/persistence/postgres.py`、`src/market_data_center/reliability.py`；新增/扩展 `tests/test_regulation_st_snapshot_postgres.py` 和 Raw replay 测试。

**Interfaces:** `regulation.st_day_snapshot(trade_date PK, symbols jsonb, symbol_count, source_code, ingestion_id, created_at)`；Worker 仅 SELECT/INSERT/UPDATE，API/anon 无表权限；`IngestionPipeline.ingest_regulation_st_snapshot(trade_date)` 调用 Task 1 Provider 并写入 Manifest+快照+成功运行同一事务。

- [ ] 隔离数据库先写失败测试：成功快照与 RawManifest/ingestion_run 血缘一致；重复采集幂等；更正以新 ingestion_id 替换当天快照但旧 Raw 保留；失败不可发布；API 角色无内部表权限。
- [ ] 运行 `uv run pytest tests/test_regulation_st_snapshot_postgres.py -q -p no:cacheprovider`，确认缺表/方法导致失败。
- [ ] migration 扩展 ingestion/audit 的 dataset check，仅新增 `regulation_st_snapshot`；创建日期主键快照表、行数/JSON/来源约束、RLS 和最小授权。不要更改旧迁移。
- [ ] Pipeline 复用 `_start_run`、`_stage_batch`、`_completed_run`、`_record_failure`；Persistence 在同一事务写 manifest、快照与 run。Replay 在已验证的 Tushare Raw 分支调用同一提交函数，不复制 Raw 对象。
- [ ] 跑新增数据库测试及现有 replay 测试，核对重复日期、失败原子性和权限，再提交准确文件。

### Task 3：监管适用性与版本一致性

**Files:** `src/market_data_center/persistence/regulation_postgres.py`、`src/market_data_center/regulation_calculator.py`、`src/market_data_center/public_api/regulation_monitor.py`、新建第二个顺序 SQL 迁移以 `create or replace` 更新 monitor RPC；`tests/test_regulation_persistence.py`、`tests/test_regulation_monitor_postgres.py`、`tests/test_regulation_monitor_public_api.py`、API 契约文件（若响应形状变化）。

**Interfaces:** `REGULATION_MONITOR_VERSION = "regulation-monitor.v2"`；原 v1 批次不覆盖。公开 JSON `schema_version` 仍为 `regulation-monitor.v1`（响应形状不变），`algorithm_version` 为 v2。

- [ ] 先写失败测试：历史名称 null + 完整 ST 名单外为 APPLICABLE；名单内为 NOT_APPLICABLE；缺名单为 INSUFFICIENT_DATA；更正 ingestion_id 导致不同输入哈希；仅缺换手时价格规则仍可给结果。
- [ ] 运行 `uv run pytest tests/test_regulation_persistence.py tests/test_regulation_monitor_postgres.py -q -p no:cacheprovider`，确认新测试失败。
- [ ] 装配 `load_calculation_source()` 时精确取目标日快照，把 `ingestion_id` 纳入 `market_watermark`；不要在 `core.daily_bar` 修改 `is_st`。名称为空不改变 ST 状态。输入缺快照时记录 `missing_regulation_st_snapshot`，不默认非 ST。
- [ ] 将监控算法版本升 v2；顺序迁移更新 `monitor_chain_current`、`monitor_query_context`、`query_regulation_monitor_inputs` 的版本筛选及目标日 ST 判定。次日仅以截至查询时已知的 ST/公司行为作条件防护；未知未来不声明确定安全。API 仍不访问内部表。
- [ ] 运行上述测试、API/SQL 契约测试与隔离数据库测试，确认旧正式公告查询与 v1 结果仍隔离，再提交。

### Task 4：Worker 日终、历史命令与运维交付

**Files:** `src/market_data_center/cli.py`、`src/market_data_center/scheduler.py`、`src/market_data_center/scheduling_catalog.py`、`docs/runbooks/regulation-monitor.md`、相关调度/CLI 测试。

**Interfaces:** 手动精确日期命令 `regulation-st-snapshot --trade-date YYYY-MM-DD`；日终监管工作流先采当日 ST 快照，再发布监控批次，现有 22:30 APScheduler 时刻不变。

- [ ] 写失败测试：非交易日跳过、ST 来源失败使监控步骤不运行、成功后才调用监控、重复运行幂等；CLI 只处理明确日期。
- [ ] 运行 `uv run pytest tests/test_regulation_scheduler.py tests/test_scheduler.py -q -p no:cacheprovider`，观察新增测试失败。
- [ ] 增加现有工作流内的 ST 步骤和单日 CLI；历史补采循环只在运维脚本/明确命令内逐日调用，不新增全市场重试调度器。运行日志只输出日期/行数/状态，不打印 Token 或 Raw 行。
- [ ] 重跑调度、CLI 与 Raw replay 测试，更新运行手册并提交准确文件。

### Task 5：验证与生产切换

**Files:** 不新增产品文件；只更新 `docs/runbooks/regulation-monitor.md` 中实测覆盖/部署记录。

**Interfaces:** 依赖 Task 1–4 的同一提交和受保护迁移；产出逐日 ST 覆盖、监控预检、连续批次、API 真实样例与 Worker 实跑记录。

- [ ] 跑 `uv run ruff format --check .`、`uv run ruff check .`、`uv run mypy src`、`uv run pytest`；集成项用 `TEST_DATABASE_URL` 隔离库，不能以跳过代替通过。失败先修复，不发布。
- [ ] 只读核对生产目标、全部待迁移文件、可用空间和 Worker/API 版本；受保护流程先 check 再 apply，确认只含本计划受准变更。
- [ ] 推送已验证提交并部署同版本 API/Worker；先逐日补 2026-07-06 至最新已收盘交易日 ST 快照，逐日核对 Raw/快照行数。暂停自动监控发布直到历史顺序补算完成，避免无可信检查点起跑。
- [ ] 只读预检完整区间。若仍无任何可执行股票，停止写入并按明细定位；若可部分发布，按交易日顺序执行 v2 补算，显式保留换手与真实日 K 缺口。
- [ ] 验证连续父批次、覆盖计数与真实候选/API 查询；Worker 启用后观察下一次实际 22:30 执行。未观察到实跑只称“已配置”，不称稳定运行。

## 自检

Task 1 提供可信 ST 输入，Task 2 提供 Raw/批次与权限，Task 3 使监控和 API 采用相同逐日事实并升级版本，Task 4 接入现有 Worker，Task 5 才执行生产切换。历史换手率全市场补采不在本计划中：现有代理全市场请求曾超时；价格结果可独立发布，换手部分保持不足并另行定位。所有五项 Review Focus 在对应任务有测试或生产核对。
