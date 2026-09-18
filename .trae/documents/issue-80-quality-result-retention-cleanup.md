# 实现 Issue #80: audit.quality_result 三十日保留清理任务

## Context

`audit.quality_result` 上线 5 周积累 2049 万行 / 6 GB，无保留策略。2026-09-16 人工清理后，
Issue #80 要求将该清理纳入既有 `data_cleanup` Worker 任务，每日自动删除 30 天前的质检结果。

既有 `DataCleanupService` 已在 `run()` 中完成竞价序列快照清理 + 历史清理，返回一个汇总
`DataCleanupSummary`。本次扩展在同一 `run()` 内追加质检结果清理步骤，扩展 summary 字段。

## 改动文件

### 1. `src/market_data_center/data_cleanup_service.py` — 核心扩展

- 新增常量 `QUALITY_RESULT_RETENTION_DAYS = 30`
- 新增 `quality_result_cutoff(reference_date: date) -> date`：返回 `reference_date - 30 天`（纯日历日，不依赖交易日历）
- `DataCleanupPersistence` Protocol 新增方法：
  ```python
  def delete_quality_results_before(self, cutoff_date: date) -> int: ...
  ```
- `DataCleanupSummary` 新增两个字段：
  - `quality_result_cutoff_date: date`
  - `quality_result_deleted_rows: int`
  - `__post_init__` 扩展非负校验
- `DataCleanupService.run()` 末尾追加质检清理，返回扩展后的 summary

### 2. `src/market_data_center/persistence/postgres.py` — SQL 与持久层

- 新增 SQL 常量（紧跟现有 `DELETE_STOCK_DAILY_INDICATORS_BEFORE` 之后）：
  ```python
  DELETE_QUALITY_RESULTS_BEFORE = text("""
  delete from audit.quality_result
  where created_at < :cutoff_date
  """)
  ```
- `PostgreSQLPersistence` 新增方法 `delete_quality_results_before`，仿照
  `delete_call_auction_market_series_snapshot_history_before` 的单事务 DELETE 模式

### 3. `supabase/migrations/20260916000100_grant_quality_result_delete.sql` — 新建

- `grant delete on audit.quality_result to market_data_worker;`
- 现有 RLS policy `quality_result_worker_all ... for all` 已覆盖 DELETE，无需新增 policy

### 4. `src/market_data_center/scheduling_catalog.py` — 描述更新

- `data_cleanup` WorkflowDefinition 描述更新为涵盖竞价快照 + 质检结果清理
- steps 保持 `("cleanup_call_auction_market_series_snapshots",)` 不变——
  `service.run()` 内部完成全部清理，scheduler 仍是一个 step

### 5. `src/market_data_center/scheduler.py` — 调度入口

- `run_data_cleanup_job()` 的 docstring 更新，仍调用同一个 `service.run(reference_date)`
- step code 和 sequence_no 不变

### 6. `src/market_data_center/operations_service.py` — 统计

- `_result_statistics` 中 `DataCleanupSummary` 分支追加 `quality_result_deleted_rows` 到 fetched/accepted

### 7. `tests/test_data_cleanup_service.py` — 单元测试

- `FakeCleanupPersistence` 新增 `delete_quality_results_before` 方法 + 记录字段
- 新增 `test_quality_result_cutoff_returns_thirty_calendar_days_before`
- 更新 `test_cleanup_service_deletes_only_after_cutoff_is_resolved` 断言新字段
- 更新 `test_cleanup_summary_rejects_negative_deleted_count` 覆盖新字段

### 8. `tests/test_postgres_integration.py` — 集成测试

- 新增 `@pytest.mark.integration` 测试：插入旧质检结果 → 调用 `delete_quality_results_before` → 验证删除 + 新行保留
- 验证 Worker 角色可执行 DELETE（权限测试）

## 验证

```bash
uv run ruff format --check .
uv run ruff check .
uv run mypy src
uv run pytest tests/test_data_cleanup_service.py -v
uv run pytest -m "not integration" -q
# 集成测试（需 TEST_DATABASE_URL 指向可用 PostgreSQL）
uv run pytest -m integration -k "quality_result or cleanup" -v
```
