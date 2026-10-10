# 专业版股票技术因子实施计划

依据：Issue #88、Accepted ADR-0066、已确认 StockTechnicalFactor 领域详设。
直接在 master 开发，保留已有修改，不提交、不部署、不执行生产操作。
按固定接口并行实现独立文件；每项先写测试并观察失败，再实现和验证。

## 1. 领域与 Provider

新增 domain/stock_technical_factor.py 和 providers/tushare_technical_factor.py。
Record 为 StockTechnicalFactorSnapshotRecord，包含 symbol、trade_date、market、
source_code、factor_catalog_version、unadjusted、forward_adjusted、backward_adjusted、common。
三种口径为 Mapping[str, Decimal | None]，common 为 Mapping[str, int | None]。
Provider 为 TushareTechnicalFactorProvider，复用 TushareHttpClient，source_code=tushare，
fetch_snapshot(trade_date) 返回 ProviderBatch[Record]；normalize_technical_factor_raw(rows, schema)。
独立模块避免修改现有 Provider 协议和其他 Provider。锁定 226 字段、Raw replay、失败边界。

## 2. Migration 与 Persistence

新有序 migration 建表、数据集约束、RLS、operations 定义与有界 RPC。
新增 persistence/technical_factor_postgres.py，PostgreSQLTechnicalFactorPersistence 继承
PostgreSQLPersistence 以复用采集审计、锁与 Raw 方法。
接口：technical_factor_coverage_floor(trade_date) -> int，
technical_factor_securities(trade_date) -> Collection[str]，
commit_technical_factor_batch(run, manifest, records, quality_results) -> None。
公开 RPC query_stock_technical_factors(p_symbols text[], p_trade_date date) -> jsonb；
另一个有界代码解析包装 query_stock_technical_factors_by_codes(p_codes text[], p_trade_date date)
通过 Security 解析 symbol，再使用标准查询，供 FastAPI 调用。
返回 trade_date、items、missing_symbols 或 missing_codes；items 含 symbol、code、name、
trade_date、source_code、factor_catalog_version、四组十进制字符串/空值映射。
覆盖 DB 权限、边界、UPSERT/血缘/事务测试，仅使用隔离数据库。

## 3. API 与契约

新增 public_api/technical_factors.py，独立请求/响应模型及路由注册。
POST /v1/stock-technical-factors/query，codes 1～100 个六位股票代码，trade_date 必填。
查询仅调用 api_v1 RPC；同步三份契约，覆盖鉴权、边界、缺失、精度与现有契约回归。

## 4. Pipeline 与 Worker

新增 technical_factor_service.py，复用 IngestionPipeline 的 Raw、run 与失败记录机制，
独立严格校验和整批发布路径；扩展 DatasetCode 和重放。
CLI stock-technical-factor-daily --as-of-date，休市跳过；Worker 21:00 独立任务。
扩展 scheduling_catalog、WorkflowCode、operations 定义和 scheduler runner。
测试运行日期/休市/失败/覆盖下降/重放/幂等，保持原有每日指标行为。

## 5. 验证与交付

更新 README、独立 runbook、领域文档的当前状态。
执行 uv run ruff format --check .、uv run ruff check .、uv run mypy src、uv run pytest，
及 uv run pytest -m integration；无法运行时报告命令和原因。
最终检查全量差异与独立审阅，不修改用户原有工作。

## 实施记录（2026-10-10）

上述五项已在 master 本地完成，Issue #88 保持打开等待代码发布流程。
专业版全部 226 个技术因子值、独立日快照、Raw 重放、有界 API 和 Worker 21:00
任务均已接线。原有 daily_basic 指标未改动。独立审阅发现的重放锁、原始行数、
缺字段 Raw 审计及源日期校验问题已修正并通过回归；同日修订显式删除缺失证券的旧记录，
失败事务恢复此前快照。新表保留 Worker 写入、备份角色只读及 API 内部表隔离。

验证证据：

- `uv sync --all-groups`：成功，既有依赖可用，未新增依赖。
- `uv run ruff format --check .`：481 个文件格式检查通过。
- `uv run ruff check .`：通过。
- `uv run mypy src`：151 个源文件通过。
- `uv run pytest`：1437 passed，150 skipped；此轮未设置 TEST_DATABASE_URL，
  数据库用例随后单独完整执行。其余三项跳过为本机无法创建测试符号链接。
- `uv run pytest -m integration`：本机一次性 PostgreSQL 16 容器，串行运行全部
  148 个数据库用例，147 passed、1 skipped。跳过的是 pg_dump 备份恢复用例，
  原因是本机没有 pg_dump；新领域真实数据库采集/Raw 重放端到端测试包含在通过集合中。
- `git diff --check`：通过；既有文件的 Windows 换行提示不属于检查失败。

这些集成用例会修改共用 PostgreSQL 角色，须在单一测试集群内串行执行。
临时测试库与容器在验证后清理。未执行生产迁移、生产采集、部署、提交或推送；
真实 Tushare 账户权限、来源覆盖和数据就绪时间尚未在线验证。
