# 存储生命周期与03:00清理 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在不破坏来源事实和重放证据的前提下，为现有03:00任务增加Raw无损压缩、白名单质量归档、受控临时文件清理和容量报告。

**Architecture:** 保留现有快照清理Service和Worker调度入口。文件校验与发布留在存储模块，SQL集中到专用Persistence；维护Service串行执行有界工作单元，复用既有Operations和advisory lock。旧发布物不由Worker处理，另见配套计划。

**Tech Stack:** Python 3.12、标准库gzip/hashlib/pathlib、SQLAlchemy、PostgreSQL、有序SQL迁移、APScheduler、pytest。

**Spec:** `docs/superpowers/specs/2026-09-22-storage-lifecycle-cleanup-design.md`（Approved）；`docs/adr/ADR-0058-无损Raw压缩与受控存储清理.md`（Accepted）。

## Global Constraints

- 任务记录以GitHub Issue #84为准；本文为其仓库内实施明细，不另建任务系统。
- 每天03:00 Asia/Shanghai，仍用 `data-cleanup-daily` / `data_cleanup`；不增加操作系统定时任务。
- 在线快照保留最近三个已完成交易日，历史快照保留六个自然月；原逐键归档门禁不放宽。
- Raw截止：上海执行日零点减七个自然日；Manifest.created_at和IngestionRun.finished_at均严格早于截止点，运行已终态。
- 旧质量明细截止：上海执行日零点减30个自然日。只处理竞价序列的lot_precision/INFO与missing_source_timestamp/WARNING。
- 原Raw内容、Manifest和lineage不变；ERROR、非白名单质量、市场事实和历史版本不删除。
- 每次最多30分钟，Raw最多10,000个实际压缩对象、审计最多100,000条原明细，按200个候选分页；单个文件/归档工作单元最多256 MiB。
- SQL锁等待最多2秒、单语句最多30秒；预算采用流式检查，不承诺硬中断永久阻塞的OS I/O。
- 自动维护仅在03:00–04:00窗口开始；手工执行同样遵守采集保护窗口09:10–09:40。
- 容量80%警告，90%或可用空间小于2 GiB严重告警；暂存前至少512 MiB且覆盖当前单元保守上界。
- `.env`仍只用 `DATA_CLEANUP_ENABLED` 启停；无任意目标表、路径通配符、时间或保留期配置。
- 只通过有序migration变更schema；Worker不执行DDL、VACUUM、分区删除或权限扩张。
- 公共API、三个checked-in contracts、32轮竞价时刻及来源语义不变。
- 不新增依赖，不将通用清理框架、消息队列或对象存储放进本次改造。
- 本计划不授权推送、生产迁移、部署、首次清理、备份跳过或生产数据重放。

## 范围、依赖与文件结构

发布目录清理是独立交付，见 `2026-09-22-release-retention.md`；本计划可独立验证，不等待发布清理代码。
执行时先按worktree技能确认隔离环境；不得把已批准的设计文档丢在另一分支而让执行者看不到。

| 文件 | 职责/改动 |
| --- | --- |
| `src/market_data_center/raw_store.py` | 兼容gzip读取、安全路径、不可覆盖发布、校验后移除明文 |
| `src/market_data_center/storage_cleanup_models.py`（新） | 维护专用不可变记录和固定预算；不加入市场Domain Record |
| `src/market_data_center/storage_cleanup_service.py`（新） | Raw/质量/临时文件有界维护、预览和容量状态 |
| `src/market_data_center/quality_archive.py`（新） | 旧质量完整行的确定性编码、gzip归档和只读核验 |
| `src/market_data_center/persistence/storage_cleanup_postgres.py`（新） | 候选分页、登记/对比/精确删除、维护报告 |
| `src/market_data_center/call_auction_market_series_service.py` | 仅在质量结果构造处汇总两个非阻断规则 |
| `src/market_data_center/data_cleanup_service.py` | 保留现有删除规则，新增只读计数预览 |
| `src/market_data_center/persistence/postgres.py` | 原快照SQL增加局部timeout与只读预览；继续复用task_lock |
| `src/market_data_center/scheduler.py`、`scheduling_catalog.py`、`operations_service.py` | 固定步骤、时间门禁和partial报告 |
| `src/market_data_center/cli.py` | 默认预览、双确认执行、只读质量归档核验入口 |
| `src/market_data_center/dragon_tiger_recovery.py` | 恢复扫描理解压缩逻辑路径，仍不删除/压缩孤儿对象 |
| `src/market_data_center/recovery.py` | 备份恢复核对纳入三个元数据表 |
| `supabase/migrations/20260922000100_add_storage_lifecycle_cleanup.sql`（新） | 三个内部表、受限权限、必要索引，无历史数据变更 |
| `tests/test_storage_cleanup_service.py`、`test_quality_archive.py`（新） | 文件事务边界、预算、白名单、只读路径 |
| 已有Raw、竞价、Operations、Scheduler、CLI、集成测试 | 复用现有fixture和断言，不引入第二套测试环境 |

路径在2026-09-22已核对。当前集成fixture在 `tests/test_postgres_integration.py`，不在不存在的 `tests/integration/`。
当前数据库主键是 `quality_result_id`，不是 `quality_id`；所有SQL/归档均使用真实字段名。
若执行前发现迁移号被其他分支使用，只顺延新迁移号，不修改任何已发布迁移。

## Task 1: 先交付gzip兼容读取和不可变逻辑身份

**Files:** Modify `src/market_data_center/raw_store.py`、`dragon_tiger_recovery.py`；Test `tests/test_raw_store.py`、`test_dragon_tiger_recovery.py`、`test_reliability.py`。

**Interfaces:**
- 保持 `LocalRawStore.read_jsonl(manifest: RawManifest) -> tuple[Mapping[str, str], ...]`。
- 增加 `LocalRawStore.read_payload(object_path: str, *, max_bytes: int) -> bytes`，只做受限读取；有Manifest的调用者仍执行其SHA/字节/行数校验。
- 写入入口签名不变，逻辑路径存在明文或gzip任一表示都抛 `FileExistsError`。

- [x] **1. 写失败测试。** 在 `tests/test_raw_store.py` 增加以下完整用例，直接复用现有常量INGESTION_ID：

```python
def test_gzip_raw_reads_by_original_manifest_and_cannot_be_rewritten(tmp_path):
    import gzip
    from uuid import uuid4

    store = LocalRawStore(tmp_path)
    kwargs = dict(
        provider="baostock",
        dataset="security",
        partition_date=date(2026, 7, 28),
        ingestion_id=INGESTION_ID,
        rows=[{"code": "sh.600000"}],
        schema_version="baostock.security.v1",
    )
    stored = store.write_jsonl(**kwargs)
    manifest = RawManifest(
        uuid4(),
        INGESTION_ID,
        stored.object_path,
        RawFileFormat.JSONL,
        stored.content_sha256,
        stored.byte_size,
        stored.row_count,
        stored.schema_version,
    )
    plain = tmp_path / stored.object_path
    compressed = plain.with_name(plain.name + ".gz")
    compressed.write_bytes(gzip.compress(plain.read_bytes(), mtime=0))
    plain.unlink()
    assert store.read_jsonl(manifest) == ({"code": "sh.600000"},)
    with pytest.raises(FileExistsError):
        store.write_jsonl(**kwargs)
    assert not plain.exists()
```

- [x] **2. 运行RED。** `uv run pytest tests/test_raw_store.py -q`；新用例须因“不支持gzip/允许重写”失败，不是环境导入失败。
- [x] **3. 实现受限读取。** 路径解析检查空路径、绝对路径、盘符/反斜杠、`..`、所有父级软链接；解析后必须严格在Raw根内且是普通文件。原路径只在FileNotFound时fallback，PermissionError/损坏不fallback。gzip用流读取 `max_bytes + 1`，超限失败；捕获gzip截断/CRC错误为RawIntegrityError，不回显路径。核心分支：

```python
try:
    with plain.open("rb") as handle:
        payload = handle.read(max_bytes + 1)
except FileNotFoundError:
    with gzip.open(compressed, "rb") as handle:
        payload = handle.read(max_bytes + 1)
if len(payload) > max_bytes:
    raise RawIntegrityError("Raw object exceeds its byte bound")
```

`plain`/`compressed`均由同一个安全路径函数生成，且在打开前检查软链接。`read_jsonl`传 `manifest.byte_size`，继续原有JSON string-mapping、精确字节数、SHA和行数校验。writer在 `xb` 前后检查gzip碰撞，冲突时只清理自身新建明文，不动已有文件。

- [ ] **4. 补兼容测试并GREEN。** 测空对象、gzip截断/CRC、超长展开、原文损坏同时存在正确gzip、路径逃逸/符号链接、原文打开前被压缩删除、压缩后重复写。孤儿恢复扫描把 `.jsonl.gz` 规范为 `.jsonl` 逻辑路径，双表示去重，通过 `read_payload(..., max_bytes=50*1024*1024)`核验；未登记压缩对象不自动授权清理。已有RawReplayService/比较/手工修复都已用read_jsonl，不改其业务限制。

Run: `uv run pytest tests/test_raw_store.py tests/test_dragon_tiger_recovery.py tests/test_reliability.py -q`。Expected: PASS，无网络请求。`scripts/bulk_collect_daily_bars.py`的read_bytes读取TDX `.day`，不属于Raw，不改。
- [x] **5. 提交。** `git add src/market_data_center/raw_store.py src/market_data_center/dragon_tiger_recovery.py tests/test_raw_store.py tests/test_dragon_tiger_recovery.py tests/test_reliability.py`；`git commit -m "feat: read compressed Raw without changing logical identity"`。本次同时提交读取兼容说明和实施进度，真实软链接验收仍按第4步保留未完成项。

## Task 2: 三个内部元数据表、类型与最小权限

**Files:** Create迁移、`storage_cleanup_models.py`、`persistence/storage_cleanup_postgres.py`；Modify `tests/test_postgres_integration.py`。

**Interfaces:** 模型集中定义，后续任务直接import，不复制结构。

```python
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID
from market_data_center.domain.ingestion import RawManifest


@dataclass(frozen=True, slots=True)
class CompressedRawObject:
    object_path: str
    compressed_sha256: str
    compressed_bytes: int


@dataclass(frozen=True, slots=True)
class RawCompressionCandidate:
    manifest: RawManifest
    created_at: datetime
    finished_at: datetime
    representation: CompressedRawObject | None


@dataclass(frozen=True, slots=True)
class CleanupStepResult:
    step_code: str
    scanned: int = 0
    completed: int = 0
    failed: int = 0
    pending: int | None = None
    original_bytes: int = 0
    compressed_bytes: int = 0
    removed_bytes: int = 0
    elapsed_ms: int = 0
    error_codes: tuple[str, ...] = ()
    truncated: bool = False

    @property
    def partial(self) -> bool:
        return bool(self.failed or self.truncated or self.pending)
```

计数验证非负；`pending=None`表示未完成全量计数，不得显示零。remaining精确count若超30秒，改为unknown且记录受控错误，不阻止已经安全完成的维护。

- [ ] **1. 写migration失败测试。** 在现有集成文件中直接使用其 `database_engine` fixture：

```python
def test_storage_cleanup_tables_are_private(database_engine):
    with database_engine.connect() as connection:
        for table in (
            "ingestion.raw_compression",
            "audit.quality_archive",
            "operations.data_cleanup_report",
        ):
            assert (
                connection.execute(text("select to_regclass(:name)"), {"name": table}).scalar_one()
                == table
            )
            for role in ("anon", "authenticated", "market_data_api"):
                assert not connection.execute(
                    text("select has_table_privilege(:role, :name, 'select')"),
                    {"role": role, "name": table},
                ).scalar_one()
```

- [ ] **2. 运行RED。** `uv run pytest tests/test_postgres_integration.py -k storage_cleanup -q`。必须使用明确隔离的 `TEST_DATABASE_URL`；缺失则记录blocked integration，不得读取生产DATABASE_URL作为替代。
- [ ] **3. 写迁移和模型。** 三表字段精确如下，计数bigint、时间timestamptz、ID uuid、SHA小写64hex；路径只允许受控相对POSIX路径。

```sql
-- ingestion.raw_compression
-- raw_id PK/FK raw_manifest, object_path UNIQUE, encoding CHECK = 'gzip',
-- compressed_bytes >= 0, compressed_sha256, verified_at NOT NULL,
-- plaintext_removed_at NULL; only its NULL -> non-NULL confirmation is mutable.
-- audit.quality_archive
-- archive_id PK, ingestion_id FK, rule_code, schema_version CHECK = 'quality_archive.v1',
-- object_path UNIQUE, content_sha256, content_bytes, compressed_sha256,
-- compressed_bytes, row_count > 0, first_created_at, last_created_at, archived_at,
-- UNIQUE(ingestion_id, rule_code), first_created_at <= last_created_at.
-- operations.data_cleanup_report
-- workflow_run_id PK/FK workflow_run, reference_date,
-- started_at, finished_at NULL, cutoffs jsonb, steps jsonb, disks jsonb,
-- owned_temps jsonb NOT NULL DEFAULT '[]', status running/succeeded/partial/failed.
```

完整DDL须使用显式字段/约束；注释列单即schema契约，不使用 `create_all`。对report的 `workflow_run_id`验证其workflow为data_cleanup。report的owned_temps存operation_id、相对路径、创建时刻、所属Raw或批次ID、expected content hash、阶段和终态；写文件前提交这条登记，不再增加第四张任务表。报告只允许受限结构，不存绝对路径、凭据、payload。

启用RLS；Worker对raw_compression SELECT/INSERT/仅plaintext_removed_at列UPDATE，用trigger拒绝重复变更时间和变更内容身份；quality_archive只SELECT/INSERT；report SELECT/INSERT及指定报告字段UPDATE。其他角色无权，Worker无上述表DELETE。

先DROP `quality_result_worker_all`，重建等价SELECT、INSERT策略，再增加受限DELETE策略和GRANT。DELETE策略同时要求：两条规则对应级别、dataset=call_auction_market_series、非汇总行、同批次已终态、created_at早于上海当日零点30天、同ingestion/rule已有quality_archive且时间在登记范围内。SQL安全门禁核心：

```sql
dataset_code = 'call_auction_market_series'
and ((rule_code = 'realtime_quote.lot_precision' and severity = 'info')
  or (rule_code = 'realtime_quote.missing_source_timestamp' and severity = 'warning'))
and not (details ? 'aggregation_version')
and created_at < ((((current_timestamp at time zone 'Asia/Shanghai')::date - 30)::timestamp)
                  at time zone 'Asia/Shanghai')
```

不能仅叠加permissive DELETE policy；旧FOR ALL会绕过。用exists连接run/archive完成其余限制，SQL仍按精确ID删除，不把RLS当文件验证替代。为候选增加窄索引：Raw(local/jsonl)按byte_size desc,created_at,raw_id；白名单非汇总质量按ingestion_id,rule_code,created_at,quality_result_id；终态run复用/核对现有索引。不得在migration中压缩、回填或删除数据。
- [ ] **4. 实现Persistence并GREEN。** 构造器 `PostgreSQLStorageCleanupPersistence(engine: Engine)`；提供 `start_report(workflow_run_id: UUID, reference_date: date) -> None`、`record_temp(workflow_run_id: UUID, operation_id: UUID, relative_path: str, owner: Mapping[str, str]) -> None`、`update_temp_stage(workflow_run_id: UUID, operation_id: UUID, stage: str) -> None`、`finish_report(workflow_run_id: UUID, report: Mapping[str, object]) -> None`。temp阶段只能为registered/published/committed/removed/failed，更新owned_temps对应operation的有限状态，不覆盖其归属；归档/Raw在各发布边界更新，进程崩溃以所属workflow和实际有效副本恢复。确认temp已不存在且正式对象登记有效后，removed阶段从owned_temps移除此项并保留累积计数；只保留待恢复项，不把每天10,000个成功操作历史重复写入同一个JSONB数组。每个写事务 `SET LOCAL lock_timeout='2s'; SET LOCAL statement_timeout='30s'`。测试唯一性/负数/坏SHA/路径/非法状态、不可变UPDATE、跨workflow报告拒绝、Worker不能删ERROR/其他规则/无归档/新行/汇总，原SELECT/INSERT仍可用。
- [ ] **5. 提交。** 仅暂存本任务迁移、两个模块和集成测试；`git commit -m "feat: add private storage maintenance metadata and grants"`。

## Task 3: Raw压缩发布、登记与恢复

**Files:** Modify `raw_store.py`、`storage_cleanup_models.py`、`persistence/storage_cleanup_postgres.py`；Create `storage_cleanup_service.py`、`tests/test_storage_cleanup_service.py`；Modify Raw和集成测试。

**Interfaces:**
- `LocalRawStore.prepare_compression(manifest: RawManifest, *, operation_id: UUID, checkpoint: Callable[[], None]) -> CompressedRawObject`：写/校验/发布gzip，绝不删原件或写DB。
- `LocalRawStore.remove_verified_plaintext(manifest: RawManifest, compressed: CompressedRawObject, *, checkpoint: Callable[[], None]) -> int`：重新核验双副本、文件身份，返回实际unlink的明文字节；原件已不存在则验证gzip返回0。
- Persistence：`raw_candidates(cutoff: datetime, *, after: tuple[int, datetime, UUID] | None, limit: int=200) -> tuple[RawCompressionCandidate, ...]`；`register_compression(candidate: RawCompressionCandidate, compressed: CompressedRawObject, *, cutoff: datetime) -> None`；`confirm_plaintext_removed(raw_id: UUID, removed_at: datetime) -> None`。
- `StorageCleanupService(repository, raw_store, *, checkpoint: Callable[[], None])`；`compress_raw(cutoff: datetime, workflow_run_id: UUID) -> CleanupStepResult`。repository为上述具体类，测试可用最小fake，不增加单实现接口框架。

- [ ] **1. 写失败测试。** 复用Task 1的write+manifest构造，新增如下断言序列：

```python
compressed = store.prepare_compression(manifest, operation_id=uuid4(), checkpoint=lambda: None)
assert (tmp_path / manifest.object_path).exists()
assert compressed.object_path == manifest.object_path + ".gz"
assert store.read_jsonl(manifest) == ({"code": "sh.600000"},)
assert (
    store.remove_verified_plaintext(manifest, compressed, checkpoint=lambda: None)
    == manifest.byte_size
)
assert store.read_jsonl(manifest) == ({"code": "sh.600000"},)
assert store.remove_verified_plaintext(manifest, compressed, checkpoint=lambda: None) == 0
```

每个测试在本文件完整构造manifest；不用生产文件。Service的fake register_compression抛异常时，原文必须仍在，remove方法调用次数为零。
- [ ] **2. 运行RED。** `uv run pytest tests/test_raw_store.py tests/test_storage_cleanup_service.py -q`，预期新方法缺失失败。
- [ ] **3. 实现文件顺序与事务。** 使用固定1 MiB块、gzip level6、mtime=0、独占本次临时文件、flush/fsync；原文与解压回读都校验Manifest的SHA/字节/JSONL行数。用 `os.link(temp, destination)` 实现同文件系统原子no-replace发布，然后fsync父目录并unlink本次temp；FileExists时验证已有目标，不使用会覆盖的os.replace。平台/文件系统不支持安全发布则保留原文并显式失败，不能退化到覆盖或先删目标。

Service顺序固定：

```python
repository.record_temp(
    workflow_run_id, operation_id, temp_relative_path, {"raw_id": str(candidate.manifest.raw_id)}
)
compressed = raw_store.prepare_compression(
    candidate.manifest, operation_id=operation_id, checkpoint=checkpoint
)
repository.register_compression(candidate, compressed, cutoff=cutoff)
checkpoint()
removed = raw_store.remove_verified_plaintext(candidate.manifest, compressed, checkpoint=checkpoint)
repository.confirm_plaintext_removed(candidate.manifest.raw_id, datetime.now(UTC))
```

temp路径由固定 `raw_temp_path(object_path: str, operation_id: UUID) -> str`生成，函数放raw_store.py并供Service导入，不能在两处自行拼不同路径。删除前检查stat身份、原始hash、gzip hash；过程中路径变化失败。源文件不是普通文件/符号链接失败；目录锁不足时不做有风险的replace。

register事务重新检查Manifest所有身份字段、run终态和截止条件。重复登记必须字段完全相同；不同hash硬失败。分页排序 `byte_size DESC, created_at, raw_id`，游标条件匹配混合顺序；排除已确认移除记录。oversized/损坏行推进游标并报告，不能永远挡住后续文件。完成10,000项或到时间即退出；未压缩成功的继续可重试。
- [ ] **4. 故障点测试并GREEN。** 分别在temp写入、fsync、回读、发布、DB登记前后、原文删除前后、确认前注入异常；验证唯一有效副本从不丢失，重跑不重复登记。另测已有gzip未登记、登记完成但原文在、确认未写但原文已删、内容碰撞、并发read、磁盘不足、超256MiB和每块checkpoint取消。集成测试两连接候选变化/登记回滚；断言原Manifest字段完全不变。

Run: `uv run pytest tests/test_raw_store.py tests/test_storage_cleanup_service.py -q`；`uv run pytest tests/test_postgres_integration.py -k 'storage_cleanup or raw_compression' -q`。
- [ ] **5. 提交。** 仅暂存本任务列出的文件；`git commit -m "feat: compress registered Raw with verified crash recovery"`。

## Task 4: 新竞价质量提示按批次无损汇总

**Files:** Modify `call_auction_market_series_service.py`、`tests/test_call_auction_market_series_service.py`。

**Interfaces:** 同模块增加 `aggregate_series_quality(results: Sequence[QualityResult]) -> tuple[QualityResult, ...]`；原 `_quality_results`在return前调用。后续旧质量归档也复用此纯函数。

- [ ] **1. 写失败测试。** 在已有测试import新函数，完整构造QualityResult如下：

```python
def test_series_quality_preserves_findings_and_errors():
    from uuid import uuid4
    from market_data_center.domain.ingestion import (
        DatasetCode,
        QualityResult,
        QualitySeverity,
        QualityStatus,
    )
    from market_data_center.call_auction_market_series_service import aggregate_series_quality

    ingestion_id = uuid4()
    rows = [
        QualityResult(
            uuid4(),
            ingestion_id,
            DatasetCode.CALL_AUCTION_MARKET_SERIES,
            "realtime_quote.lot_precision",
            QualitySeverity.INFO,
            QualityStatus.FAILED,
            "non-integral lots",
            {"symbol": code, "observed_at": "2026-09-22T09:15:00+08:00"},
        )
        for code in ("SSE:600000", "SZSE:000001")
    ]
    error = QualityResult(
        uuid4(),
        ingestion_id,
        DatasetCode.CALL_AUCTION_MARKET_SERIES,
        "call_auction_market_series.missing_symbol",
        QualitySeverity.ERROR,
        QualityStatus.FAILED,
        "missing",
        {"symbol": "SSE:600001"},
    )
    result = aggregate_series_quality((*rows, error))
    summary = next(row for row in result if row.severity is QualitySeverity.INFO)
    assert summary.details["aggregation_version"] == "auction_series_quality.v1"
    assert summary.details["finding_count"] == 2
    assert summary.details["affected_keys"] == [dict(row.natural_key) for row in rows]
    assert summary.status is QualityStatus.FAILED
    assert error in result
```

- [ ] **2. 运行RED。** `uv run pytest tests/test_call_auction_market_series_service.py -k quality -q`。
- [ ] **3. 最小实现。** 不改Validator，按以下key分组；只接受匹配dataset、rule/severity的两条规则且没有aggregation_version的行。其他对象原样返回。affected_keys保持每个finding的完整自然键和确定次序，不去重后丢原finding个数；没有自然键或存在非空details的意外新结构原样保留并不扩大白名单。

```python
group_key = (row.ingestion_id, row.rule_code, row.severity, row.status, row.message)
details = {
    "aggregation_version": "auction_series_quality.v1",
    "finding_count": len(group),
    "affected_keys": [dict(item.natural_key) for item in group],
}
```

用新quality_result_id生成汇总，natural_key=None，severity/status/message/ingestion不变。GROUP多个message时生成多条汇总。既有汇总再次传入原样保留。质量存储行数与findings数分别表达，不能修改行情accepted/rejected数量。
- [ ] **4. GREEN及竞价回归。** 测不同ingestion/message/status不混合、同规则ERROR不聚合、其他dataset不变、零finding、重复输入自然键保留数量、已汇总幂等、missing_symbol/归一化异常仍阻断。Run: `uv run pytest tests/test_call_auction_market_series_service.py tests/test_call_auction_market_series_writer.py -q`。
- [ ] **5. 提交。** `git add src/market_data_center/call_auction_market_series_service.py tests/test_call_auction_market_series_service.py`；`git commit -m "perf: aggregate repetitive auction quality findings"`。

## Task 5: 旧白名单质量先归档、逐条校验后精确删除

**Files:** Create `quality_archive.py`、`tests/test_quality_archive.py`；Modify维护models/service/persistence、集成测试。

**Interfaces:**
- `QualityArchiveGroup`：ingestion_id:UUID、rule_code:str、row_count:int、first_created_at:datetime、last_created_at:datetime。
- `QualityArchiveObject`：archive_id:UUID、group:QualityArchiveGroup、object_path:str、content_sha256:str、content_bytes:int、compressed_sha256:str、compressed_bytes:int。
- `load_quality_rows(group: QualityArchiveGroup) -> tuple[Mapping[str, object], ...]`：每行所有真实字段，UUID和timestamp可序列化表示。
- `write_quality_archive(root: Path, group: QualityArchiveGroup, rows: Sequence[Mapping[str, object]], *, operation_id: UUID, checkpoint: Callable[[], None]) -> QualityArchiveObject`。
- `read_quality_archive(root: Path, archive: QualityArchiveObject) -> tuple[Mapping[str, object], ...]`：有界全量验证才返回。
- Persistence `quality_candidates(cutoff: datetime, *, after: tuple[UUID, str] | None, limit: int=200) -> tuple[QualityArchiveGroup, ...]`；`commit_quality_archive(archive: QualityArchiveObject, rows: Sequence[Mapping[str, object]], summaries: Sequence[QualityResult]) -> int`。
- Service `archive_quality(cutoff: datetime, workflow_run_id: UUID) -> CleanupStepResult`。

- [ ] **1. 写失败测试。** 用一条包含全部字段的合成记录验证字节/语义往返，不使用生产payload：

```python
row = {
    "quality_result_id": "00000000-0000-0000-0000-000000000001",
    "ingestion_id": "00000000-0000-0000-0000-000000000002",
    "dataset_code": "call_auction_market_series",
    "rule_code": "realtime_quote.lot_precision",
    "severity": "info",
    "status": "failed",
    "natural_key": {"symbol": "SSE:600000", "observed_at": "2026-08-01T01:15:00+00:00"},
    "message": "non-integral lots",
    "details": {"original": "preserve"},
    "created_at": "2026-08-01T01:15:01+00:00",
}
```

构造group的row_count=1、首末时刻等于row时刻，传 `rows=(row,)`；断言 `read_quality_archive(root, archive) == (row,)`。修改gzip字节须抛RawIntegrityError；测试DB提交失败原audit行不变。
- [ ] **2. 运行RED。** `uv run pytest tests/test_quality_archive.py -q`。
- [ ] **3. 实现确定性完整归档。** 严格quality_result_id排序，全字段UTF-8 JSONL，`sort_keys=True,separators=(',', ':'),ensure_ascii=False`；旧jsonb数字使用Decimal无损编码或直接从SQL取确定性jsonb文本，禁止float往返损失。timestamp统一为明确UTC ISO表示但值不改变；schema_version写metadata。用gzip固定mtime及no-replace发布，最终路径为批准的content-sha路径；临时路径由operation_id登记。sha、rows、字段和精确ID全回读一致后才调用DB提交。

候选只返回整个ingestion/rule组的所有未聚合白名单行，`max(created_at) < cutoff`。组跨截止日则全部等下一次；不得部分归档同一唯一组后让后半组无法登记。组超过剩余100,000行预算或256MiB时跳过并记partial，不偷偷分片改变唯一键。

事务使用 `SELECT ... FOR UPDATE`重读该组全部未聚合行，按相同规范逐字段比较，再登记归档、插入保留受影响键/计数的汇总、删除精确ID：

```sql
delete from audit.quality_result
where quality_result_id = any(cast(:quality_ids as uuid[]))
  and ingestion_id = :ingestion_id and rule_code = :rule_code
returning quality_result_id;
```

返回ID集合必须等于本次归档集合，否则抛异常整事务回滚。已存在相同归档而无旧明细返回0；登记不同SHA/行数、仍有不一致明细硬失败。旧details非空也完整归档；若不满足Task4汇总函数输入，生成保留该组finding数及全部原自然键的归档摘要，并保留archive_id/hash引用，不能丢details原档。同组多个message/severity/status分别摘要，原创建时间范围保存在archive中。
- [ ] **4. GREEN和角色回归。** 测完整字段/Unicode/null/Decimal/时区/重复ID、记录增减或变更、已有不同内容文件、归档缺失/损坏、回读超限、过期边界、跨截止组、部分成功/failed run可归档、running排除、非白名单/ERROR不动；事务任何阶段失败都不删原行。Run: `uv run pytest tests/test_quality_archive.py tests/test_storage_cleanup_service.py -q`；`uv run pytest tests/test_postgres_integration.py -k 'quality_archive or storage_cleanup' -q`。
- [ ] **5. 提交。** 暂存本任务文件；`git commit -m "feat: archive legacy auction quality evidence before deletion"`。

## Task 6: 有界维护、只读预览、临时文件和容量报告

**Files:** Modify维护models/service/persistence、`data_cleanup_service.py`、`persistence/postgres.py`、`cli.py`；Test维护、快照、CLI与集成测试。

**Interfaces:**
- `CleanupBudget(deadline: float, *, monotonic: Callable[[], float], cancelled: Callable[[], bool])`，`checkpoint() -> None`超时/锁丢失抛受控维护中断。
- `StorageCleanupService.preview(reference_date: date) -> Mapping[str, object]`，不写文件/DB/Operations。
- `StorageCleanupService.cleanup_temps(cutoff: datetime, workflow_run_id: UUID) -> CleanupStepResult`；`disk_report() -> tuple[Mapping[str, object], ...]`。
- 原 `DataCleanupService.run(reference_date)`保留；新增 `preview(reference_date) -> Mapping[str, object]`只计算cutoff、待删除数和缺归档数。
- CLI `data-cleanup [--execute --confirm]`、`quality-archive-inspect --ingestion-id UUID`（只读，输出校验/数量/字段，不输出整个源内容）。

- [ ] **1. 写失败测试。** CLI复用现有 `cli._parser()`，无需任何数据库：

```python
def test_data_cleanup_defaults_to_preview():
    from market_data_center.cli import _parser

    args = _parser().parse_args(["data-cleanup"])
    assert not args.execute and not args.confirm
```

Service测试fake持久化所有写方法设为抛AssertionError，tmp_path只读目录快照前后相同；preview必须成功。模拟monotonic推进到deadline之后，checkpoint失败且所有尚未验证原件保留。
- [ ] **2. 运行RED。** `uv run pytest tests/test_cli.py tests/test_data_cleanup_service.py tests/test_storage_cleanup_service.py -q`。
- [ ] **3. 实现预算和preview。** 固定常量与Spec完全一致；连续时间使用monotonic，截止日期来自aware Shanghai执行日。每个SQL事务SET LOCAL timeout，文件每块checkpoint，singletons使用 `PostgreSQLPersistence.task_lock('maintenance:data_cleanup', on_lock_lost=cancel_event.set)`，不用新锁实现。不创建脱离Worker的压缩线程。preview使用READ ONLY事务，不获取写入型任务状态、不创建report/temp/Raw根；未知容量不估算为0。

```python
def maintenance_cutoffs(reference_date):
    midnight = datetime.combine(reference_date, time.min, ZoneInfo("Asia/Shanghai"))
    return midnight - timedelta(days=7), midnight - timedelta(days=30)
```

上述函数放storage_cleanup_service.py。它需要datetime/time/timedelta/ZoneInfo显式import；tests断言2026-09-22得到09-15零点和08-23零点。

容量使用 `shutil.disk_usage`，按 `st_dev`去重Raw/质量归档实际文件系统；usage用整数比例，不对市场值引入float。工作单元保守临时空间至少 `2 * input_bytes + 1 MiB`，同时free>=512MiB；archive也计入未压缩编码峰值。超限记录稳定code，不“紧急删除”。snapshot原有校验删除继续独立事务并增加2s/30s限时。

临时文件候选只读report登记的owned_temps，不glob扫描Raw；确认所属workflow终态、age严格>7天、相对路径身份未变、可用正式原文或gzip/质量归档能够证明不是唯一副本，再只unlink精确文件。报告状态只允许从运行中到终态；旧failed report的temp也可验证恢复，未知文件只列异常。更新所有步骤statistics和已删除文件实际字节，DB删除行不换算释放字节。

CLI执行必须execute与confirm同时true（只有其一报参数错误）；实际运行先拒绝09:10–09:40开始，并把deadline限制为30分钟和下一09:10两者较早者，避免09:00手工启动延伸进入采集窗口。preview随时可运行且不读取secret到输出。退出码成功0、partial/failed1、参数错误2，沿用JSON脱敏输出模式。
- [ ] **4. GREEN与安全测试。** 覆盖单元过大/磁盘阈值/不同挂载点/统计未知、跨零点、周末、闰年、任务预算、锁丢失、连续坏候选不饿死后续分页、单批SQLtimeout、合法/未知/活跃/唯一副本temp和目录逃逸；确认没有业务表或Raw登记之外的数据被删除。Run同RED命令；集成运行 `-k 'storage_cleanup or cleanup or raw_compression or quality_archive'`。
- [ ] **5. 提交。** 暂存本任务文件；`git commit -m "feat: add bounded storage cleanup preview and reporting"`。

## Task 7: 接入现有03:00 Worker与Operations

**Files:** Modify `scheduler.py`、`scheduling_catalog.py`、`operations_service.py`、维护Service；Test `test_scheduler.py`、`test_operations.py`、`test_storage_cleanup_service.py`。

**Interfaces:** 增加 `run_storage_cleanup(settings: WorkerSettings, *, execution: WorkflowExecution, now: datetime) -> Mapping[str, object]`（在storage_cleanup_service.py）；由scheduled与manual入口共用，preview绝不调用。代码目录step_codes与实际执行一致：

```python
(
    "inspect_storage_before",
    "cleanup_call_auction_market_series_snapshots",
    "compress_registered_raw",
    "archive_auction_quality",
    "cleanup_owned_temps",
    "inspect_storage_after",
)
```

- [ ] **1. 写失败测试。** 替换原operations目录只含单步骤的断言为上述tuple；scheduler在Shanghai04:00运行时，断言任何DELETE/压缩方法均未调用并记录维护窗口错过；03:00按固定顺序调用，02:30归档任务保持原注册。
- [ ] **2. 运行RED。** `uv run pytest tests/test_scheduler.py tests/test_operations.py -q`。
- [ ] **3. 编排最小实现。** 创建report、获取共享维护锁、容量前照、现有snapshot.run、Raw/quality/temp、容量后照，均使用同一budget。把CleanupStepResult纳入 `_result_statistics`：fetched=scanned、accepted=completed、rejected=failed，partial取模型属性；磁盘字节留report，不伪装行情行数。

```python
if isinstance(result, CleanupStepResult):
    status = ExecutionStatus.PARTIAL if result.partial else ExecutionStatus.SUCCEEDED
    return result.scanned, result.completed, result.failed, status
```

错误/预算不足的步骤保留pending和error_code；个别文件失败仍可继续独立候选。原snapshot归档失败抛出并阻断在线删，DB不可用/锁失效/空间不足停止新文件写；finally尝试后照与finish_report，不能覆盖主错误。Operations外层仍调用execution.fail或succeed，不能把预算截断记全成功。

默认启用开关、cron03:00不改。现有catalog.timeout_seconds实际用于APScheduler misfire而不是硬执行中止：只将该job值改为3600秒，Service独立执行1800秒协作预算；不把两者误当同一个超时。入口再用实际上海时刻检查 `[03:00,04:00)`。迟到越界没有SKIPPED状态时用CleanupStepResult(truncated=True)及 `outside_maintenance_window`记录PARTIAL，不伪造成功。禁用配置继续不注册整个任务，不增加新的env时间参数。
- [ ] **4. GREEN。** 验证scheduled/manual争同一锁、禁用开关、窗口边界、partial传播、第二步失败后report收尾、恢复不碰正在运行的report；32轮调度及09:24:53/09:25:20不变。Run: `uv run pytest tests/test_scheduler.py tests/test_operations.py tests/test_storage_cleanup_service.py tests/test_call_auction_market_series.py -q`。
- [ ] **5. 提交。** 暂存本任务文件；`git commit -m "feat: extend the existing nightly cleanup workflow safely"`。

## Task 8: 恢复说明、隔离验收与交付

**Files:** Modify `src/market_data_center/recovery.py`、`tests/test_postgres_integration.py`；Create `tests/test_storage_cleanup_recovery.py`；Docs `README.md`、`docs/Raw重放与运行恢复.md`、`docs/Worker调度系统.md`、`docs/最小生产发布运行手册.md`、`docs/领域详设-Operations-2026-08-02.md`、本Spec。

- [ ] **1. 写恢复验收失败断言。** 现有backup/restore fixture增加三个表数据并断言恢复快照行数一致；`COUNT_QUERIES`必须包含下列常量，不省略压缩登记：

```python
"raw_compression": "select count(*) from ingestion.raw_compression",
"quality_archive": "select count(*) from audit.quality_archive",
"data_cleanup_report": "select count(*) from operations.data_cleanup_report",
```

新单测 `tests/test_storage_cleanup_recovery.py`直接断言COUNT_QUERIES包含上述三个键和值，不连接数据库。Run: `uv run pytest tests/test_storage_cleanup_recovery.py -q`及 `uv run pytest tests/test_postgres_integration.py -k backup -q`，先确认新增断言RED。仓库没有 `tests/test_recovery.py`，不要把它当已有fixture来源。
- [ ] **2. 更新代码与运行文档。** 加入以上计数，保持APPLICATION_SCHEMAS现有包含的ingestion/audit/operations；本次不重构其他备份缺口。说明数据库备份不能替代Raw文件备份，后者必须包含gzip和 `_quality_archive`；旧明文reader不兼容压缩后的物理状态，回退只选兼容release或经单独授权的校验展开。只在代码真正完成后把文档“尚未实现”更新为“已实现、待生产部署”。
- [ ] **3. 执行完整本地门禁。** 依赖未安装时才 `uv sync --all-groups --locked`；依次运行并保存真实输出：

```text
uv run ruff format --check .
uv run ruff check .
uv run mypy src
uv run pytest
uv run pytest -m integration
git diff --check
```

integration仅隔离TEST_DATABASE_URL；不可用就明确阻塞，不将skip当PASS。测试使用合成5MiB/10000finding样本测压缩、archive峰值内存和耗时，不访问实时行情、生产Raw或生产数据库；报告测试环境数据，不把压缩样本比例当生产全部容量承诺。
- [ ] **4. 逐项安全复核。** 用spec§9矩阵核对测试；`git diff -- contracts`应为空。确认新SQL无宽泛DELETE和Worker DDL，公共角色不能读三个表，gzip后Raw重放同内容，ERROR记录数不变，preview前后DB/files无差异。按requesting-code-review技能安排适用的复核，发现问题先修再跑相关门禁。
- [ ] **5. 提交并交付待部署版本。** `git commit -m "docs: document safe storage cleanup operations and recovery"`（先精确暂存本任务文件）。汇报提交、门禁及未完成项；生产只能在另一次明确授权后按“隔离门禁→保护workflow迁移→兼容reader部署→preview→授权执行”推进。不得把执行本计划视为授权第一次生产DELETE。

## 覆盖检查与执行选择

| Spec | 计划覆盖 |
| --- | --- |
| §1–3范围/内部表 | Global Constraints、Task2 |
| §4 Raw及读写身份 | Task1、Task3 |
| §5新汇总/旧全字段归档 | Task4、Task5 |
| §6临时/发布物 | Task6；独立release-retention计划 |
| §7预算/preview/容量/调度 | Task6、Task7 |
| §8RLS/回退/物理空间 | Task2、Task8；不主动VACUUM |
| §9验收 | 各任务RED/GREEN、Task8完整门禁 |

以上复用现有模块和标准库，不新建通用清理平台。用户已选择当前任务内按executing-plans逐项实施，
在当前目录使用 `codex/storage-lifecycle-cleanup` 分支，不创建worktree，不使用子代理。

## 实施记录（2026-09-22）

- Task1代码和兼容测试已完成；RED确认17项因缺失行为失败，随后定向验证53项通过、3项跳过。
- Task1第4步仍待Linux真实软链接验证：Windows当前权限不能创建测试软链接，未绕过权限。
- 本地全量：`uv run --no-sync pytest -q -p no:cacheprovider` 为939通过、100跳过；
  其中96项未配置隔离TEST_DATABASE_URL、1项缺少pg_dump、3项软链接权限受限。跳过不算验收通过。
- `uv run --no-sync ruff format --check .`、`uv run --no-sync ruff check .`、
  `uv run --no-sync mypy src` 均通过；计划中Python代码块同步由formatter格式化，无设计变更。
- 后续数据库验收按用户选择使用Docker Desktop本机一次性PostgreSQL。当前Docker引擎管道不存在，
  Task2迁移和权限测试等待引擎启动；不使用生产库或历史远程测试库替代。
- 当前未执行压缩、数据删除、推送、生产迁移或部署；其他实施任务尚未开始。
