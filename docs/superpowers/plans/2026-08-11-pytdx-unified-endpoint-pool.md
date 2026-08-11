# PYTDX 统一能力节点池 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 用一个由 Worker 启动时及每 12 小时自动刷新的、带能力标记的 PYTDX 节点池，取代 Daily Bar 与五档行情的全部旧 endpoint 配置，同时保留 `PYTDX_VIPDOC_PATH` 本地日线优先语义。

**Architecture:** 新增 `providers.pytdx_pool` 作为唯一的节点池读写、探测、验证和能力筛选边界；Daily Bar 与五档 Provider 只消费不可变池快照。Worker 在取得全局调度锁后先刷新池，再构建 APScheduler；刷新失败时仅可回退到最后一个有效池，没有有效池则拒绝启动。定时刷新作为受控 `operations` workflow 运行，不创建 IngestionRun。

**Tech Stack:** Python 3.12、Pydantic Settings、pytdx、APScheduler、SQLAlchemy/PostgreSQL、pytest、uv、Ruff、mypy。

## Global Constraints

- 先满足 GitHub Issue、accepted ADR、领域详设和 migration 治理门禁，再修改运行时代码。
- 开始 Task 0 前先按 `superpowers:using-git-worktrees` 征得用户同意并建立隔离 worktree；不得直接在当前 dirty `master` 上执行实现。
- 每项行为变更严格执行红—绿—重构：先写一个会按预期失败的测试，再写最小实现。
- 网络探测测试全部使用 fake client/candidate，不访问真实公共节点。
- 不运行生产 migration、生产采集、服务器发布或凭据操作；这些需要用户另行明确授权。
- 不注册 cron、Windows Task Scheduler 或其他 OS 级采集/刷新触发器。
- 不提交池文件、Raw 行情、`.env`、数据库 URL、服务器地址或密钥。
- Daily Bar 继续本地 vipdoc 优先；远程失败与 BSE 无可用节点必须保持显式缺口。
- 一个成功的采集批次固定一个实际 endpoint，并继续把 endpoint 写入 Raw request metadata。
- 公共 `api_v1`、FastAPI 和 agent-tools 契约不变。

---

### Task 0: 完成治理前置条件

**Files:**

- Create: GitHub Issue（仓库 issue tracker）
- Create: `docs/adr/ADR-0026-统一PYTDX能力节点池.md`
- Modify: `docs/adr/README.md`
- Modify: `docs/adr/ADR-0024-远程TDX日K数据源.md`
- Modify: `docs/领域详设-RemoteTdxDailyBar-2026-08-09.md`
- Create: `supabase/migrations/20260811000300_constrain_operations_workflow_codes.sql`
- Modify: `tests/test_postgres_integration.py`
- Modify: `tests/test_production_checks.py`
- Verify: `docs/项目宪法-MarketDataCenter-2026-07-24.md`
- Verify: `docs/股票数据中心技术方案.md`
- Reference: `docs/superpowers/specs/2026-08-11-pytdx-unified-endpoint-pool-design.md`

- [ ] **Step 1: 恢复 GitHub CLI 认证并确认仓库**

Run:

```powershell
gh auth status
gh repo view --json nameWithOwner,url
```

Expected: 两条命令退出码均为 `0`，且仓库与当前工作区远端一致。若认证仍失败，在此停止，不创建 ADR 或实现代码。

- [ ] **Step 2: 创建唯一的实现 Issue**

Run:

```powershell
gh issue create --title "统一 PYTDX 能力节点池并由 Worker 每 12 小时刷新" --body-file docs/superpowers/specs/2026-08-11-pytdx-unified-endpoint-pool-design.md
```

Expected: 返回一个当前仓库的 Issue URL。记录 URL 中的数字标识，后续 ADR 的“关联 Issue”必须写入该真实数字。

- [ ] **Step 3: 写 accepted ADR，并明确替代 ADR-0024 的配置决定**

`docs/adr/ADR-0026-统一PYTDX能力节点池.md` 至少包含以下决定正文：

```markdown
## 决定

1. Daily Bar 与五档行情只读取 `PYTDX_POOL_PATH` 指向的
   `pytdx.endpoint_pool.v1` 文件，并按 capability 选择节点。
2. Worker 取得全局调度锁后执行启动刷新，随后每
   `PYTDX_POOL_REFRESH_HOURS`（默认 12）小时刷新。
3. 刷新失败不得覆盖最后一个有效池；新旧池均无效时 Worker 拒绝启动。
4. 删除 `PYTDX_DAILY_BAR_ENDPOINTS`、`PYTDX_DAILY_BAR_POOL_PATH`、
   `PYTDX_HQ_HOST`、`PYTDX_HQ_PORT`、`PYTDX_HQ_POOL_PATH`。
5. `PYTDX_VIPDOC_PATH` 保留，且本地 `.day` 文件继续优先。
```

在 ADR-0024 顶部增加“本 ADR 的显式 endpoint 与禁止运行时发现决定已由 ADR-0026 替代；Daily Bar 领域语义仍有效”。在 `docs/adr/README.md` 增加 ADR-0026 索引。同步修改领域详设：用统一 v1 pool、市场 capability、vipdoc 本地优先、last-good、显式 BSE 缺口和单批次 endpoint 固定取代显式 endpoint 配置。

- [ ] **Step 4: 校验文档中没有未解析的治理标记**

Run:

```powershell
rg -n "TBD|待创建|Issue #0|关联 Issue：$" docs/adr/ADR-0026-统一PYTDX能力节点池.md
```

Expected: 无输出，退出码为 `1`。

- [ ] **Step 5: 先写 workflow code 数据库约束的失败测试**

在 `tests/test_postgres_integration.py` 增加：插入 `pytdx_pool_refresh` 可以成功；插入
`unknown_workflow` 触发 `CheckViolation` 并回滚连接。在 `tests/test_production_checks.py`
增加 migration manifest 断言，要求约束覆盖 catalog 中的九个受控 code。

Run:

```powershell
uv run pytest tests/test_production_checks.py -q
uv run pytest tests/test_postgres_integration.py -m integration -q
```

Expected: production check 因 migration 不存在而失败；integration 只允许在
`TEST_DATABASE_URL` 指向 disposable PostgreSQL 时运行，绝不回退到生产 URL。

- [ ] **Step 6: 写首次 workflow code 约束 migration**

`20260811000300_constrain_operations_workflow_codes.sql`：

```sql
alter table operations.workflow_run
    add constraint workflow_run_workflow_code_check
    check (workflow_code in (
        'daily_market',
        'stock_daily_indicator',
        'stale_run_recovery',
        'deducted_profit',
        'stock_pool',
        'auction_collection',
        'eod_quote_snapshot',
        'call_auction_snapshot',
        'pytdx_pool_refresh'
    )) not valid;

alter table operations.workflow_run
    validate constraint workflow_run_workflow_code_check;
```

重新运行 Step 5 两条命令。若没有隔离数据库，production check 必须通过，并在交付报告中
精确记录 integration 未执行原因。

- [ ] **Step 7: 分别提交治理文档和 migration**

```powershell
git add docs/adr/ADR-0026-统一PYTDX能力节点池.md docs/adr/ADR-0024-远程TDX日K数据源.md docs/adr/README.md docs/领域详设-RemoteTdxDailyBar-2026-08-09.md
git commit -m "docs: accept unified pytdx endpoint pool"
git add supabase/migrations/20260811000300_constrain_operations_workflow_codes.sql tests/test_postgres_integration.py tests/test_production_checks.py
git commit -m "db: constrain operations workflow codes"
```

---

### Task 1: 建立共享节点池模型、严格读取和能力筛选

**Files:**

- Create: `src/market_data_center/providers/pytdx_pool.py`
- Create: `tests/test_pytdx_pool.py`

- [ ] **Step 1: 写 v1 池解析、排序和能力筛选的失败测试**

在 `tests/test_pytdx_pool.py` 写入：

```python
from datetime import datetime

import pytest

from market_data_center.providers.contracts import ProviderError
from market_data_center.providers.pytdx_pool import (
    PytdxCapability,
    endpoints_for,
    load_endpoint_pool,
)


def test_loads_v1_pool_and_filters_stably_by_capability(tmp_path) -> None:
    path = tmp_path / "pool.json"
    path.write_text(
        """{
          "schema_version": "pytdx.endpoint_pool.v1",
          "refreshed_at": "2026-08-11T10:00:00+08:00",
          "nodes": [
            {"host":"b.example","port":7709,"latency_ms":20,
             "capabilities":{"quote":true,"daily_bar_sse":true,
               "daily_bar_szse":false,"daily_bar_bse":false}},
            {"host":"a.example","port":7709,"latency_ms":10,
             "capabilities":{"quote":false,"daily_bar_sse":true,
               "daily_bar_szse":true,"daily_bar_bse":false}}
          ]
        }""",
        encoding="utf-8",
    )

    pool = load_endpoint_pool(path)

    assert pool.refreshed_at == datetime.fromisoformat("2026-08-11T10:00:00+08:00")
    assert endpoints_for(pool, PytdxCapability.DAILY_BAR_SSE) == (
        ("a.example", 7709),
        ("b.example", 7709),
    )
    assert endpoints_for(pool, PytdxCapability.QUOTE) == (("b.example", 7709),)


@pytest.mark.parametrize(
    "mutation",
    [
        lambda document: document.update(schema_version="unknown"),
        lambda document: document["nodes"][0].update(port=0),
        lambda document: document["nodes"][0]["capabilities"].pop("quote"),
    ],
)
def test_rejects_invalid_pool_documents(tmp_path, mutation) -> None:
    document = {
        "schema_version": "pytdx.endpoint_pool.v1",
        "refreshed_at": "2026-08-11T10:00:00+08:00",
        "nodes": [
            {
                "host": "a.example",
                "port": 7709,
                "latency_ms": 10,
                "capabilities": {
                    "quote": True,
                    "daily_bar_sse": True,
                    "daily_bar_szse": True,
                    "daily_bar_bse": False,
                },
            }
        ],
    }
    mutation(document)
    path = tmp_path / "pool.json"
    path.write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(ProviderError):
        load_endpoint_pool(path)
```

同时添加重复 `(host, port)`、无时区 `refreshed_at`、非布尔 capability、损坏 JSON 的独立断言。

- [ ] **Step 2: 运行新测试并确认红灯原因是模块尚不存在**

Run:

```powershell
uv run pytest tests/test_pytdx_pool.py -q
```

Expected: FAIL，错误包含 `No module named 'market_data_center.providers.pytdx_pool'`。

- [ ] **Step 3: 实现最小不可变模型和严格加载器**

在 `pytdx_pool.py` 实现下列公共边界：

```python
class PytdxCapability(StrEnum):
    QUOTE = "quote"
    DAILY_BAR_SSE = "daily_bar_sse"
    DAILY_BAR_SZSE = "daily_bar_szse"
    DAILY_BAR_BSE = "daily_bar_bse"


@dataclass(frozen=True, slots=True)
class PytdxPoolNode:
    host: str
    port: int
    latency_ms: int
    capabilities: Mapping[PytdxCapability, bool]


@dataclass(frozen=True, slots=True)
class PytdxEndpointPool:
    refreshed_at: datetime
    nodes: tuple[PytdxPoolNode, ...]


def load_endpoint_pool(path: Path) -> PytdxEndpointPool:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ProviderError("pytdx endpoint pool is unreadable") from error
    return _parse_endpoint_pool(document)


def endpoints_for(
    pool: PytdxEndpointPool, capability: PytdxCapability
) -> tuple[tuple[str, int], ...]:
    return tuple(
        (node.host, node.port)
        for node in pool.nodes
        if node.capabilities[capability]
    )
```

实现要求：精确版本、字段集合、时区、端口、非负整数延迟、四个显式布尔 capability、endpoint 唯一；加载后按 `(latency_ms, host, port)` 排序。不要吞掉文件或 JSON 错误，统一转换为无敏感文本的 `ProviderError`。

- [ ] **Step 4: 运行测试至绿灯并检查类型**

```powershell
uv run pytest tests/test_pytdx_pool.py -q
uv run mypy src/market_data_center/providers/pytdx_pool.py
```

Expected: 全部通过。

- [ ] **Step 5: 提交共享读取契约**

```powershell
git add src/market_data_center/providers/pytdx_pool.py tests/test_pytdx_pool.py
git commit -m "feat: add strict pytdx endpoint pool contract"
```

---

### Task 2: 实现有界能力探测、发布门禁和原子刷新

**Files:**

- Modify: `src/market_data_center/providers/pytdx_pool.py`
- Modify: `tests/test_pytdx_pool.py`

- [ ] **Step 1: 写有效刷新原子替换的失败测试**

新增可注入候选、probe、clock 的测试：

```python
def test_refresh_publishes_only_a_complete_pool_atomically(tmp_path) -> None:
    target = tmp_path / "pytdx_pool.json"
    probe = FakeProbe(
        {
            ("fast", 7709): capabilities(quote=True, sse=True, szse=True),
            ("slow", 7709): capabilities(quote=True, sse=False, szse=False),
        }
    )

    result = refresh_endpoint_pool(
        target,
        candidates=(("slow", 7709), ("fast", 7709)),
        probe=probe,
        clock=lambda: AWARE_NOW,
    )

    assert result.published is True
    assert result.used_last_good is False
    assert load_endpoint_pool(target).nodes[0].host == "fast"
    assert list(tmp_path.glob("*.tmp")) == []
```

- [ ] **Step 2: 写刷新失败保留 last-good 与首启失败测试**

```python
def test_invalid_refresh_preserves_last_good_pool(tmp_path) -> None:
    target = write_valid_pool(tmp_path)
    before = target.read_bytes()

    result = refresh_endpoint_pool(
        target,
        candidates=(("dead", 7709),),
        probe=FakeProbe({}),
        clock=lambda: AWARE_NOW,
    )

    assert result.published is False
    assert result.used_last_good is True
    assert target.read_bytes() == before


def test_refresh_fails_when_no_new_or_last_good_pool_exists(tmp_path) -> None:
    with pytest.raises(ProviderError, match="no usable pytdx endpoint pool"):
        refresh_endpoint_pool(
            tmp_path / "missing.json",
            candidates=(("dead", 7709),),
            probe=FakeProbe({}),
            clock=lambda: AWARE_NOW,
        )
```

还要覆盖：只缺 BSE 仍可发布；缺 quote/SSE/SZSE 任一项不能发布；同一节点各 capability 独立；异常按稳定类别计数且结果不含异常文本。

- [ ] **Step 3: 运行两个测试确认红灯**

```powershell
uv run pytest tests/test_pytdx_pool.py -q
```

Expected: FAIL，缺少 `refresh_endpoint_pool`/probe 类型。

- [ ] **Step 4: 实现探测协议与结果统计**

公共接口：

```python
@dataclass(frozen=True, slots=True)
class PytdxProbeResult:
    host: str
    port: int
    latency_ms: int
    capabilities: Mapping[PytdxCapability, bool]


@dataclass(frozen=True, slots=True)
class PytdxPoolRefreshResult:
    candidate_count: int
    usable_node_count: int
    rejected_node_count: int
    published: bool
    used_last_good: bool
    pool: PytdxEndpointPool


class PytdxEndpointProbe(Protocol):
    def probe(self, host: str, port: int) -> PytdxProbeResult | None:
        """Return measured capabilities, or None when the node is unusable."""


def refresh_endpoint_pool(
    path: Path,
    *,
    candidates: Sequence[tuple[str, int]] = DEFAULT_PYTDX_CANDIDATES,
    probe: PytdxEndpointProbe | None = None,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> PytdxPoolRefreshResult:
    """Publish a valid measured pool or return the strictly validated last-good pool."""
```

默认候选只从 pytdx 自带 host 目录生成并去重。默认 probe 用一个连接分别执行固定 quote、SSE 日 K、SZSE 日 K、BSE 日 K 样本；单节点 timeout 和并发上限为代码常量。用临时文件写 JSON，`flush()`、`os.fsync()` 后 `Path.replace()`；在 Windows 上也使用同目录临时文件。只有新池满足 quote/SSE/SZSE 门禁才替换。

- [ ] **Step 5: 把刷新结果接入 Operations 的行数统计**

修改 `src/market_data_center/operations_service.py` 的 `_result_statistics`，在 `IngestionRun` 分支前添加：

```python
if isinstance(result, PytdxPoolRefreshResult):
    return (
        result.candidate_count,
        result.usable_node_count,
        result.rejected_node_count,
        ExecutionStatus.PARTIAL if result.used_last_good else ExecutionStatus.SUCCEEDED,
    )
```

并在 `tests/test_operations.py` 增加两个测试：新池成功发布时断言
`fetched = accepted + rejected` 且 workflow 为 succeeded；回退 last-good 时 workflow 为
partial。

- [ ] **Step 6: 运行聚焦测试并提交**

```powershell
uv run pytest tests/test_pytdx_pool.py tests/test_operations.py -q
uv run ruff check src/market_data_center/providers/pytdx_pool.py tests/test_pytdx_pool.py
git add src/market_data_center/providers/pytdx_pool.py src/market_data_center/operations_service.py tests/test_pytdx_pool.py tests/test_operations.py
git commit -m "feat: refresh pytdx pool with last-good fallback"
```

---

### Task 3: 收敛配置并迁移 Provider 注册

**Files:**

- Modify: `src/market_data_center/settings.py`
- Modify: `src/market_data_center/providers/registry.py`
- Modify: `tests/test_provider_registry.py`
- Create: `tests/test_settings.py`（若已有则 Modify）

- [ ] **Step 1: 写唯一新配置契约的失败测试**

```python
def test_pytdx_pool_settings_have_safe_defaults(monkeypatch) -> None:
    for name in (
        "PYTDX_DAILY_BAR_ENDPOINTS",
        "PYTDX_DAILY_BAR_POOL_PATH",
        "PYTDX_HQ_HOST",
        "PYTDX_HQ_PORT",
        "PYTDX_HQ_POOL_PATH",
    ):
        monkeypatch.delenv(name, raising=False)

    settings = PytdxPoolSettings(_env_file=None)

    assert settings.pytdx_pool_path == Path("data/pytdx_pool.json")
    assert settings.pytdx_pool_refresh_hours == 12
```

另写 `refresh_hours=0` 与 `refresh_hours>168` 被 Pydantic 拒绝的测试。更新 provider registry 测试，只设置临时 `PYTDX_POOL_PATH`，不得再注入旧变量。

- [ ] **Step 2: 运行测试确认红灯**

```powershell
uv run pytest tests/test_settings.py tests/test_provider_registry.py -q
```

Expected: FAIL，缺少 `PytdxPoolSettings`。

- [ ] **Step 3: 实现共享配置并删除旧字段**

`settings.py` 最终结构：

```python
class PytdxPoolSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    pytdx_pool_path: Path = Path("data/pytdx_pool.json")
    pytdx_pool_refresh_hours: int = Field(default=12, ge=1, le=168)


class PytdxHqSettings(BaseSettings):
    pytdx_hq_timeout_seconds: float = Field(default=2.0, gt=0, le=4.0)
    pytdx_hq_batch_size: int = Field(default=80, ge=1, le=80)
    pytdx_hq_max_retries: int = Field(default=1, ge=0, le=1)


class PytdxDailyBarSettings(BaseSettings):
    pytdx_vipdoc_path: str = ""
    pytdx_daily_bar_timeout_seconds: float = Field(default=3.0, gt=0, le=10)
    pytdx_daily_bar_max_attempts: int = Field(default=2, ge=1, le=5)
    pytdx_daily_bar_page_size: int = Field(default=800, ge=1, le=800)
    pytdx_daily_bar_max_pages: int = Field(default=16, ge=1, le=64)
```

旧字段和 `SecretStr` endpoint 解析全部删除；`PYTDX_VIPDOC_PATH` 不改名、不改默认值。

- [ ] **Step 4: 运行测试并提交**

```powershell
uv run pytest tests/test_settings.py tests/test_provider_registry.py -q
uv run mypy src/market_data_center/settings.py src/market_data_center/providers/registry.py
git add src/market_data_center/settings.py src/market_data_center/providers/registry.py tests/test_settings.py tests/test_provider_registry.py
git commit -m "refactor: replace pytdx endpoint settings with pool settings"
```

---

### Task 4: 让 Daily Bar 按市场能力使用统一池

**Files:**

- Modify: `src/market_data_center/providers/pytdx.py`
- Modify: `tests/test_pytdx_provider.py`

- [ ] **Step 1: 删除旧 parser 测试并先写 capability 行为测试**

删除 `parse_daily_bar_endpoints`、旧 pool fallback 相关测试，新增：

```python
def test_remote_daily_bar_selects_only_matching_market_capability(tmp_path) -> None:
    pool = write_pool(
        tmp_path,
        [
            node("quote-only", quote=True),
            node("sse", sse=True),
            node("szse", szse=True),
        ],
    )
    clients = RecordingClientFactory()
    provider = PytdxProvider(
        PytdxDailyBarSettings(_env_file=None),
        pool_settings=PytdxPoolSettings(pytdx_pool_path=pool, _env_file=None),
        client_factory=clients,
    )

    with provider:
        provider.fetch_daily_bars("sh.600000", START, END)
        provider.fetch_daily_bars("sz.000001", START, END)

    assert clients.connected_endpoints == [("sse", 7709), ("szse", 7709)]
```

新增以下独立测试：

- vipdoc 文件命中时不读池、不创建 client；
- vipdoc 缺文件时按市场 capability 连接；
- 一个市场第一次连接成功后后续 symbol 复用固定 endpoint；
- 只在连接阶段按 `pytdx_daily_bar_max_attempts` failover；
- 请求抛错时不创建第二 client；
- BSE 无 `daily_bar_bse` 节点时抛 `ProviderRequestUnavailable`；
- Raw request metadata 中 `endpoint` 等于实际连接节点；
- remote/local Raw replay 测试保持不变。

- [ ] **Step 2: 运行 Daily Bar 测试确认红灯**

```powershell
uv run pytest tests/test_pytdx_provider.py -q
```

Expected: FAIL，构造器尚不接受 `pool_settings`，且旧解析逻辑仍存在。

- [ ] **Step 3: 最小改造 Provider 会话生命周期**

实现要点：

```python
CAPABILITY_BY_EXCHANGE = {
    "sh": PytdxCapability.DAILY_BAR_SSE,
    "sz": PytdxCapability.DAILY_BAR_SZSE,
    "bj": PytdxCapability.DAILY_BAR_BSE,
}


class PytdxProvider:
    def __init__(
        self,
        settings: PytdxDailyBarSettings,
        *,
        pool_settings: PytdxPoolSettings | None = None,
        client_factory: Callable[[], PytdxDailyBarClient] | None = None,
    ) -> None:
        self._pool_settings = pool_settings or PytdxPoolSettings()
        self._sessions: dict[str, tuple[PytdxDailyBarClient, tuple[str, int]]] = {}

    def __enter__(self) -> Self:
        return self

    def _session_for(self, exchange: str):
        existing = self._sessions.get(exchange)
        if existing is not None:
            return existing
        pool = load_endpoint_pool(self._pool_settings.pytdx_pool_path)
        endpoints = endpoints_for(pool, CAPABILITY_BY_EXCHANGE[exchange])
        for host, port in endpoints[: self._settings.pytdx_daily_bar_max_attempts]:
            client = self._client_factory()
            if client.connect(
                host,
                port,
                time_out=self._settings.pytdx_daily_bar_timeout_seconds,
            ):
                session = (client, (host, port))
                self._sessions[exchange] = session
                return session
            _disconnect(client)
        raise ProviderRequestUnavailable(
            f"pytdx endpoint pool has no usable {exchange} Daily Bar node"
        )
```

`__exit__` 断开所有已建立会话。删除 `json` import、`parse_daily_bar_endpoints`、`_read_daily_bar_pool`、`_resolve_daily_bar_endpoints`。本地命中路径必须位于 `_session_for` 之前。

- [ ] **Step 4: 运行测试和 Raw replay 回归**

```powershell
uv run pytest tests/test_pytdx_provider.py tests/test_raw_replay.py tests/test_provider_registry.py -q
```

Expected: 全部通过。

- [ ] **Step 5: 提交 Daily Bar 迁移**

```powershell
git add src/market_data_center/providers/pytdx.py tests/test_pytdx_provider.py
git commit -m "refactor: route pytdx daily bars by pool capability"
```

---

### Task 5: 让五档行情只使用 quote 能力节点

**Files:**

- Modify: `src/market_data_center/providers/pytdx_hq.py`
- Modify: `src/market_data_center/snapshot_collector.py`
- Modify: `src/market_data_center/scheduler.py`
- Modify: `tests/test_pytdx_hq_provider.py`
- Modify: `tests/test_snapshot_collector.py`

- [ ] **Step 1: 写 quote 筛选与会话固定测试**

```python
def test_hq_provider_uses_only_quote_nodes_and_fixes_one_session(tmp_path) -> None:
    pool = write_pool(
        tmp_path,
        [node("daily-only", sse=True, szse=True), node("quote", quote=True)],
    )
    factory = RecordingManagedClientFactory()
    provider = PytdxHqProvider(
        PytdxHqSettings(_env_file=None),
        pool_settings=PytdxPoolSettings(pytdx_pool_path=pool, _env_file=None),
        client_factory=factory,
    )

    with provider:
        provider.fetch_five_level_quotes(("SSE:600000",))
        provider.fetch_five_level_quotes(("SZSE:000001",))

    assert factory.endpoints == [("quote", 7709)]
```

补充：无 quote 节点明确失败；损坏池明确失败且不能再 fallback 到 HQ host；连接阶段可按顺序 failover；fetch 阶段不重新选节点；Decimal、手转股、批量失败语义不变。

- [ ] **Step 2: 运行测试确认红灯**

```powershell
uv run pytest tests/test_pytdx_hq_provider.py tests/test_snapshot_collector.py -q
```

- [ ] **Step 3: 删除重复池解析与单 host fallback**

`PytdxHqProvider` 接收 `pool_settings`；默认 client factory 使用：

```python
pool = load_endpoint_pool(self._pool_settings.pytdx_pool_path)
hosts = endpoints_for(pool, PytdxCapability.QUOTE)
if not hosts:
    raise ProviderError("pytdx endpoint pool has no quote-capable node")
return _NetworkQuoteClient(hosts, settings.pytdx_hq_timeout_seconds)
```

删除 `json`、`Path`、`_read_pool` 和 `_resolve_hosts` 的旧 fallback 分支。`snapshot_collector.py` 与 `scheduler.py` 的生产构造器使用默认 `PytdxPoolSettings()`，测试则显式注入临时池，避免读取开发机文件。

- [ ] **Step 4: 运行五档与竞价回归并提交**

```powershell
uv run pytest tests/test_pytdx_hq_provider.py tests/test_snapshot_collector.py tests/test_auction_service.py -q
git add src/market_data_center/providers/pytdx_hq.py src/market_data_center/snapshot_collector.py src/market_data_center/scheduler.py tests/test_pytdx_hq_provider.py tests/test_snapshot_collector.py
git commit -m "refactor: route pytdx quotes through capability pool"
```

---

### Task 6: 在 Worker 启动和 APScheduler 中运行 12 小时刷新

**Files:**

- Modify: `src/market_data_center/domain/operations.py`
- Modify: `src/market_data_center/operations_service.py`
- Modify: `src/market_data_center/scheduling_catalog.py`
- Modify: `src/market_data_center/scheduler.py`
- Modify: `src/market_data_center/worker_admin.py`
- Modify: `tests/test_operations.py`
- Modify: `tests/test_scheduler.py`
- Modify: `tests/test_worker_admin.py`
- Modify: `tests/test_postgres_integration.py`
- Modify: `tests/test_production_checks.py`

- [ ] **Step 1: 写目录与 12 小时 interval 的失败测试**

```python
def test_catalog_registers_twelve_hour_pytdx_pool_refresh() -> None:
    jobs = {job.code: job for job in job_definitions(SchedulerSettings(_env_file=None))}

    assert jobs["pytdx-pool-refresh"].workflow_code == "pytdx_pool_refresh"
    assert jobs["pytdx-pool-refresh"].trigger_type == "interval"
    assert jobs["pytdx-pool-refresh"].interval_hours == 12
    assert jobs["pytdx-pool-refresh"].enabled is True
```

让 `job_definitions` 接受 `PytdxPoolSettings`，或增加只传 `refresh_hours` 的明确参数；不要把池路径放进 `SchedulerSettings`。

- [ ] **Step 2: 写 Worker 启动顺序测试**

将 `run_worker` 的启动核心提取为可测试函数，例如：

```python
def start_locked_worker(
    scheduler_settings: SchedulerSettings,
    *,
    refresh: Callable[[], PytdxPoolRefreshResult],
    scheduler_factory: Callable[[SchedulerSettings], BlockingScheduler],
    admin_factory: Callable[[BlockingScheduler], WorkerAdminServer],
) -> None:
    """Refresh, construct, expose and run the Worker while the caller holds the lock."""
```

测试记录事件并断言：

```python
assert events == ["lock-acquired", "pool-refreshed", "scheduler-built", "admin-started"]
```

再写两个失败语义测试：刷新返回 last-good 时继续；刷新抛 `ProviderError` 时 scheduler/admin 均未构建。

- [ ] **Step 3: 运行目录和启动测试确认红灯**

```powershell
uv run pytest tests/test_scheduler.py tests/test_operations.py tests/test_worker_admin.py -q
```

- [ ] **Step 4: 增加 workflow、job 和执行函数**

`WorkflowCode` 补齐 controlled catalog 中现有但枚举缺少的两个 code，并增加新 code：

```python
EOD_QUOTE_SNAPSHOT = "eod_quote_snapshot"
CALL_AUCTION_SNAPSHOT = "call_auction_snapshot"
PYTDX_POOL_REFRESH = "pytdx_pool_refresh"
```

在 scheduling catalog 增加：

```python
PYTDX_POOL_REFRESH_JOB_ID = "pytdx-pool-refresh"

WorkflowDefinition(
    "pytdx_pool_refresh",
    "PYTDX 节点池刷新",
    "探测候选节点能力并原子发布最后有效节点池。",
    ("refresh_pytdx_pool",),
)
```

刷新 job 使用 `IntervalTrigger(hours=12)`；执行函数启动 `WorkflowCode.PYTDX_POOL_REFRESH`，只运行一个 `refresh_pytdx_pool` step。`scheduled_for` 使用当前 UTC 时间；定时调用的 trigger source 为 scheduled，Worker 启动调用使用 recovery，并由同一函数记录 Operations。

- [ ] **Step 5: 调整 Worker 顺序**

`run_worker` 必须先构造锁 engine，取得 `task_lock(SCHEDULER_LOCK_KEY)`，再运行启动刷新，然后调用 `build_scheduler`、启动只读管理页、`scheduler.start()`。信号处理闭包需允许 scheduler 在刷新前尚未创建，例如持有 `BlockingScheduler | None`。

- [ ] **Step 6: 运行聚焦与隔离集成测试**

```powershell
uv run pytest tests/test_scheduler.py tests/test_operations.py tests/test_worker_admin.py tests/test_production_checks.py -q
uv run pytest tests/test_postgres_integration.py -m integration -q
```

Expected: 单元测试通过；集成测试仅在 `TEST_DATABASE_URL` 指向隔离 disposable PostgreSQL 时运行。若变量未配置，记录为未运行，不得改用生产 `DATABASE_URL`。

- [ ] **Step 7: 提交 Worker**

```powershell
git add src/market_data_center/domain/operations.py src/market_data_center/operations_service.py src/market_data_center/scheduling_catalog.py src/market_data_center/scheduler.py src/market_data_center/worker_admin.py tests/test_operations.py tests/test_scheduler.py tests/test_worker_admin.py tests/test_postgres_integration.py tests/test_production_checks.py
git commit -m "feat: refresh pytdx pool in worker"
```

---

### Task 7: 删除旧脚本与配置引用，更新部署检查和运行文档

**Files:**

- Delete: `scripts/check_pytdx_daily_bar_endpoints.py`
- Delete: `scripts/probe_pytdx_hq_hosts.py`
- Create: `scripts/check_pytdx_pool.py`
- Modify: `deploy/linux/market-data-center-worker.service`
- Modify: `deploy/linux/smoke-check.sh`
- Modify: `deploy/linux/market-data-center.env.example`
- Modify: `.env.example`
- Modify: `deploy.ps1`
- Modify: `README.md`
- Modify: `INSTALL-WINDOWS.md`
- Modify: `docs/Worker日常采集与调度.md`
- Modify: `docs/Worker调度系统.md`
- Modify: `docs/最小生产发布运行手册.md`
- Modify: `docs/集合竞价五档采集运行手册.md`
- Modify: `tests/test_production_checks.py`

- [ ] **Step 1: 写仓库级旧配置零引用测试**

在 `tests/test_production_checks.py` 增加允许 ADR 历史提及、但禁止运行代码/活跃文档提及的测试：

```python
LEGACY_PYTDX_SETTINGS = (
    "PYTDX_DAILY_BAR_ENDPOINTS",
    "PYTDX_DAILY_BAR_POOL_PATH",
    "PYTDX_HQ_HOST",
    "PYTDX_HQ_PORT",
    "PYTDX_HQ_POOL_PATH",
)


def test_active_release_files_do_not_reference_legacy_pytdx_settings() -> None:
    roots = (
        PROJECT_ROOT / "src",
        PROJECT_ROOT / "scripts",
        PROJECT_ROOT / "deploy",
        PROJECT_ROOT / ".env.example",
        PROJECT_ROOT / "deploy.ps1",
        PROJECT_ROOT / "README.md",
        PROJECT_ROOT / "INSTALL-WINDOWS.md",
    )
    text = read_release_text(roots)
    assert all(name not in text for name in LEGACY_PYTDX_SETTINGS)
```

另断言 systemd unit 不包含 `ExecStartPre` 的旧 endpoint check，Linux env 模板包含绝对池路径和 `12` 小时。

- [ ] **Step 2: 运行生产检查确认红灯**

```powershell
uv run pytest tests/test_production_checks.py -q
```

Expected: FAIL，并列出旧变量/脚本引用。

- [ ] **Step 3: 用只读池检查脚本替代 endpoint 连通性脚本**

`scripts/check_pytdx_pool.py` 只调用 `load_endpoint_pool` 并检查 quote/SSE/SZSE capability 非空；输出稳定计数，绝不刷新、不访问网络：

```python
pool = load_endpoint_pool(PytdxPoolSettings().pytdx_pool_path)
counts = {
    capability.value: len(endpoints_for(pool, capability))
    for capability in PytdxCapability
}
if min(counts["quote"], counts["daily_bar_sse"], counts["daily_bar_szse"]) == 0:
    raise SystemExit(1)
print(json.dumps(counts, sort_keys=True))
```

systemd 删除旧 `ExecStartPre`，由 Worker 自身启动刷新/拒绝启动。`smoke-check.sh` 在服务启动后调用新只读脚本验证已发布池。

- [ ] **Step 4: 更新环境模板和安装脚本**

活跃模板只保留：

```dotenv
PYTDX_POOL_PATH=/var/lib/market-data-center/pytdx_pool.json
PYTDX_POOL_REFRESH_HOURS=12
PYTDX_VIPDOC_PATH=
```

Windows `.env.example` 使用 `PYTDX_POOL_PATH=data/pytdx_pool.json`，并保留用户指定的可选示例 `PYTDX_VIPDOC_PATH=D:\new_tdx64\vipdoc`。`deploy.ps1` 首次配置要求不再包含 endpoint；只要求数据库、Raw 根目录等真正必需项。不要把用户实际 `.env` 内容写入仓库。

- [ ] **Step 5: 更新领域详设与运行手册**

文档必须说明：统一 v1 schema、能力门禁、启动 last-good 规则、12 小时 Worker interval、vipdoc 本地优先、BSE 可见缺口、没有 OS scheduler、池文件不进 Git、单批次 endpoint 固定。ADR-0024 仅作为历史决定保留旧变量名；活跃运行手册不得指导配置旧变量。

- [ ] **Step 6: 运行零引用检查**

```powershell
rg -n "PYTDX_DAILY_BAR_ENDPOINTS|PYTDX_DAILY_BAR_POOL_PATH|PYTDX_HQ_HOST|PYTDX_HQ_PORT|PYTDX_HQ_POOL_PATH|pytdx_hq_pool|check_pytdx_daily_bar_endpoints|probe_pytdx_hq_hosts" src tests scripts deploy .env.example deploy.ps1 README.md INSTALL-WINDOWS.md docs --glob "!docs/adr/ADR-0024-*" --glob "!docs/superpowers/specs/2026-08-11-pytdx-unified-endpoint-pool-design.md" --glob "!docs/superpowers/plans/2026-08-11-pytdx-unified-endpoint-pool.md"
```

Expected: 无输出，退出码为 `1`。

- [ ] **Step 7: 运行检查并提交**

```powershell
uv run pytest tests/test_production_checks.py -q
git add -A scripts deploy .env.example deploy.ps1 README.md INSTALL-WINDOWS.md docs tests/test_production_checks.py
git commit -m "docs: migrate deployment to unified pytdx pool"
```

---

### Task 8: 完整验证与本地交付

**Files:**

- Verify: entire repository
- Verify: `contracts/postgrest-openapi-v1.json`
- Verify: `contracts/agent-tools-v1.json`
- Verify: `contracts/fastapi-openapi-v1.json`

- [ ] **Step 1: 格式化本次修改并检查差异范围**

```powershell
uv run ruff format src tests scripts
git status --short
git diff --check
```

Expected: 只出现本计划列出的文件；`git diff --check` 退出码为 `0`。

- [ ] **Step 2: 运行完整本地门禁**

```powershell
uv run ruff format --check .
uv run ruff check .
uv run mypy src
uv run pytest
```

Expected: 四条命令均退出码为 `0`。

- [ ] **Step 3: 在隔离数据库运行 integration gate**

```powershell
uv run pytest -m integration
```

Expected: 当 `TEST_DATABASE_URL` 指向 disposable PostgreSQL 时全部通过。没有该变量则精确报告未验证项，不得使用生产数据库替代。

- [ ] **Step 4: 证明公共契约未发生变化**

```powershell
git diff --exit-code c646574 -- contracts/postgrest-openapi-v1.json contracts/agent-tools-v1.json contracts/fastapi-openapi-v1.json
```

Expected: 退出码为 `0`。若有差异，停止并调查，因为本变更不应改变公共读取契约。

- [ ] **Step 5: 检查提交与工作树**

```powershell
git log --oneline --decorate -10
git status --short
```

Expected: 工作树干净；提交按治理、池契约、刷新、设置、两个 Provider、Worker、部署文档的边界清晰分组。

- [ ] **Step 6: 单独列出生产发布步骤，不执行**

交付报告必须明确：代码与 migration 已本地验证，但尚未连接服务器、应用生产 migration、替换 release、修改生产 `.env` 或启动采集。只有用户再次明确授权生产发布后，才按设计文档第 8 节执行备份、migration、部署、启动和任务状态验证。
