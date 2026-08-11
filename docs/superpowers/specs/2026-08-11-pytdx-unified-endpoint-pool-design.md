# PYTDX 统一能力节点池设计

> 状态：设计已获项目所有者批准
> 日期：2026-08-11
> 治理门禁：替代 ADR 与实现必须关联一个 GitHub Issue；当前本机 GitHub CLI 未认证，因此 Issue 建立、ADR 编号关联和实现尚未开始。

## 1. 背景

当前远程 Daily Bar 和五档行情虽然都能读取 `data/pytdx_hq_pool.json`，但仍保留两套配置与 fallback：

- Daily Bar 使用 `PYTDX_DAILY_BAR_ENDPOINTS` 或 `PYTDX_DAILY_BAR_POOL_PATH`；
- 五档行情使用 `PYTDX_HQ_HOST`/`PYTDX_HQ_PORT` 或 `PYTDX_HQ_POOL_PATH`；
- systemd 启动前检查只识别 `PYTDX_DAILY_BAR_ENDPOINTS`，即使池文件可用也可能拒绝启动；
- 两个 Provider 各自解析相同形状的池文件，错误语义和能力判断重复。

本设计用一个带能力标记的本地节点池取代所有显式 endpoint 配置。它将替代 ADR-0024 中“运行时禁止节点发现、节点由 `PYTDX_DAILY_BAR_ENDPOINTS` 显式维护”的决定，但不改变 Daily Bar 的领域语义、Provider 路由、Raw lineage 或缺口可见性。

## 2. 目标与非目标

### 2.1 目标

- Daily Bar、集合竞价五档和收盘五档共用一个节点池文件；
- Worker 启动时刷新节点池，之后每 12 小时刷新；
- 每个节点分别记录实时行情、沪市日 K、深市日 K、北交所日 K 能力；
- 刷新失败时保留并使用最后一个有效池；
- 池文件发布采用原子替换，消费者永远看不到半写文件；
- 保留 `PYTDX_VIPDOC_PATH`，普通股票 Daily Bar 继续本地 `.day` 文件优先；
- 保持单个成功 IngestionRun 只使用一个实际 endpoint，并在 Raw request metadata 中记录该 endpoint。

### 2.2 非目标

- 不把节点池写入 PostgreSQL、Raw、Core 或公共 API；
- 不把节点发现结果提交 Git；
- 不改变 BaoStock/AKShare/pytdx 的 Provider 路由；
- 不用其他 Provider 填补 Daily Bar 缺口；
- 不新增分钟、tick、逐笔或 Level-2 语义；
- 不在一个成功批次中途切换 endpoint。

## 3. 配置契约

统一配置为：

```dotenv
PYTDX_POOL_PATH=/var/lib/market-data-center/pytdx_pool.json
PYTDX_POOL_REFRESH_HOURS=12
PYTDX_VIPDOC_PATH=
```

Windows 默认池路径为 `data/pytdx_pool.json`；Linux 模板使用 Worker 可写的 `/var/lib/market-data-center/pytdx_pool.json`。

删除以下配置及其解析代码：

- `PYTDX_DAILY_BAR_ENDPOINTS`；
- `PYTDX_DAILY_BAR_POOL_PATH`；
- `PYTDX_HQ_HOST`；
- `PYTDX_HQ_PORT`；
- `PYTDX_HQ_POOL_PATH`。

Daily Bar 的超时、最大连接尝试数、分页大小和最大页数继续保留。五档行情的超时、批量大小和重试上限继续保留。部署检查必须拒绝仍包含上述旧变量的生产环境模板或发布配置，避免旧配置被误认为仍然有效。

## 4. 节点池格式

池文件使用版本化 JSON：

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

规则：

- `schema_version` 必须精确匹配；未知版本拒绝读取；
- `refreshed_at` 是带时区时间戳；
- `(host, port)` 唯一；端口范围为 `1..65535`；
- 节点按探测延迟升序保存；相同延迟按 host、port 稳定排序；
- capability 必须为布尔值，不能以缺失字段代替 `false`；
- 池中至少存在一个 `quote=true` 节点、一个 `daily_bar_sse=true` 节点和一个 `daily_bar_szse=true` 节点；
- BSE 能力独立记录，不作为池发布硬门禁；没有 BSE 节点时北交所缺口保持可见。

## 5. 共享节点池组件

新增共享组件 `market_data_center.providers.pytdx_pool`，职责仅包括：

- 定义不可变的节点与能力记录；
- 严格读取和验证池文件；
- 从 pytdx 内置候选目录执行有界探测；
- 按 capability 返回稳定有序的 endpoint；
- 将有效新池写入同目录临时文件，执行 flush/fsync 后原子替换目标文件；
- 刷新失败时不修改现有池。

探测使用固定样本验证协议能力：

- `quote`：获取一个沪市流动性证券的五档快照；
- `daily_bar_sse`：读取沪市日线样本；
- `daily_bar_szse`：读取深市日线样本；
- `daily_bar_bse`：读取北交所日线样本。

探测有固定最大并发数、单节点超时和整体完成边界。日志只记录候选数、成功数、能力计数、耗时和稳定错误类别，不记录数据库 URL、Token、Raw 行情正文或异常堆栈中的敏感路径。

## 6. 刷新生命周期

Worker 获得全局 Scheduler advisory lock 后、注册其他 APScheduler job 前执行启动刷新：

1. 读取并验证旧池；
2. 探测候选节点并构造新池；
3. 新池满足发布门禁时原子替换旧池；
4. 新池不满足门禁时保留旧池；
5. 新池失败但旧池有效时继续启动；
6. 新旧池都无效时拒绝启动。

新增受控任务 `pytdx-pool-refresh`，使用 APScheduler interval trigger 每 12 小时执行。任务定义、调度注册、Operations 执行记录和本地管理页引用同一个代码目录。不得注册 cron、Windows Task Scheduler 或其他 OS 级刷新任务。

Operations 新增内部 workflow code `pytdx_pool_refresh`，通过有序 SQL migration 扩展约束。该 workflow 不创建 IngestionRun，因为节点可用性探测不是市场事实采集；它只记录运行状态和受控行数统计。`operations` 仍不加入公共 PostgREST/FastAPI 契约。

## 7. Provider 行为

### 7.1 Daily Bar

- `PYTDX_VIPDOC_PATH` 有效且本地 `.day` 文件存在时直接读取本地文件，不要求节点池；
- 本地文件缺失时，根据证券市场筛选 `daily_bar_sse`、`daily_bar_szse` 或 `daily_bar_bse` 节点；
- Provider 为不同市场维护独立的固定会话；同一证券的一个 IngestionRun 只使用一个 endpoint；
- 连接阶段按池顺序执行有限 failover；请求开始后发生读取失败时不在批次中途换节点；
- Router 丢弃失败 Provider 后，下一次独立尝试才重新读取最新池并选择节点；
- 没有对应市场能力的节点时返回显式 `ProviderRequestUnavailable`；
- Raw schema 和标准化规则保持不变，request metadata 继续记录实际 endpoint。

### 7.2 五档行情

- 只选择 `quote=true` 的节点；
- 一个集合竞价会话或收盘采集调用固定一个节点；
- 建立会话时允许按池顺序有限 failover，成功后不在采集中途切换；
- 没有 `quote=true` 节点时抛出明确 ProviderError；
- Decimal 解码、手转股、竞价语义和质量门禁保持不变。

## 8. 部署与迁移

发布包必须包含新的共享组件、刷新任务、migration、更新后的 systemd unit、运行手册和配置模板。

服务器上线顺序：

1. 备份当前 release 和 `.env`；
2. 应用仅扩展 Operations workflow code 的 migration；
3. 部署新 release；
4. 删除旧 endpoint 变量，配置绝对 `PYTDX_POOL_PATH` 与 `PYTDX_POOL_REFRESH_HOURS=12`；
5. 确保池目录由专用 Worker 用户可写，权限保持最小化；
6. 保持集合竞价和收盘五档任务开关为 `true`；
7. 启动 systemd Worker，让启动刷新生成第一份有效池；
8. 验证服务状态、管理页、能力计数、下一次刷新时间以及两个采集任务的启用状态。

回滚恢复旧 release 和 `.env` 备份。节点池是可重建本地运行数据，回滚时不删除；Raw、Core、派生事实和 Operations 历史均不修改。

## 9. 测试策略

测试严格遵循 TDD，网络探测全部使用 fake client，不访问真实公共节点。

共享组件测试覆盖：

- v1 池的严格解析和稳定排序；
- 重复 endpoint、非法端口、未知版本、缺失 capability 和损坏 JSON；
- 按 quote/SSE/SZSE/BSE 能力筛选；
- 有效刷新原子替换；
- 无效刷新保留旧池；
- 无旧池且刷新失败时启动失败；
- 有旧池且刷新失败时启动成功；
- 12 小时受控任务注册和 Operations 状态记录。

Provider 测试覆盖：

- vipdoc 本地优先且不触发网络；
- 本地缺失后按市场能力选择节点；
- 单批次 endpoint 固定；
- 连接阶段有限 failover；
- 请求阶段不在批次中途 failover；
- BSE 无能力时保持显式缺口；
- 五档只选择 quote 节点；
- Raw replay 与 endpoint lineage 不变。

部署测试覆盖：

- 源码、模板、文档和 unit 不再引用旧 endpoint 配置；
- systemd 不再调用旧端点预检脚本；
- Linux 池路径位于 Worker 可写目录；
- migration 从空数据库执行并保持公共契约不变。

完整交付门禁：Ruff format、Ruff lint、mypy、全部单元测试和隔离 PostgreSQL integration tests。

## 10. 验收标准

- 仓库中不存在旧配置字段和旧 endpoint 解析代码；
- Worker 启动能生成或复用一个有效版本化节点池；
- Worker 每 12 小时刷新，失败不覆盖最后成功池；
- Daily Bar 本地优先和缺口可见语义不变；
- 日 K 与五档任务按 capability 使用统一池；
- 每个成功批次只记录一个实际 endpoint；
- 服务器由 systemd 单实例 Worker 正常运行；
- 集合竞价与收盘五档任务在管理页显示启用；
- 所有本地与隔离集成门禁通过。
