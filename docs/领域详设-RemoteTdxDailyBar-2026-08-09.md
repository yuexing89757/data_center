# 领域详设：Remote TDX Daily Bar

本设计实现 ADR-0024，并按 ADR-0026 使用统一 PYTDX 能力节点池。领域边界仍是
`core.daily_bar` 未复权客观事实；节点能力、网络 endpoint、TDX market 编号和响应字段只
存在于 Provider、节点池与 Raw request metadata 边界。

## 配置

- `PYTDX_POOL_PATH`：版本化统一节点池路径；Windows 默认 `data/pytdx_pool.json`，Linux
  生产模板使用 Worker 可写的 `/var/lib/market-data-center/pytdx_pool.json`。
- 节点池刷新间隔固定在 Worker 任务目录中为 1 小时，不接受环境变量覆盖。
- `PYTDX_VIPDOC_PATH`：可选本地通达信 `vipdoc` 根目录；有效 `.day` 文件优先于远程请求。
- `PYTDX_DAILY_BAR_TIMEOUT_SECONDS`：连接和读取超时，默认 3 秒，范围 `(0, 10]`。
- `PYTDX_DAILY_BAR_MAX_ATTEMPTS`：建立每个市场会话最多尝试节点数，默认 2，范围 `1..5`。
- `PYTDX_DAILY_BAR_PAGE_SIZE`：单页记录数，默认及最大 800。
- `PYTDX_DAILY_BAR_MAX_PAGES`：单证券最大页数，默认 16，范围 `1..64`。

旧的 per-provider endpoint 列表、独立 pool path 和单 HQ host/port 配置不再属于运行契约。

## 统一节点池

Worker 取得全局 Scheduler advisory lock 后执行启动刷新，之后由 APScheduler 每 1 小时
刷新。候选来自 pytdx 内置目录；探测分别验证实时 quote、SSE 日 K、SZSE 日 K 和 BSE 日 K。
新池至少需要 quote、SSE 与 SZSE 各一个可用节点才能原子发布；BSE 能力缺失不阻断发布，
但北交所远程 Daily Bar 保持显式缺口。刷新失败不修改 last-good；新旧池均无效时 Worker
拒绝启动。

`pytdx.endpoint_pool.v1` 严格要求带时区的 `refreshed_at`、唯一合法的 `(host, port)`、
非负整数 `latency_ms` 以及四个显式布尔 capability。节点按延迟、host、port 稳定排序。

## 本地优先与远程会话

Provider 先按 symbol 定位本地 `.day` 文件。文件存在且产生目标区间记录时直接返回
`pytdx.local_daily_bar.v2`，不读取池、不建立网络 client。文件缺失时，SSE、SZSE、BSE
分别筛选 `daily_bar_sse`、`daily_bar_szse`、`daily_bar_bse` 节点。

Provider 为每个市场延迟建立一个固定会话。建立会话时按池顺序有限尝试；成功后该市场在
当前 Provider 生命周期内固定一个 endpoint。任何读取异常或协议形状错误产生
`ProviderError`，不得在同一请求或成功批次中途切换节点；下一个独立 Provider 尝试才重新
读取最新池。无对应 capability 或区间无记录产生 `ProviderRequestUnavailable`。

## 请求、标准化与追溯

TDX category 固定为 `9`。offset 从 0 递增，直到覆盖开始日期、返回不足一页、空页或达到
页数上限。每页必须是 list；每行必须包含日期、OHLC、volume、amount。按交易日排序去重，
冲突重复行阻断。请求范围外的最近前一条记录只用于 `previous_close`，不写入结果。

SSE 映射 market 1；SZSE 与现有 pytdx BSE 协议映射保持 Provider 边界内。Raw schema
`pytdx.remote_daily_bar.v1` 保存字符串字段，request metadata 包含实际 endpoint、TDX market、
symbol、日期范围、category、分页边界和 `adjust=none`。Domain Record 只保留
`source_code=pytdx`，不包含节点池或 endpoint 字段。

## 运维与可观测性

节点池刷新只记录内部 Operations workflow `pytdx_pool_refresh`，不创建 IngestionRun。日志
只输出候选数、能力计数、耗时和稳定错误类别，不输出数据库 URL、Token、Raw 行情正文或
本地敏感路径。池文件是可重建运行数据，不进入 Git、Raw、Core 或公共 API。

日常通过 Worker 管理页、Operations、IngestionRun、QualityResult、Raw manifest、缺口审计和
Router 熔断状态观察。不得通过 BaoStock/AKShare 填补普通 Daily Bar 缺口，也不得注册 OS
级节点刷新任务。
