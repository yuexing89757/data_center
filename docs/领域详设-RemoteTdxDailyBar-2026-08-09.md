# 领域详设：Remote TDX Daily Bar

本设计实现 ADR-0024。领域边界仍是 `core.daily_bar` 未复权客观事实；网络 endpoint、TDX
market 编号和响应字段只存在于 Provider/Raw 边界。

## 配置

- `PYTDX_DAILY_BAR_ENDPOINTS`：必填、逗号分隔的 `host:port` 有序列表，不允许重复。
- `PYTDX_DAILY_BAR_TIMEOUT_SECONDS`：连接和读取超时，默认 3 秒，范围 `(0, 10]`。
- `PYTDX_DAILY_BAR_MAX_ATTEMPTS`：建立会话最多尝试节点数，默认 2，范围 `1..5`。
- `PYTDX_DAILY_BAR_PAGE_SIZE`：单页记录数，默认及最大 800。
- `PYTDX_DAILY_BAR_MAX_PAGES`：单证券最大页数，默认 16，范围 `1..64`。

## 会话与失败

Provider `__enter__` 依配置顺序建立一个连接，并记录实际 endpoint。连接失败只记录异常
类型。成功后整个实例固定该 endpoint。任何读取异常或协议形状错误产生 `ProviderError`，
Router 丢弃会话并累计熔断；空结果或区间无记录产生 `ProviderRequestUnavailable`，保持缺口。

## 请求与标准化

TDX category 固定为 `9`。offset 从 0 递增，直到覆盖开始日期、返回不足一页、空页或达到
页数上限。每页必须是 list；每行必须包含日期、OHLC、volume、amount。按交易日排序去重，
冲突重复行阻断。请求范围外的最近前一条记录只用于 `previous_close`，不写入结果。

SSE 映射 market 1，SZSE/BSE 映射 market 0。BSE 是否可用取决于节点；无数据时明确返回
Unavailable。Raw schema `pytdx.remote_daily_bar.v1` 保存字符串字段，request metadata 包含
endpoint、TDX market、symbol、日期范围、category、分页边界和未复权标志。

## 运维验收

公共节点可能随时下线、限流或缩短历史。发布前对配置列表逐个做 TCP 和三市场样本检查；
日常以 IngestionRun、QualityResult、Raw manifest、缺口审计和 Router 熔断状态观察。不得把
探测结果自动写回 endpoint 列表，也不得通过 BaoStock/AKShare 填补普通 Daily Bar 缺口。
systemd 启动前只要求至少一个节点可连接，完整发布 smoke 要求配置列表全部可连接；两者均
不请求行情记录。
