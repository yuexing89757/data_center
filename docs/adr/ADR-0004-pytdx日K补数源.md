# ADR-0004：pytdx 本地日 K 数据源

- 状态：Accepted
- 日期：2026-07-28
- 实现澄清：2026-07-29 扩展本地 BSE 日 K，并固定为股票日 K 唯一来源
- 决策者：项目所有者
- 关联：ADR-0001、ADR-0002

## 背景

BaoStock 批量抓取触发上游限制，需要独立且稳定的日 K 数据源。项目所有者的通达信安装目录为 `D:\new_tdx64`，本地 `vipdoc/{sh,sz}/lday/*.day` 已保存沪深证券日线。pytdx 同时提供网络协议 Client 和本地二进制 Reader；本项目选择本地 Reader，避免依赖公共行情节点和网络限流。

2026-07-29 已确认同一目录还包含 `vipdoc/bj/lday/*.day`，其记录格式与沪深本地日线一致；以下扩展决策取代背景中仅描述沪深文件的初始假设。

## 决策

1. 新增 `pytdx` Provider Adapter，仅实现本地未复权股票 Daily Bar 能力，不连接通达信网络行情服务器。
2. 本地目录通过 `PYTDX_VIPDOC_PATH` 显式配置，当前运行值为 `D:\new_tdx64\vipdoc`；仓库不硬编码个人安装路径。
3. Security 继续以 BaoStock/AKShare 为准，Trading Calendar 使用已入库的统一市场日历。
4. Adapter 使用 pytdx `TdxDailyBarReader.parse_data_by_file` 解码 32 字节 `.day` 记录，并自行完成标准 DTO 转换，不使用其 Pandas 浮点转换结果。
5. 来源文件路径固定为 `sh/lday/sh{code}.day`、`sz/lday/sz{code}.day` 或 `bj/lday/bj{code}.day`。BSE 标准代码使用 `BSE:920xxx`，本地市场前缀使用 `bj`。
6. `.day` 中 OHLC 整数按 `Decimal(raw) / 100` 转换；成交额通过字符串构造 `Decimal`；股票成交量在文件中已是“股”，保持原整数，不再乘除 100。
7. `.day` 不单独保存前收盘；Adapter 使用同一文件中严格早于当前记录的上一条 close 生成 `previous_close`。文件第一条记录仍为 `None`；停牌状态和 ST 标识分别映射为 `unknown`、`None`。
8. Provider 必须按请求闭区间裁剪、去重并升序输出。以上证综指 `sh000001.day`、深证成指 `sz399001.day`、北证 50 `bj899050.day` 作为对应市场的新鲜度哨兵；哨兵最新日期早于请求结束日时批次失败。不得用个股最后日期判断新鲜度，因为停牌股可能没有当日记录。
9. 个股文件或市场哨兵不存在、为空、格式异常时，当前批次失败，禁止把不完整的本地目录标记为成功。
10. `source_code=pytdx` 只用于追溯，不参与 `(symbol, trade_date)` 重复判断；相同自然键按现有 UPSERT 更新。
11. 通达信客户端负责下载和更新本地数据；Provider 只读文件，不启动客户端、不自动下载、不回退到网络源。

## 后果

- 读取速度不受公共行情节点限流影响，且不需要维护服务器地址；
- 不改变既有领域自然键和 PostgREST API；
- 本地文件的新鲜度成为采集前置条件，上海、深圳、北京数据必须先在通达信客户端完成下载；
- pytdx 项目已归档，二进制格式兼容风险由样本和契约测试隔离；
- 运行环境必须能只读访问配置的 `vipdoc` 目录。

## 验收

- Provider 契约测试覆盖本地路径映射、日期裁剪、升序、Decimal、成交量单位、缺失文件和陈旧文件；
- Pipeline、Raw Manifest、ingestion run 均记录 `pytdx`，新采集 Raw schema 为 `pytdx.local_daily_bar.v2`；v1 Raw 回放时在批次内按日期推导前收盘，保持历史 Raw 可读；
- 数据库约束允许 `provider_code/source_code=pytdx`；
- Ruff、mypy、pytest 和 migration 检查通过。
