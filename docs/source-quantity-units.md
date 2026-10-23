# 来源数量单位目录

本目录对应 ADR-0068、Issue #92 和 `stock-quantity.v1`。可执行目录为
`providers/quantity_units.py` 的 `QUANTITY_UNIT_RULES`；身份必须同时匹配
Provider、endpoint、Raw schema、品种和字段。未登记或明确未核实的规则抛出
`ProviderError`，不能按数量级猜测，不能把未知数值改为 null 后发布成功批次。
静态规则仅证明转换约定，不能代替历史 ingestion 与 Raw hash 的逐批认证。

标准股票数量为整数股，零保留为零，缺失保留为 null。来源 `0.37` 手精确转换为
`37` 股，`0.001` 手因不能表示整数股而失败。拒绝 bool、float、负数、非有限值和
PostgreSQL bigint 溢出。Decimal 上下文精度不能使转换发生舍入。

| Provider / endpoint | Raw schema | 品种 / 字段 | 来源单位及处理 | 证据 |
|---|---|---|---|---|
| akshare / stock_zh_a_hist | akshare.daily_bar.v1、v2 | 股票 / 成交量 | 手 ×100；金额元 | [AKShare 东财历史行情字段表](https://akshare.akfamily.xyz/data/stock/stock.html#id20)及日 K 适配器测试 |
| pytdx / get_security_bars | pytdx.remote_daily_bar.v1、v2 | 股票 / volume | 手 ×100；金额元 | ADR-0024、Issue #90 与现有精确协议解码器回归 |
| pytdx / local_day_file | pytdx.local_daily_bar.v1、v2、v3 | 股票 / volume | uint32 股，不乘100 | 本地 `.day` 二进制读取契约及合成文件回归 |
| baostock / query_history_k_data_plus | baostock.daily_bar.v1 | 股票 / volume | 股 | 既有 BaoStock 股票日 K 契约及 adapter/replay 测试；不扩大到指数 |
| tushare / daily | tushare.daily_bar.v1 | 股票 / vol | 手 ×100；amount 千元 ×1000 | [Tushare 日线字段表](https://tushare.pro/document/2?doc_id=27)及既有 adapter/replay 测试 |
| tushare / daily_basic | tushare.stock_daily_indicator.v1 | 股票 / total_share、float_share、free_share | 万股 ×10000 | [Tushare 每日指标字段表](https://tushare.pro/document/2?doc_id=32)及既有资本数量测试 |
| akshare / stock_zh_a_gbjg_em | akshare.capital.v1 | 股票 / 总股本、流通受限股份、已流通股份、已上市流通A股 | 未核实，阻断 | 当前官方字段表未明确来源单位；现有标准 DTO 的“股”不能反证来源单位 |
| akshare / stock_history_dividend_detail | akshare.capital.v1 | 股票 / 基准股本 | 股 | [AKShare 官方配股历史输出表](https://akshare.akfamily.xyz/data/stock/stock.html)明确“基准股本”单位股（2026-10-23 核验）；零值/缺失回归 |
| tushare / top10_floatholders | tushare.top10_float_holder.v1 | 股票 / hold_amount | 股 | 已接受 Top10FloatHolder 领域契约及整数股/Raw replay 测试 |
| pytdx_hq / get_security_quotes | pytdx_hq.security_quotes.v1 | 股票 / volume_lots、bid_vol1…5、ask_vol1…5 | 协议整手 ×100 | 现有 `get_price` 整数协议解码器；拒绝小数手、bool、溢出 |
| tencent_quote / qt.gtimg.cn | tencent_quote.qt_gtimg.v1 | 股票 / cumulative_volume、bid1…5、ask1…5 | 协议整手 ×100 | 已接受腾讯五档适配器协议与 replay 回归；不扩展小数协议 |
| eastmoney / stock/details/get | eastmoney.call_auction_indicative_detail.v1 | 股票 / volume_lots | 展示匹配整手 ×100 | 已接受竞价指示领域契约；不表述为实际成交量 |
| pytdx_hq / standard_series_archive | market_data_center.call_auction_market_series.raw.v1、v2 | 标准股票记录 / cumulative_volume、五档量 | 已是股，禁止再乘100 | `_raw_envelopes` 中标准领域记录归档契约；原生 provider payload 与标准记录不可互换 |
| eastmoney / quotes、eastmoney_kline | auction_sources.v1（请求时适配器契约） | 股票 / f5、volume | 手 ×100，结果须整数股 | 既有 bounded source 适配器契约及奇数股、非法小数股回归 |
| tencent / tencent_kline、minute | auction_sources.v1（请求时适配器契约） | 股票 / volume、cumulative_volume | 手 ×100，结果须整数股 | 既有 bounded source 适配器契约及奇数股、非法小数股回归 |
| sina / sina_kline | auction_sources.v1（请求时适配器契约） | 股票 / volume | 股，不乘100，须整数 | 既有 source 适配器契约及非法小数股回归 |
| tushare / cb_daily | tushare.convertible_bond_daily_bar.v1 | 可转债 / vol | 手 ×10 张，独立精确转换 | 已接受 ConvertibleBond 契约；`0.1` 手为1张，拒绝非整数张、非有限及溢出；不使用股票规则 |

`auction_sources.v1` 是内部固定 endpoint 解析契约的标识，不是新增持久 Raw schema。
上述“既有契约”不宣称本轮重新获得了全部上游历史版本的原始证明；历史认证仍需
核对当时 source/schema 与不可变 Raw。外部文档只对其明确的 endpoint/品种有效。

## 明确阻断的来源

| Provider / endpoint / schema | 字段或品种 | 原因与当前标准化行为 |
|---|---|---|
| akshare_ths / board_index_daily_bars / akshare_ths.board_index_daily_bar.v1 | THS 板块指数成交量 | 尚无独立确认的聚合量纲；普通批次、Raw replay 和直接 live history 均拒绝输出标准记录 |
| tushare / index_daily / tushare.index_daily.v1 | 指数 vol | 独立指数语义未认证；保留来源 Raw，拒绝标准化，不调用股票日 K ×100 |
| baostock / query_history_k_data_plus / baostock.daily_bar.v1 | 非股票代码（包含指数） | 不能从同 endpoint 股票规则推断；身份没有股票规则即失败 |
| tushare / stk_factor_pro / tushare.stock_technical_factor.v1 | 三组 OBV、EMV、MAEMV | [官方参数表](https://tushare.pro/document/2?doc_id=328)不能证明输出缩放；不假设 OBV 为手或简单×100，整批标准化失败，全部为null的来源也不绕过 |
| akshare / stock_zh_a_gbjg_em / akshare.capital.v1 | 四个股本数量字段 | 尚无独立来源单位证据；保留完整三类 Capital Raw，遇到 ShareCapital 记录则整批标准化失败，不能只删未知字段继续发布成功 |
| akshare / stock_zt_pool_em / akshare_eastmoney.current_day_limit_up_pool.v1 | 封板资金 | 官方当前表未明确来源单位；来源观察标准化失败，不能从字段名或数值推断元 |
| akshare / stock_zt_pool_dtgc_em / akshare_eastmoney.current_day_limit_down_pool.v1 | 封单资金 | 同上，独立来源观察标准化失败；不借用涨停资金契约 |
| 已退役 pysnowball 及其 Raw | 股票盘口数量 | 本整改不重新启用无已确认契约的适配器或 replay |
| 未登记 schema、endpoint、品种或字段 | 所有数量 | fail closed；未知股票/基金/债券不可借股票规则转换 |

技术因子来源公式与带符号 OBV 不由无符号整数股 helper 重定义。对外 JSON 叶子屏蔽、
目录版本及历史数量认证由领域/持久化/公共读取门禁负责。

可转债张数、金额与发行规模分别证明，不借用股票规则：

- `convertible_bond_daily_bar.volume`：`cb_daily.vol` 手 ×10 张。
- `convertible_bond_daily_bar.amount`：`cb_daily.amount` 为万元，精确 ×10000 转元；
  [官方 doc187](https://tushare.pro/document/2?doc_id=187)，2026-10-23 核验。
- `convertible_bond.bond.quantities`：仅证明 `cb_basic.issue_size` 已为元；
  [官方 doc185](https://tushare.pro/document/2?doc_id=185)，2026-10-23 核验。
- `convertible_bond_daily_bar.remain_size`：官方 `cb_daily` 表没有独立单位证明，仍未认证。
  缺失/空白/null 保留 null，任意非空来源值（包括0）阻断新标准化及旧 v1 replay。

旧 `tushare.convertible_bond_daily_bar.v1` Raw 保持原值和版本，成交额重放纠正为万元×10000，
不再使用旧错误的千元×1000。新批次金额证书不认证剩余规模；本次代码不代表历史金额已修复。

## Raw 与 normalizer 版本

`quantity-normalizer.v1` 是独立标准化版本。新 AKShare 日 K 写 `akshare.daily_bar.v2`，
v1 和 v2 的 Raw 均保留原手值，正确标准化为股。远程 TDX v1/v2 replay 均正确转换为股；
不再为“保真”输出错误手值。远程 v2 的 Raw 本身仍是手，因此每次从 Raw 标准化只换算一次。

新本地 `.day` 采集写 `pytdx.local_daily_bar.v3`：金额直接从 binary32 位模式精确解码为
Decimal，保留存储中的小数元，不先转 Python float 或四舍五入为整数元。文件本身已有的
binary32 精度损失不可恢复。旧本地 v1/v2 Raw 不修改，旧整数金额按其 Raw 原值重放，不能
凭空补回已经丢失的小数。所有本地版本的 volume 均为股，不因金额版本升级而乘100。

新批次 request_params 中记录单位契约、normalizer版本；已核实的适配器另记录来源单位与标准单位。
Capital 未核实批次记录 `quantity_unit_registration=unverified`，不冒称来源股数已认证。

日 K 的 `daily_bar.volume` 与 `daily_bar.amount` 使用独立的 endpoint/字段证明。
金额规则分别登记 AKShare `成交额`、TDX/BaoStock `amount` 为元，Tushare `daily.amount`
为千元；成交量证明不能隐式签发金额证书。字段组精确列出字段，缺少必要字段规则或
新增未登记 endpoint/schema 均失败；CNY/千元单位不能传入整数股转换器。

只含已核实配股事实的 Capital 批次使用 `capital.rights_issue.quantities`，该组仅证明
`stock_history_dividend_detail.基准股本` 的股数，不依赖未知 GBJG 规则。
`capital.shares` 仍未认证，配股募集金额 `capital.rights_issue.proceeds` 也不随基准股本
自动认证；未核实的募集金额在公开读取保留 null 与独立质量状态。
历史 Raw 中缺少这些新键并不自动判为已认证；必须结合确切 registry 身份及 Raw 证明。

本目录及代码不授权生产 migration、历史修复、采集、服务切换或重新计算。
