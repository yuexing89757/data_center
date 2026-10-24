# 来源数量单位目录

Tushare逐字段业务、真实单位、来源证据及运行时适配状态见
[Tushare接口字段单位契约](Tushare接口字段单位契约.md)（2026-10-24核验）。

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
| tushare / index_daily | tushare.index_daily.v1 | 仅四个监管股票基准指数 / vol、amount | 独立指数聚合股数契约：手 ×100 → 股；千元 ×1000 → 元，点位不变 | [指数日线doc95](https://tushare.pro/document/2?doc_id=95)、ADR-0048来源澄清；适配器、Raw replay、独立字段组认证回归 |
| tushare / daily_basic | tushare.stock_daily_indicator.v1 | 股票 / total_share、float_share、free_share | 万股 ×10000 | [Tushare 每日指标字段表](https://tushare.pro/document/2?doc_id=32)及既有资本数量测试 |
| akshare / stock_zt_pool_em | akshare_eastmoney.current_day_limit_up_pool.v1 | 股票 / 封板资金 | 元，不缩放 | 下述 2026-10-24 官方映射、同花顺金额基准及当日 Raw 逐项核验 |
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
| baostock / query_history_k_data_plus / baostock.daily_bar.v1 | 非股票代码（包含指数） | 不能从同 endpoint 股票规则推断；身份没有股票规则即失败 |
| akshare / stock_zh_a_gbjg_em / akshare.capital.v1 | 四个股本数量字段 | 尚无独立来源单位证据；保留完整三类 Capital Raw，遇到 ShareCapital 记录则整批标准化失败，不能只删未知字段继续发布成功 |
| akshare / stock_zt_pool_dtgc_em / akshare_eastmoney.current_day_limit_down_pool.v1 | 封单资金 | 官方字段表未明确来源单位，独立来源观察标准化失败；不借用涨停资金契约 |
| 已退役 pysnowball 及其 Raw | 股票盘口数量 | 本整改不重新启用无已确认契约的适配器或 replay |
| 未登记 schema、endpoint、品种或字段 | 所有数量 | fail closed；未知股票/基金/债券不可借股票规则转换 |

技术因子来源公式与带符号 OBV 不由无符号整数股 helper 重定义。
2026-10-24接受的来源尺度澄清已替代此前的整批阻断/null隔离方案：三组OBV、EMV、MAEMV
完整保存和返回来源Decimal原值，不作手股换算，公开目录v3标记
`source_native / provider_native_factor_scale`。这不认证它们为股数或无量纲；
其他股票数量认证门禁仍有效。详见[技术因子运行手册](runbooks/stock-technical-factor.md)。

2026-10-24监管基准修正：ADR-0048已批准的独立股票指数聚合量契约已接入单位目录、
适配器、重放及入库认证。仅允许`000002.SH`、`000688.SH`、`399107.SZ`、`399102.SZ`；
不把指数点位当股价，不扩展到期货指数、THS或旧BaoStock指数Raw。来源元数据含原始手、
标准股、来源千元与标准元；旧Raw无这些标签可按原schema认证，有矛盾标签仍拒绝。
部署、补数和历史剩余范围见[监管基准恢复记录](runbooks/regulation-index-units-2026-10-24.md)。

### 2026-10-24：涨停来源“封板资金”核验

核验范围限定为 `akshare / stock_zt_pool_em / akshare_eastmoney.current_day_limit_up_pool.v1`
的“封板资金”，不扩展至跌停池或其他金额字段。

- [AKShare 官方输出表](https://akshare.akfamily.xyz/data/stock/stock.html#id424)
  对该字段只列出 `int64`，单位说明为空，不能据此认证为元。
- [AKShare 固定提交的适配代码](https://github.com/akfamily/akshare/blob/4771e6ea34f6139f226e2bf4fab88ce17579831d/akshare/stock_feature/stock_ztb_em.py)
  从 Eastmoney `getTopicZTPool` 获取数据，对“封板资金”仅调用 `pd.to_numeric`，未做乘除缩放。
  这证明 AKShare 未主动换算该字段，不能单独证明上游基准单位。
- [东方财富官方页面脚本](https://quote.eastmoney.com/ztb/newstatic/build/detail.js)
  中，`detail-ztgc` 的 `fund` 列描述为涨停价买入挂单的资金总量，使用 `numbericFormat`
  以万、亿缩略展示，未见该列的元/万元基准单位声明。核验文件 SHA256 为
  `4230dc2141d97c63223db43e5bb78241c4ad76c35b3f58eaca239ca1b3eb7ad5`。
- 复核更正：此前把“封单资金(手)”误归入 `ztgc`；该表头实际属于跌停首页 `dtgc`，
  不能作为涨停详情 `detail-ztgc` 的单位冲突证据，也不能用于认证涨停字段。
- 本轮另核对[官方源码映射](https://quote.eastmoney.com/ztb/newstatic/build/detail.js.map)
  的 `src/modules/tableconfig/confighead.ts`、`urlconfig.ts` 和 `src/modules/common/utils.ts`：
  `detail-ztgc` 对应 `getTopicZTPool`，`fund` 直接交给通用数字格式化函数；
  函数除以 10000/100000000 后添加万/亿，没有明确基准货币单位。
  源码映射 SHA256 为 `445587c116b915267d23f537f4afb73f59fb33a176f2df1f5509743ce01f1e17`。
  [官方另一份页面脚本](https://quote.eastmoney.com/ztb/newstatic/js/detail.js)行为相同，
  SHA256 为 `c38e66371790f495c90f6304163395feaa3380cb2d22b2cb7e198eaa98834e62`。
- 经用户授权，在 MuMu 中只读查看开盘啦：当前涨停列表为 82 家；华数传媒（000156）
  详情展示“封单 3903万”，买一价格 7.71、数量 5.06万。该页面没有明确标注封单基准单位，
  也未证明开盘啦与 Eastmoney `fund` 的来源对应关系。这仅是交叉核对线索，
  不以价格乘数量或数值接近作为来源单位认证依据。
- 同日继续在 MuMu 的同花顺查看华数传媒：买一栏显示 0.39亿，涨停数据面板的
  “最高封单额”显示 1.74亿，与开盘啦对应展示在精度范围内一致；当前封单与日内最大封单
  属于不同指标，不能混比。随后用户明确指定 Tushare `limit_list_d` 并授权恢复当天列表。
  [Tushare limit_list_d](https://tushare.pro/document/2?doc_id=298) 的 `fd_amount` 描述未列基准单位；
  [limit_list_ths](https://tushare.pro/document/2?doc_id=355) 的 `limit_amount`、`lu_limit_order`
  描述为“元|个”。须逐字段、榜单类别核对，不能将同花顺展示值或 Tushare 契约外推给 AKShare。

此前仅有文档空白和客户端展示时保持阻断。随后完成以下显式对照，不采用数量级/价格启发式：

1. Tushare `limit_list_ths`，`trade_date=20261023`、`ts_code=000156.SZ`、
   `limit_type=涨停池`，返回 `limit_amount=39029562.0`；该字段是官方表中的封单**额**（元），
   同次 `limit_order=5062200.0` 为另一数量字段，不能互换。该金额与同花顺 0.39亿、
   开盘啦3903万的展示吻合；没有通过股价乘数量反推单位。
2. 同日 Tushare `limit_list_d` 的 `fd_amount=39029562.0` 与上述金额完全一致。
   已保存的 AKShare v1 Raw 中华数传媒“封板资金”为 `39029562`，映射未缩放。
3. 完整按股票代码对照 Tushare `limit_list_d` 与既有 AKShare Raw，82条金额全部相等，差异0。
   其他人工展示核对样本：平安电工8789939元、曙光股份23970446元、奥普特45969345元，
   对应开盘啦879.0万、2397万、4597万。
4. 原批次 `39a3c676-d528-4a8e-a9fc-7ae5b371be92`，Raw
   `a0113adf-e761-4f0d-8b68-5c8fec05e514`，82条；完整性校验 SHA256
   `60d6b03be73eb5cf8bff9b866ce792aa1c233360d78c4821bff857b66fc48e70`。

结合官方 endpoint/字段/格式化链路和上述有明确金额语义的同日核验，将本目录精确身份的
“封板资金”登记为 `cny`，适配器使用 Decimal 原值，不乘除100/10000。
这不是跨来源通用单位推断器；未知 schema、跌停接口仍阻断。
本段涨停恢复中的Tushare仅作为核验来源，涨停生产适配器未切换。
随后跌停生产来源已独立切换为Tushare D，见本文件后文跌停契约；没有拼接来源批次。
没有修改不可变 Raw。历史快照和其他日期不会因这条静态规则自动认证，仍须逐批核验并受控重建。

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


## 2026-10-24 Tushare跌停封单金额契约

Issue #93，ADR-0056/0068澄清。新源唯一身份：tushare / limit_list_d /
tushare.current_day_limit_down_pool.v1 / stock / fd_amount，源单位CNY，标准字段
source_reported_sealed_funds_cny为元，使用Decimal原值，不乘除100或10000。

官方接口doc298：https://tushare.pro/document/2?doc_id=298 支持limit_type=D，
fd_amount为封单金额；limit_amount为跌停板上成交金额，禁止混用。
该页fd_amount注释只写涨停买单且未单列元单位，不能单独作为跌停完整单位证明。
补充实证：2026-10-23以D查询得到6条，fd_amount均有值；六只股票与既有独立
AKShare Raw逐symbol/精确日期金额相同。002388返回190983232.0，MuMu同花顺
新亚制程卖一金额显示1.91亿元，为金额单位交叉证据；同端点fd_amount涨停分支
已有此前82条及客户端金额核验。不通过股价乘数量、日期或数量级推断转换规则。
作为对照查询的limit_list_ths跌停池金额均null，不将该空响应当作单位证据。

文档不提供ST统计；2026-10-23规范主板候选8只、来源6只。无来源的规范成员保留
null及partial，不用其他提供商补字段。本次证据只认证新Tushare v1身份，不追认旧
AKShare跌停接口、其他采集版本或未知字段。Raw和旧快照保持不可变。
