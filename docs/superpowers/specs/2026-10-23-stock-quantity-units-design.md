# 股票数量单位全链路排查与整改设计

日期：2026-10-23（Asia/Shanghai）  
状态：设计已由项目所有者于 2026-10-23 接受，正在本地实现；不是整改完成报告。  
代码基线：master / f496ba8，并包含排查开始时已有的未提交技术因子、调度和文档修改。  
治理：实施任务以 [GitHub Issue #92](https://github.com/yuexing89757/data_center/issues/92) 为唯一记录；本文件是证据和设计，不替代 Issue。

## 1. 结论及授权边界

目前**不能验收为全部统一**。已定位入口转换缺漏、旧版重放恢复错误口径、来源单位证据缺口、
缺失值汇总和接口字段业务描述不足。历史修复尚未执行，Worker 端 Raw 完整性尚未核验，外部调用方尚未签收。

本轮仅执行代码/文档/测试检查、带 `default_transaction_read_only=on` 的数据库 SELECT、
本地 Raw 可读性抽查和合成数据复现。未运行生产 migration、采集、Raw 重放、历史更新、重算、
快照重建、服务重启、缓存清理或下游消息通知。未修改现有产品代码和已有用户修改。

实施前需要接受 [ADR-0068 草案](../../adr/ADR-0068-股票数量单位全链路契约与受控修复.md)：
现有远程 TDX 设计及 Issue #90 明确保留旧远程 v1 的错误重放结果，本次要求禁止这种回流；
新增历史修复账本和跨链路单位准入也属于需要明确决策的行为变化。不能静默把草案当成 Accepted。

## 2. 排查方法与证据边界

- [数据库证据](../../volume-unit-audit-evidence-2026-10-23.json)：来源版本汇总、当前行血缘、
  数量字段目录、表行数与日期范围、快照输入血缘、本地 Raw 抽查结果。只保存元数据与聚合，未保存 Raw 行、凭据或本地路径。
- [静态字段及接口目录](../../volume-unit-contract-inventory-2026-10-23.json)：118 个领域候选字段、
  27 个 FastAPI 数量/金额/比例候选 schema、31 个经响应引用找到的候选路由，及 PostgREST 路径。
  这些是审查候选，包含金额、比例等，不是 31 个已证实错误的接口。
- 数据库目录检索发现 107 个数量/股本/量比候选列，排除物理分区重复；JSON 因子叶子需要额外审查。
- 最后一次 `repeatable_read_snapshot.current_stock_daily_versions` 是同一只读可重复读快照，
  应优先于前期分次查询。其他探查分别在只读连接执行，采集进程仍运行，不能把不同时间的统计当成同一审计水位。
- 前期 OR 血缘连接超时，已改用等值连接和先聚合。部分探查引用错层级的日期字段而失败；
  错误保留在证据 JSON，后续改为表字段探查或关联 snapshot 后重查。
- `pg_stat_user_tables.n_live_tup` 只是估计，尤其分区父表可为 0；本报告表格使用后续精确 `count(*)`。
- 所有记录数量均为本轮观察值，实施时必须重新冻结候选和审计水位，不能直接以本报告数字执行更新。

### 当前日 K 存量（按实际行血缘，不按采集批次数推断）

| 业务 | 来源 / Raw 版本 | 当前行数 | 证券数 | 交易日期 | 本次判断 |
|---|---|---:|---:|---|---|
| 股票 | pytdx.local_daily_bar.v2 | 2,013,506 | 5,578 | 2025-01-02～2026-10-22 | 本地数量按股；不批量乘 100；仍需 Raw 逐项认证 |
| 股票 | pytdx.remote_daily_bar.v1 | 224,175 | 5,226 | 2026-08-11～2026-10-21 | 已知旧 normalizer 未转换手；历史修复候选，尚未逐项验证 Raw |
| 股票 | pytdx.remote_daily_bar.v2 | 8 | 8 | 2026-10-22 | 新 normalizer 已 ×100；禁止重复转换 |
| 股票 | pytdx / 无可用 manifest | 20 | 4 | 2026-08-04～2026-09-18 | 无法据血缘确定；列为隔离候选，不猜测 |
| 指数 | baostock.daily_bar.v1 | 270 | 3 | 2026-05-25～2026-09-29 | 独立指数单位核验，不套股票规则 |
| 指数 | tushare.index_daily.v1 | 112 | 4 | 2026-07-01～2026-10-22 | 独立指数单位核验，不套股票规则 |

`core.daily_bar` 现实中包含指数业务，不能仅凭表名把所有行标为“股票股数”。
本库 Raw 中远程 v1 共 240,439 个 manifest、241,349 条 Raw 行；这与当前受影响行数是不同统计对象。
本次未读到 AKShare 日 K manifest 或当前 AKShare 日 K 行，因此不能声称本库存在 AKShare 历史待修数量。

### 需要覆盖的其他存量

| 表 / 业务 | 精确行数 | 日期范围 / 说明 | 依赖与处置 |
|---|---:|---|---|
| realtime.five_level_quote_snapshot | 9,400 | 2026-08-14～2026-08-18 | pytdx_hq / 已退役 pysnowball；档位量、累计量、封单衍生 |
| derived.auction_quote_metric | 9,400 | 从原快照关联日期 | 深度股数、无量纲盘口不平衡、封单元；逐计算版本核验 |
| realtime.stock_quote_snapshot | 0 | 本库为空 | 腾讯请求时直连接口仍需检查，空表不证明接口正确 |
| realtime.call_auction_indicative_detail | 243,204 | 2026-08-14～2026-10-22 | 展示匹配量股，不能改成实际成交量 |
| realtime.call_auction_market_snapshot | 98,828 | 2026-08-13～2026-09-04 | 累计与五档量、竞价价格量额语义 |
| realtime.call_auction_market_series_snapshot | 835,776 | 2026-10-20～2026-10-23 | 热分区；不可只查父表统计 |
| realtime.call_auction_market_series_snapshot_history | 4,336,803 | 2026-09-09～2026-10-22 | 历史归档；必须与热表同口径 |
| realtime.eod_quote_snapshot | 2,626 | 2026-08-11～2026-10-21 | 涨跌停快照封单价量额的输入 |
| today_limit_up.member | 2,558 | snapshot 交易日期 2026-08-11～2026-10-22 | 含日成交量、股本与盘口股数 |
| today_limit_down.member | 324 | snapshot 日期 2026-09-18～2026-10-22 | 收盘卖盘股数及封单元 |
| core.board_index_daily_bar | 192 | 2026-01-05～2026-10-22 | THS 指数独立核验；不能从股票接口推断 |
| core.stock_technical_factor_snapshot | 1,057,417 | 2026-01-05～2026-10-22，1 个目录版本 | 三组 JSON 中 OBV/EMV 等须逐因子明确量纲 |
| metrics.classification_daily_metric | 0 | 本库为空 | 仍有可执行汇总/重算路径，必须修正准入及缺失语义 |

涨停池中 **2,528 个 member** 的 `daily_bar_ingestion_id` 直接指向远程 v1，
其中 **2,473 个 member 的 volume 非空**，日期 2026-08-11～2026-10-21。
这是已找到的下游候选，不代表这些 member 所有字段都错，也不等于可以原地修改不可变快照。
应按 snapshot 输入清单核验后生成新版本。

抽查 12 个数量相关 Raw 版本共 34 个最新对象，本地配置 Raw 根目录中 JSONL 和 gzip 表示均不可用。
这证明**本机无法完成 Raw 验证**，不证明生产 Worker 的 Raw 丢失或损坏。不得据此盲目修复或删除 manifest。

## 3. 已复现问题与风险

### F1：AKShare 股票日 K 未把手转换成股

`providers/akshare.py` 调用 `stock_zh_a_hist`，`_map_daily_bar` 将“成交量”直接交给 `_integer`。
官方字段表明确该接口成交量为手、成交额为元：
[AKShare 历史行情数据－东财](https://akshare.akfamily.xyz/data/stock/stock.html#id20)。
本机安装版本为 1.18.80；在线文档显示 1.19.1。历史版本必须结合当时安装包、上游接口与 Raw 核验，
不能用今天文档替代全部历史证据。

入口与 `normalize_akshare_raw` 共享此映射，因此采集和旧版重放都可能产出错量。
当前路由不自动用 AKShare 补普通日 K，不代表显式入口和 Raw 回放可免检。

### F2：远程 TDX v1 重放会重新引入手值

`providers/pytdx.py:normalize_pytdx_raw` 仅为 remote v2 走 `_remote_daily_bar_records`，
remote v1 走不换算的 `_daily_bar_records`。`tests/test_pytdx_provider.py` 的
`test_legacy_remote_raw_replay_preserves_original_volume` 明确断言旧错误结果。

合成输入复现：同一 Raw 来源量 `1234`，v1 返回 `1234`，v2 返回 `123400`。
Issue #90、RemoteTdxDailyBar 详设及相关 ADR 增补刻意保留该行为，整改必须显式替代此决策。
不能把错误结果保真解释为标准事实可继续按手入库。原始 Raw 仍需原样保留。

### F3：量纲/版本缺少统一、可执行的准入证据

当前有 schema allowlist、Raw 哈希、Provider/来源校验和领域校验，但缺少覆盖全部数量字段的
`provider + endpoint + schema_version + instrument_type + field` 单位登记及入口校验。
大多数 request_params 不携带单位证据，只有远程 TDX v2 等少数例外。
不能从来源名整体推断同一 Provider 全部接口同单位。已退役 pysnowball Raw 也要纳入证据目录，
未受支持的回放应继续明确阻断，不能为整改重新启用未知语义的适配器。

### F4：分类汇总把 null 当零

`calculators.py:calculate_classification_metrics` 使用 `bar.volume or 0` 和 `bar.amount or ZERO`。
部分成员缺量/额时会输出一个看似完整的总数，没有相应完整性口径。这不是通过乘 100 能修的问题。
建议保留完整成员集合，任一应参与成员缺少数量时总量为 null，并明确覆盖数；
实现前须同步领域记录、SQL 可空性、RPC、算法版本及文档，不擅自改成“缺失行剔除后求和”。
“应参与成员”沿用当前精确交易日选择的 `active.members`，不在本整改中按上市状态、停牌或退市另改成员集合。
成员整条日 K 缺失与量/额字段 null 分别记为缺失，任一缺少即使对应合计为 null；
volume 与 amount 各自维护覆盖数。停牌只有来源明确的 0 才计零，不能凭停牌状态制造 0。
上市前/退市后成员如仍在选定集合且缺日 K，记录成员生命周期/覆盖质量问题，合计为 null，不自动剔除。
原有价格收益统计的成员与分母不随数量整改改变。

### F5：临时来源接口允许非整数股进入标准数量字段

`providers/auction_sources.py:_bar/_quote` 用 Decimal 乘因子，但没有验证最后结果为整数股。
合成新浪 `volume=0.5` 能输出 `volume_shares=0.5`。股数事实应接受 1、37、50 等非整手数量，
但不应接受 0.5 股；均量等统计量可为 Decimal，不能复用整数事实约束。
腾讯五档与 pytdx_hq 已各自执行整手 ×100 校验，东财展示匹配量亦有转换；
这些现有实现是代码证据，不是所有接口及历史版本来源单位都已核实的证明。

### F6：指数、可转债及技术因子需要独立契约

- 可转债 `volume` 有独立“张”契约，Tushare `cb_daily` 当前 ×10，但 `int(volume_lots * 10)` 会截断非法小数张。
  需独立证据和精确整数校验，禁止机械 ×100。
- THS 动态板块日线直接读取来源数量，数据库注释却写“股”；常规与 live 两种 Raw 必须分别核验。
- 指数日 K 已进入 core，标准股票单位不得被用来给指数成交量冒名。
- 技术因子三组 JSON 直接保存 Decimal。OBV 是数量相关因子，EMV/MAEMV 对成交量尺度敏感；
  MFI/VR 等比率须给出实际来源公式、窗口、缩放常数与量纲。不能对所有相关因子统一 ×100。
  本轮官方技术因子页面未提供可核实的完整因子量纲，故尚未认证。

### F7：字段名相同不代表相同业务

OpenAPI 有按字段名补全中文的通用描述，例如 `volume_shares` 的缺省描述为盘口数量，
`cumulative_volume` 泛称累计数量。实际字段分别用于成交量、盘口量、竞价指示量和分时累计量。
需要模型级描述与 `x-unit`、业务语义，不能用一个全局文案证明所有接口满足契约。
api_v1 视图数量列目前未查到列注释；底层股票列已有股注释，债券已有张注释，但注释并不证明值正确。

### F8：本地日线金额读取有额外舍入

本地 `.day` 解码用 float32 读取来源金额，再 `:.0f` 格式化为整数文本，可能额外丢弃元以下精度。
来源二进制精度损失本身不能恢复；应准确保存来源表示并解为 Decimal，禁止为了单位统一再加浮点或整数截断。
此变更要另升 Raw 采集版本，历史金额不靠价格乘量推算，不能与成交量 ×100 混为一次盲目修复。

### F9：只查 checked-in 契约会漏掉数据库入口

数据库目录中有 8 个数量相关查询函数未出现在当前 PostgREST OpenAPI 路径中：
`query_auction_one_price_limits`、`query_call_auction_grab_lines`、
`query_call_auction_indicative_details`、`query_call_auction_one_price_patterns`、
`query_daily_limit_down_list`、`query_daily_limit_up_list`、`query_limit_up_pool`、
`query_recent_daily_bars`。应逐个核实当前角色授权、FastAPI 调用与是否退役，再同步契约。
函数存在不等于已经向匿名消费者授权，不能通过扩大权限来“同步”。
`persist_call_auction_indicative_details` 是受控写入桥接，不列为公共只读契约，也不为整改扩展它的权限。

## 4. 来源及链路覆盖清单

| 边界 | 已查路径与现状 | 实施要求 |
|---|---|---|
| BaoStock | 日 K 直接股数；本库当前为指数输入 | 股票/指数分开认证，金额元，版本映射明确 |
| AKShare | stock_zh_a_hist 漏转换；股本、股东等另有映射 | endpoint/version 分别核实，股票手×100，股数不再缩放 |
| pytdx 日 K | 本地股、远程 v1 漏转换、远程 v2 已转换 | 禁止 price_scale 判断单位；新旧 schema 都只能产出已认证标准值 |
| pytdx_hq / 腾讯五档 | 累计量与各档现有 ×100；Raw 版本独立 | 验证累计、当前、买卖量各字段，不只检查 bid1 |
| 东财竞价展示 | 来源手转股，缺档有自己的含义 | 保留 displayed/indicative 业务，不将指示量伪称成交量 |
| 竞价序列 Raw | v1/v2 是已标准化归档结构，非线上的手值 | 禁止再次 ×100；按归档版本和实际 Provider 证明量纲 |
| pysnowball 退役历史 | 存量 Raw/五档快照仍存在 | 独立核验旧版本；不凭其他 Provider 结果推断 |
| Tushare | daily 手×100、amount 千元×1000；股本万股×10000 | 分开 daily/index_daily/cb_daily/daily_basic/stk_factor_pro |
| THS 板块 | 常规/live 独立读取，无转换证据闭环 | 明确指数原生数量单位，未确认不宣称股 |
| 临时东财/腾讯/新浪 | 对应代码有×100/×1；Decimal 结果无整股约束 | 逐 endpoint 固定来源单位，不能以价格/日期/量级猜测 |
| 开盘啦接口 | 主要是金额、比例及来源观察，独立 DTO | 每字段核实元/比例/百分数；不能按名称把全部数值当数量 |
| 标准 DTO/校验 | DailyBar、OrderBook、Auction、StockPool、Derived 等 | 整数事实/Decimal统计量区分，类型、有限、非负/允许有符号范围、null 检查 |
| 存储/批处理/定时 | Provider→pipeline/services→Persistence；Worker 共用入口 | 单位准入在共享写入路径覆盖手动、批处理和调度；不增加 OS 任务 |
| 派生 | 深度/封单、分类汇总、快照成交量、监管来源换手率 | 建立字段级依赖；源换手率不假称由本库成交量计算 |
| 历史重放 | reliability.py 的 Provider normalizer 分派 | Raw hash/行数/schema + 单位证据双门禁；禁止旧代码结果回灌 |
| 归档 | AuctionSeriesArchiveService 调 Persistence 搬运 | 热表/历史表同契约；受影响不可变数据生成新版本 |
| 缓存 | 竞价实时接口有 0～5 秒进程内缓存 | 只缓存认证后的标准对象；发布切换时失效，旧状态不得恢复 |
| 文件/导入导出 | Raw、质量归档、审计报告和内部结果；未找到通用行情 CSV/XLS 导出入口 | 文件契约带版本/业务/单位；Raw 不作为标准文件；存在性不是全生态保证 |
| REST/PostgREST/RPC | FastAPI typed schema、api_v1 views/RPC | 同一事实对照，更新三个 checked-in contracts 和 SQL 注释 |
| MCP/Agent | 有 agent-tools 描述，项目未建设 MCP 服务 | 更新工具契约；不为了验收新建 MCP |

## 5. 目标契约（设计，尚未实施）

1. 所有股票成交、逐笔成交、竞价、各档买卖盘及累计事实为整数“股”；均量为 Decimal“股/指定窗口”，
   最大/最小量保持股。逐笔仅规定未来边界，当前不引入采集或 tick 领域。
2. 成交金额、封单金额等为 Decimal“元”；报价须说明股票元/股、债券元/张或指数点。
3. 缺失为 null；零保留零。允许非整手成交，如 37 股；拒绝小数股、NaN、Infinity、bool、负的无符号事实及溢出。
   有符号数量因子按其独立字段契约校验，不套非负成交量规则。
4. 来源股票手数可为精确 Decimal：0.37 手→37 股、0.5 手→50 股；0.001 手不产生可接受整股。
   转换仅在 providers 完成；之后只传标准值。来源已股→原样，不依赖数值、价格、日期推断。
5. 单位登记键是 Provider、endpoint、Raw schema、品种和字段；记录来源文档/实现证据、数量/金额缩放、
   标准单位、normalizer 版本和生效范围。未登记/矛盾证据硬失败，Raw 可保留，标准数据不得通过。
6. 同一 schema 的历史 Raw 字节不改变。修复标准化输出需要独立 normalizer/单位契约版本和新 ingestion 血缘，
   不能伪称 Raw 是另一个采集版本，也不允许“兼容”分支重新输出手。
7. 非股票品种按独立来源与业务契约登记；可转债张、基金份、指数原生/明确聚合量纲各自认证。
8. 项目规范、宪法统一契约说明、领域详设、字段注释、接口说明、OpenAPI 的单位与业务从同一审查目录核对。
   现有字段名可保留；不能把所有 `volume` 无差别改名或统一乘 100。

### 派生公式与版本

| 字段业务 | 明确公式与单位 | 缺失/来源规则 |
|---|---|---|
| N 日均量 | 完整 N 个应计交易日的股数和 / N，单位股 | 窗口缺量为 null，不能缩小分母或补零 |
| 最大/最小量 | 完整声明窗口内的 max/min(股数)，单位股 | 是否排除停牌必须沿既有业务设计，不能在整改中暗改 |
| 日量比（如现有业务采用） | 当日股数 / 同口径前 N 日平均股数，无量纲 | N、当日是否完整须注明；分母 0/null 为 null |
| 盘中量比 | 当日累计股数/已过有效交易分钟，除以基准窗口每分钟均量，无量纲 | 交易分钟、午休及窗口由具体源契约证明；不得冒称本库已实现 |
| 流通/自由流通换手率 | 对应期间成交股数 / 对应生效流通或自由流通股本 ×100，单位百分点 | 不交换股本分母；来源值注明 source-reported 与窗口，不强行用本库重算 |
| 五档深度 | 五档股数和，单位股 | 缺档沿既有完整性语义，不伪造零 |
| 封单金额 | 对应档位价(元/股)×股数，单位元 | 竞价时点、方向与指示/成交语义不变 |
| 盘口不平衡 | (买深度－卖深度)/(买深度＋卖深度)，无量纲 | 分母零/缺失保持 null |
| 分类合计 | 同一成分时点完整成员股数/元的和 | 缺量/额输出相应 null，保留覆盖信息并升算法版本 |
| OBV/EMV/MAEMV/MFI/VR | 按来源具体公式登记量纲、窗口、常数、复权口径 | 未完成证据核实前不得标为标准股数或统一乘除100 |

表中公式是契约要求；目前并未发现统一的本地均量/盘中量比计算域，不为整改新建这些业务。
Tushare daily_basic 的量比、换手率与技术因子是独立来源事实，不依赖本库日 K 的重新计算，
只有证明来源/尺度或窗口错误后才修对应来源结果。

## 6. 历史修复设计

采用“证据驱动重建目标值”，不执行 `UPDATE volume=volume*100` 全表操作。

### 预览与冻结

拟提供只读审计及修复预览入口，默认无写操作。筛选必须指定业务表、字段白名单、证券类型、
来源/endpoint/schema、交易日期范围、原 ingestion 与 Raw manifest。
输出确定可修、已正确、证据不足、Raw 不可读、冲突/并发变更分类、范围、数量及下游依赖。
以有界分页读取，不做全库长事务；记录候选水位，最终签名/hash 固定精确自然键集合。
审计对象身份、前后值等行级证据保存在受控审计存储，不提交行情 Raw/修复前市场行到仓库。

### 认证粒度及启用顺序

这是实施者提出的具体方案，尚未获得所有者批准：

- 静态代码目录认证“某 Provider/endpoint/schema/品种/字段的来源量纲与转换规则”；
  这不能代替对某条历史事实的血缘认证。
- 持久认证以 `(ingestion_id, unit_contract_version, field_group)` 为粒度，
  保存有界字段组、关联 Raw hash 集合、normalizer/规则版本、认证状态及证据摘要。
  新增受限 `audit.quantity_unit_certification`，经 migration 及权限测试；不把证书塞入领域 DTO。
- 历史已正确的 local v2/remote v2 也需在 Worker 端验证所依赖的 manifest 和 Raw 完整性，
  核对已知 normalizer 的标准输出，再按 ingestion 认证；不因 schema 名称看似正确就免检，
  也不为所有 201 万个正确日 K 额外创建逐行修复账本。实际值与认证输出冲突则单独分类。
- 真正要修改的记录才创建逐行 repair item，并且必须证明当前自然键/原 ingestion/before 值与该 Raw 对应。
  remote v1 未修复前不能获得“股”的合格证；20 条缺 manifest 也不能获得。
- 先在 Worker 做只读认证、形成固定范围，再分阶段启用生产读/写门禁；
  不因本机缺少 Raw 就突然屏蔽全部 201 万行，也不在认证前宣称它们已验证通过。
  新证书、修复后新 ingestion 与事实写入必须同事务提交，读入口不能观察未提交的认证状态。

证书只是经证据验证的单位事实，不是按日期猜测单位的白名单。恢复旧备份/旧归档后缺少对应证书，
须重新认证；不能导入一份脱离 Raw hash 与事实血缘的“全量已通过”标记。

### 可执行修复与回滚

- 通过有序 SQL migration 新增 `audit.quantity_unit_repair_run/item` 受限账本（名称待实现核对），
  不改 Raw schema 内容，不授予 public 写权限。记录审批范围、证据摘要、版本、分页进度、统计和结果。
- Item 唯一身份包含修复规则版本、表/字段、自然键、原 ingestion、原 Raw hash；
  保存必要的修复前值/血缘、目标值/新血缘、before/after hash、结果状态和时间。
- `--apply` 必须引用经审核预览摘要；默认预览。批次建议 500 行，由实施者按锁/超时测量下调。
  不把当前报告日期范围自动扩展到未来采集，也不从模糊来源批量推断单位。
- 每批对仍符合 before 值与原 ingestion 的事实执行受限 compare-and-set，数据更新、
  新 ingestion 及账本提交同一事务。发生并发变化归为 conflict，重新预览，不能覆盖新事实。
- 修复目标值来自校验通过的原 Raw，经正确版本 normalizer 精确转换；不是从当前数据库值再乘一次。
  已达目标且血缘/账本一致为 already_applied。无账本但值已等于目标须独立核实，不能自动再次变换。
- 批次提交后记录游标；崩溃后以 durable item 状态恢复。再次执行同一计划不再换算。
  若事务失败则该批全回滚，先前批次保留；计划总计明确未完成项。
- 回滚使用保存的 before 状态，只有当前 after hash/新血缘仍匹配时才允许；否则停在 conflict。
  回滚不恢复错误为可发布事实，读屏障须把未认证量继续标为隔离/缺失。
- 沿项目备份恢复规范准备精确范围的恢复依据；不使用 ad-hoc DDL、create_all、Alembic 或删除表重采。
- 无 manifest 的 20 行和无法读取/确认单位的对象保留独立清单；未获操作授权前不修改现有行，
  发布阶段通过读/计算准入隔离未认证数量，不静默删除整条行情。

### 派生、快照及缓存

冻结原始输入证据后，只对有真实数量依赖的结果重算：分类量额合计、快照复制的 volume、
深度及价量金额等。价格、收益率、复权价格、涨跌停规则不因数量整改而改变。
价格驱动结果可记录“依赖已检查且不受数量影响”，不能统一全业务重跑。

涨停池 2,528 个 v1 输入 member 先关联 snapshot/input_hash，发布新 snapshot/version/calculation_id，
不更新原 member。旧版通过现有选择机制退役，保留审计可查且不混入默认结果；
若需要新增退役/质量标记，应纳入同一受保护 migration 并测试精确日期选择。
若某不可变表现有机制不能表达替代版本，不绕过 append-only 约束，先扩展既有版本机制再重建。
热/历史竞价序列需分别验证；不直接篡改已归档版本。

进程内竞价缓存只接收已认证标准对象，发布时失效。若将来有持久缓存，缓存键必须含单位契约版本，
恢复入口同样拒绝旧版本。现仓库未查到 Redis 等独立持久行情缓存，不能据此证明所有外部缓存不存在。

## 7. 接口兼容与调用方

- 直接受候选日 K 影响：`GET /api/v1/daily-bars/{symbol}`、
  `api_v1.daily_bars`、`query_daily_bars`、`query_recent_daily_bars`。
  涨停列表、分类汇总和其他快照按实际输入血缘判断；旧值如确为手，修复后的同一 `volume`
  数值将为旧值的 100 倍，含义始终为文档承诺的股。已正确数据不变。
- 五档/竞价/分时/临时 K 线候选路由及 schema 见机器目录；字段必须逐模型写成交股数、
  委托股数、展示匹配股数或累计股数。业务语义不能因单位相同而合并。
- 技术因子 `POST /v1/stock-technical-factors/query`、两个技术因子 RPC 因叶子在 JSON 中，
  不会被顶层 `volume` 搜索发现，必须单独列入验收。
- PostgREST/Agent 契约当前部分响应只是宽泛 description，需补字段业务、单位、null 与示例；
  `agent-tools` 是工具描述，不是项目存在 MCP 服务的证据。
- v1 已承诺股的字段按纠错处理，发布记录必须列精确范围与数值变化；没有依据不把错误单位当“兼容特性”。
  若发布新增质量状态、normalization version 或不能兼容的 JSON 因子量纲，明确升相应契约/因子目录版本，
  不能同一目录版本暗改既有因子缩放。
- 未修复/未认证历史数量不能继续冒称股对外提供。读屏障保留行情主体并显式返回数量 null 与质量原因，
  每个入口行为按下述规则在 contract tests 固定，不能每个路由自行选择隐藏方式。
- 已知内部调用方：pipeline、batch、Worker jobs、reliability replay、classification calculator、
  today_limit_up/down、auction services、archive service、FastAPI PostgresReader 与临时 Provider。
- 仓库文档提到九合一消费应用、研究脚本和 apiServer 技术因子调用；真实部署版本、
  调用人及是否额外乘/除100没有可验证注册表。本轮不能宣称调用方已适配。
  发布前建立调用方确认表，逐一记录接口/字段、旧补偿、切换版本与验证状态，不触发群消息。
- 规范示例统一为：**成交量 100 股就是 100 股，不是 100 手；0.37 手的来源事实转换为 37 股。**

### 未认证数量的具体公开/写入行为

以下为设计者提出、需要随方案批准的兼容选择；不描述为已有能力：

| 业务 | 标准写入/计算 | 公共读取 |
|---|---|---|
| 新采集或 replay 任一必要数量字段单位不明 | Raw 可留存，整批标准化失败，failed run及error质量记录；不能将未知来源数值改成null后继续当成功批次入标准层 | 不发布这批来源原生数量 |
| 既有日 K / 股本 / 普通只读视图、RPC | 未认证数量不参与计算，产生明确输入不完整状态 | 保留非受影响事实，数量为null；附 `quantity_quality`，含单位契约版本和字段组的status/reason，不静默隐藏整行 |
| 既有 THS / 指数 | 独立契约未认证前不接受新标准批次 | 数量null及明确质量原因；已认证价格、指数点位可读，不改价格语义 |
| 既有技术因子 JSON | 未核实必要数量相关因子量纲，阻断对应新标准批次；不重定义来源公式 | 升因子目录/响应契约版本，未认证叶子保留key但为null，`quantity_quality`指出因子组/叶子；价格/明确无量纲叶子只在自身契约已确认时可读 |
| 不可变快照默认读取 | 仅发布已认证输入的新版本；不把修复批次与旧calculation_id混用 | 精确日选择已认证版本；无合格版本时返回现有领域的不完整状态并附质量原因，不回退旧日期或旧有问题版本 |
| 显式请求旧有问题 snapshot/version | 保留原对象仅供受限审计，不原地改内容 | HTTP返回409 `quantity_units_unverified`；RPC返回同一machine-readable错误，PostgREST映射409。不能凭显式旧版本绕过门禁 |

`quantity_quality` 是拟新增的稳定字段，不含内部路径、密钥或原始来源payload。
api_v1普通视图也须暴露可机读的认证状态及数量null语义，而不只由FastAPI临时遮蔽。
快照若没有可公开的显式版本参数，也要测试内部可达的旧版本不会经其他公共RPC泄漏。
旧技术因子原生值只保留在Raw/受限审计层；不得以“source-reported”标签绕过标准数量契约继续对外发布。

## 8. 验证设计和当前测试证据

实施必须新增能够先复现错误再通过的测试；已有测试通过不能替代整改回归。

| 层次 | 必须覆盖 |
|---|---|
| 来源适配 | 已认证手/股各接口、金额元/千元、0/null、37股/0.37手/0.5手、非法小数股、负数、bool、非有限和极大值 |
| 版本与回放 | AKShare v1/新采集版本、TDX 本地 v1/v2、远程 v1/v2、竞价归档v1/v2、未知版/品种阻断、Raw hash与压缩恢复 |
| 无重复换算 | 新采集→Raw replay 结果相同、标准归档不×100、二次修复already_applied、断点续跑、事务中断、并发冲突与回滚冲突 |
| 质量门禁 | 未知单位阻断整批标准写入，保留失败运行及error级质量证据；异常100倍只告警，不决定转换 |
| 领域/派生 | 均量窗口和分母、null/零、深度/封单金额/比例、分类缺失、signed OBV 独立校验 |
| DB/版本 | 受限角色、RLS/grants/timeout、审计唯一键、幂等、快照新版本及精确日选择、热/历史分区 |
| 缓存及API | Provider→标准对象→DB→缓存/快照→RPC/PostgREST/FastAPI同值，null保留、业务单位描述与x-unit、示例100股 |
| 非股票 | 指数、THS、可转债张、未来基金份与股票规则隔离；无新tick/MCP领域 |

本轮基线命令：

```text
uv run pytest tests/test_pytdx_provider.py tests/test_pytdx_daily_bar_protocol.py tests/test_akshare_provider.py tests/test_pytdx_hq_provider.py tests/test_tencent_quote_provider.py tests/test_auction_sources.py tests/test_raw_store.py tests/test_calculators.py -q -p no:cacheprovider
145 passed, 3 skipped in 35.68s
```

3 项跳过均是 Windows 无法创建测试符号链接；不是数量测试已通过完整端到端验收。
合成 TDX v1/v2 与新浪小数股问题已在当前运行时复现。
上述是设计阶段的只读基线。所有者接受方案后已开展本地实现；最终门禁结果以
[交付与操作边界](../../stock-quantity-unit-remediation-2026-10-23.md)的实施验证记录为准。
实施后须运行：

```text
uv run ruff format --check .
uv run ruff check .
uv run mypy src
uv run pytest
uv run pytest -m integration
```

集成测试只能使用隔离可丢弃的 `TEST_DATABASE_URL`，绝不能用本轮只读检查的业务数据库替代。
未运行的命令不得记成通过。

## 9. 实施顺序与验收门禁

先把设计与验收范围登记 GitHub Issue，接受 ADR-0068 及对应领域澄清，再实施：

1. 契约与入口：补单位证据目录、共享精确适配校验、新采集/标准化版本，修 AKShare，阻断不合规旧版写入；
   同步领域文档与接口字段级注释。旧 Raw 保持不可变。
2. 保护标准读/计算：对历史未认证数量提供明确隔离状态，禁止批处理、回放、缓存/快照恢复重新发布错误口径。
   同步 contracts；审计与质量能力只在现有领域内增加，不引入新服务。
3. 工具与隔离验证：实现默认预览的历史脚本、受限账本 migration、分批幂等/恢复/回滚和快照替代机制；
   用可丢弃数据库跑全链路验证。历史脚本及 migration 文件存在不代表已执行生产操作。
4. Worker 端只读核验：读取批准范围的实际 Raw，逐对象验证完整性和单位；形成固定候选、无法修复清单、
   下游范围、备份与恢复证明。部署代码与服务启停遵循现有生产运行手册及操作授权。
5. 生产操作：只执行已明确授权且有精确计划摘要的 migration、修复、重算和快照切换。
   本轮没有执行授权范围与备份证据，不能把整改请求视为一次无限范围的历史写入批准。
6. 验收：重新只读对账、相同事实多入口比较、Raw replay和缓存恢复验证、调用方去补偿签收，
   发布修复审计与剩余缺口；全部门禁满足前状态保持“部分整改/待验证”。

最终交付必须同时包含：字段/来源覆盖清单、可复现代码变更、历史预览/执行/回滚脚本、
生产前后审计统计及证据hash、派生与快照新版本清单、同步接口文档/三份contracts、
测试命令与结果、未认证/未修复/未适配/未执行范围。不得用只通过单元测试替代历史与端到端验收。

## 附：核心只读查询的复现方式

连接凭据仅从现有环境配置读取；启动连接时强制只读并设置有界 statement timeout。
以下 SQL 仅 SELECT 元数据与聚合，使用已部署列，避免把物理分区父表统计当精确结果。
当前行血缘应在一个只读可重复读事务中读取；大范围 Worker 端 Raw 检查另做分页水位冻结。

```sql
begin transaction isolation level repeatable read read only;
set local statement_timeout = '25s';
show transaction_read_only;

select coalesce(s.security_type, 'UNREGISTERED') as business,
       b.source_code,
       coalesce(replayed.schema_version, original.schema_version, 'MISSING') as raw_version,
       count(*) as records, count(distinct b.symbol) as securities,
       min(b.trade_date), max(b.trade_date)
from core.daily_bar b
left join core.security s using (symbol)
join ingestion.ingestion_run r on r.ingestion_id = b.ingestion_id
left join ingestion.raw_manifest original on original.ingestion_id = r.ingestion_id
left join ingestion.raw_manifest replayed on replayed.raw_id = r.replayed_from_raw_id
group by 1, 2, 3
order by 1, 2, 3;

select count(*) as affected_input_members,
       count(*) filter (where m.volume is not null) as nonnull_volume_members,
       min(s.trade_date), max(s.trade_date)
from today_limit_up.member m
join today_limit_up.snapshot s using (snapshot_id)
join ingestion.raw_manifest raw on raw.ingestion_id = m.daily_bar_ingestion_id
where raw.schema_version = 'pytdx.remote_daily_bar.v1';

rollback;
```

本库已核查相关日 K 批次未存在多个 manifest 导致计数倍增；若其他部署同一 ingestion 有多个对象，
先按行键/manifest 分片证明唯一对应关系，不能直接复用此聚合来生成修复计划。
