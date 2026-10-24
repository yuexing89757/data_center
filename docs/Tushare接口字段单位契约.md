# Tushare 接口字段单位契约

核验日期：2026-10-24。范围为本项目已接入接口及本轮封单金额核验使用的接口，
不是 Tushare 全站字段认证。权限清单与单位契约分别维护；有调用权限不等于单位已经认证。

## 阅读与维护规则

- **官方明确**：链接中的字段表直接给出单位。
- **项目已确认**：已有领域契约或逐批核验依据；如官网没有明写，必须说明证据范围。
- **待确认**：缺少独立证明，不能靠字段名、价格、数量级或其他接口推断。
- **来源指标尺度**：保存供应商指标原值，不宣称为股数或无量纲，不擅自乘除100。

每条规则须绑定接口、业务、字段及 Raw schema，执行目录见
[来源数量单位目录](source-quantity-units.md)与 `providers/quantity_units.py`。
来源转换只在适配层执行一次，使用 Decimal；null 保持 null，零保留为零。
文档核实不等于运行时已放行，也不等于历史批次已经修复。

## 行情、每日指标与股本

| 接口 / 业务 | 来源字段 | 来源真实单位 / 证据状态 | 项目标准口径及当前处理 |
|---|---|---|---|
| `daily` 股票成交量 | `vol` | 手；官方明确 [doc27](https://tushare.pro/document/2?doc_id=27) | ×100 → 股；必须精确到整数股 |
| `daily` 股票成交额 | `amount` | 千元；官方明确 doc27 | ×1000 → 元 |
| `daily` 盘后固定成交 | `ah_vol`、`ah_amount` | 分别为手、千元；官方明确 doc27 | 当前字段清单未请求，未形成项目标准字段；不得直接复制到标准输出 |
| `daily_basic` 股本 | `total_share`、`float_share`、`free_share` | 万股；官方明确 [doc32](https://tushare.pro/document/2?doc_id=32) | ×10000 → `total_shares`、`circulating_shares`、`free_float_shares`，股 |
| `daily_basic` 市值 | `total_mv`、`circ_mv` | 万元；官方明确 doc32 | ×10000 → `total_market_value`、`circulating_market_value`，元 |
| `daily_basic` 换手率 | `turnover_rate`、`turnover_rate_f` | 百分比数值；官方明确 doc32 | 原值 → `turnover_rate_pct`、`free_float_turnover_rate_pct`；2.5 表示2.5%，不是250% |
| `daily_basic` 股息率 | `dv_ratio`、`dv_ttm` | 百分比数值；官方明确 doc32 | 原值 → `dividend_yield_pct`、`dividend_yield_ttm_pct` |
| `daily_basic` 量比 | `volume_ratio` | 比值，官方公式 `VOL/MA`；doc32 | 原值、无量纲；公开表未给出 MA 的完整窗口，不能假定本地均量可完全复现 |
| `daily_basic` 估值 | `pe`、`pe_ttm`、`pb`、`ps`、`ps_ttm` | 同币种金额之比，倍；doc32给出分子分母 | 原值，不按百分比缩放；缺失不补零 |
| `index_daily` 指数点位 | `open`、`high`、`low`、`close`、`pre_close` | 点；官方明确 [doc95](https://tushare.pro/document/2?doc_id=95) | 指数点位，不是股票价格“元/股” |
| `index_daily` 指数聚合成交 | `vol`、`amount` | 官方分别明确为手、千元；doc95 | **运行时指数数量契约仍阻断**。尚未建立按指数品种的独立聚合量契约，不能仅因官网写手就套用股票规则 |

股票换手率公式为 `同期间成交股数 / 对应流通股数 ×100%`；自由流通换手率使用自由流通股数。
量比的分子分母必须采用相同数量单位及时间口径。市盈率为总市值/净利润，TTM使用滚动净利润；
市净率、市销率按官方对应净资产、收入定义，不能只因都是“倍”就互换。

`daily` 适配器存在不代表普通股票日线自动路由使用 Tushare；自动路由仍遵循 ADR-0024。
指数规则待完成契约、适配器与 replay 测试后才能恢复标准化，本次文档维护没有放行。

## 股东、财务与板块资金

| 接口 / 业务 | 来源字段 | 单位与证据 | 当前项目处理 / 易混点 |
|---|---|---|---|
| `stk_holdernumber` 股东户数 | `holder_num` | 户；[doc166](https://tushare.pro/document/2?doc_id=166) | 整数户，不是股数 |
| `top10_floatholders` 持仓 | `hold_amount` | 股；官方明确 [doc62](https://tushare.pro/document/2?doc_id=62) | 原整数股，不乘10000 |
| 同上，持仓比例 | `hold_ratio`、`hold_float_ratio` | %；doc62 | 分别为持股数/总股本、持股数/流通股本 ×100%；原值 |
| 同上，持股变动 | `hold_change` | 股、带符号；项目已确认 ADR-0064 | 官方只写持股变动，未列单位；项目按较上期增减股数处理。0为不变，null为来源新进含义，不能转成0或百分比 |
| `fina_indicator` 扣非净利润 | `profit_dedt`、`q_dtprofit` | 项目契约为元；ADR-0020 | 分别为累计、单季度值，Decimal原值；[doc79](https://tushare.pro/document/2?doc_id=79)没有直接写明这两项的货币缩放，属于仍需补强的独立来源证据，不能标成“官方已明确” |
| `moneyflow_ind_dc` 板块净资金 | `net_amount`、`buy_elg_amount`、`buy_lg_amount`、`buy_md_amount`、`buy_sm_amount` | 元；官方明确 [doc344](https://tushare.pro/document/2?doc_id=344) | 原值，可负；`buy_*` 在该接口是对应档位**净流入**，不是买入总额 |
| 同上，占比 / 涨跌幅 | 五个 `*_amount_rate`、`pct_change` | %；官方明确 doc344 | 原值。官网未完整定义净占比分母，保留来源口径，不擅自重算 |
| 同上，板块指数 | `close` | 指数点位；doc344 | 不是元，不参与股价×股数公式 |
| 同上，名称 / 排名 | `buy_sm_amount_stock`、`rank` | 股票标识文本、排名序号 | 前者不是金额；后者不是股数 |

`stock_basic`、`trade_cal`、`stock_st`、`disclosure_date` 的代码、日期、状态及分类字段
不赋予股/元单位；分类数字不是数量。

## 涨跌停列表：封单金额与板上成交金额分开

| 接口 / 参数范围 | 字段 | 真实单位 / 核验范围 | 当前使用情况 |
|---|---|---|---|
| `limit_list_d`，U/D | `fd_amount` | **元，原值**；项目已确认，官方 [doc298](https://tushare.pro/document/2?doc_id=298)未明写基准单位 | D为生产跌停来源，映射封单资金；U用于涨停资金交叉核验，不代表生产涨停来源已切换 |
| `limit_list_d`，D | `limit_amount` | 跌停价格成交的金额总和；基准缩放待独立确认 | **不是封单资金**，当前生产跌停字段清单不请求，不得顶替 `fd_amount` |
| `limit_list_d` | `amount`、`float_mv`、`total_mv` | 金额/市值业务；doc298未写元/万元基准，待独立确认 | 当前跌停适配器不请求，不能借用 `daily_basic` 的万元规则 |
| `limit_list_d` | `open_times`、`limit_times` | 开板次数、连续封板计数；doc298 | 整数计数，不能作为成交股数 |
| `limit_list_ths`，已核验涨停池样本 | `limit_amount` | 封单额，元；官方 [doc355](https://tushare.pro/document/2?doc_id=355)混写“元\|个”，结合明确金额展示核验 | 仅核验用途；与 `limit_list_d.limit_amount` **业务不同** |
| `limit_list_ths` | `limit_order`、`free_float`、`lu_limit_order` | 官网“元\|个”不能逐字段定标，待确认 | 封单量、实际流通、最大封单不同；不能一律标元/股。最大封单也不同于收盘封单 |
| `limit_list_ths` | `sum_float` | 亿元；官方明确 doc355 | 尚未适配；若纳入元契约须独立×100000000 |
| `limit_list_ths` | `turnover`、`limit_up_suc_rate`、`rise_rate` | 官网未完整注明缩放或计算口径，待确认 | 不作为已认证标准字段 |

`fd_amount` 证据：2026-10-23 U的82条金额逐项核对；D的6条金额与保留的Raw核对，
另有同花顺金额展示。华数传媒39029562元、新亚制程190983232元，均不乘除100或10000。
完整批次、Raw hash及 UI 核验依据见[来源目录](source-quantity-units.md)、
[涨停恢复记录](runbooks/limit-up-recovery-2026-10-24.md)和
[跌停恢复记录](runbooks/limit-down-tushare-2026-10-24.md)。
这是2026-10-24确认的字段契约，其他日期历史批次仍需来源与版本认证；没有把金额吻合当成通用单位猜测器。

## 可转债：独立张数契约

| 接口 | 字段 | 来源单位 / 证据 | 标准处理 |
|---|---|---|---|
| `cb_daily` | `vol` | 手；[doc187](https://tushare.pro/document/2?doc_id=187)；项目可转债契约1手=10张 | ×10 → 整数张，**不能乘100** |
| `cb_daily` | `amount` | 万元；官方明确 doc187 | ×10000 → 元，不能使用股票日线千元规则 |
| `cb_daily` | `pct_chg` | %；doc187 | 百分比数值原值 |
| `cb_basic` | `issue_size` | 元；官方明确 [doc185](https://tushare.pro/document/2?doc_id=185) | 元原值，不是亿元 |
| `cb_basic` | `remain_size` | 元；官方明确 doc185 | 当前 `CB_BASIC_FIELDS` 不请求该字段，此证明不适用于 `cb_daily` 同名字段 |
| `cb_daily` | `remain_size` | 未确认；doc187无此字段定义 | 当前请求未包含；旧Raw或额外响应若出现非空值（含0），仍阻断标准化/replay；空值保留null |
| `cb_basic` | `maturity` | 年；doc185 | 期限，不是交易日数 |

当前适配器请求的 `convert_value`、`convert_pct`、`convert_price` 及部分 `cb_basic`
字段名与本次官网的 `cb_value`、`cb_over_rate`、`conv_price` 等字段名不一致。
本次仅记录差异，不声明字段互为别名或已取得真实响应单位；需单独核对接口版本、响应及映射。
旧转债金额千元×1000实现已纠正，**不代表历史金额已全部修复**。

## 龙虎榜与技术因子

| 接口 | 字段 | 单位 / 证据状态 | 项目处理与剩余工作 |
|---|---|---|---|
| `top_inst` | `buy`、`sell`、`net_buy` | 元；官方明确 [doc107](https://tushare.pro/document/2?doc_id=107) | 原值；净额可负 |
| `top_inst` | `buy_rate`、`sell_rate` | 官网写占总成交比例，未明示缩放 | 项目按百分比原值处理；仍需补独立比例尺度证据 |
| `top_list` | `amount`、`l_sell`、`l_buy`、`l_amount`、`net_amount`、`float_values` | 项目使用元；[doc106](https://tushare.pro/document/2?doc_id=106)字段表未列基准单位 | 不冒称官网已认证；待补来源尺度证据，不能从 `top_inst` 推及全部字段 |
| `top_list` | `pct_change`、`turnover_rate`、`net_rate`、`amount_rate` | 项目使用百分比数值；官网未逐项标明% | 原值；比例分母和事件统计窗口须按ADR-0055，不把多日事件金额冒充单日 |
| `hm_detail` | `buy_amount`、`sell_amount`、`net_amount` | 元；官方明确 [doc312](https://tushare.pro/document/2?doc_id=312) | 目录构建按ADR-0055将买卖额四舍五入到100元精度匹配，**不是来源单位为百元**；`net_amount`虽请求，但不进入匹配记录 |
| `stk_factor_pro` | `obv_bfq/qfq/hfq`、`emv_bfq/qfq/hfq`、`maemv_bfq/qfq/hfq` | **来源指标尺度**；[doc328](https://tushare.pro/document/2?doc_id=328)未证明输出量缩放 | 已接受公开目录v3：保留带符号Decimal原值，`source_native / provider_native_factor_scale`；不换算成股、不屏蔽为null、不再整批阻断 |
| `stk_factor_pro` | `downdays`、`updays`、`lowdays`、`topdays` | 来源指标天数/周期计数 | 按固定目录映射，不能作为成交量 |
| `stk_factor_pro` | 附带的 `vol`、`amount`、股本、市值及行情字段 | 技术因子领域外的来源字段 | 当前仅保留Raw，不进入技术因子事实；不能直接作为标准行情字段提供 |

其他技术因子按固定字段/参数/复权口径目录处理；本表不将所有因子笼统标为“无单位”。
来源指标尺度不是单位认证失败：它是独立、已接受的公开指标契约，不能据此放宽普通股票数量门禁。
详见[技术因子运行手册](runbooks/stock-technical-factor.md)与 ADR-0068 的来源尺度澄清。
`hm_list` 只提供名录文本，不含成交数量或金额；名录构建不代表自动批准来源主体映射。

## 示例与后续核验

- 股票 `daily.vol=1` 手 → 标准成交量100股；标准成交量100就是100股，不是100手。
- `daily.amount=1` 千元 → 1000元；`cb_daily.amount=1` 万元 → 10000元。
- `daily_basic.free_share=1` 万股 → 10000股；`turnover_rate_f=1` → 1%。
- 可转债 `cb_daily.vol=0.1` 手 → 1张；股票 `daily.vol=0.37` 手 → 37股。

待补证据：龙虎榜未标尺度字段、扣非利润来源货币尺度、THS混合说明字段、转债同名/异名字段。
待运行时适配：独立指数聚合量契约。每项补证时记录接口参数/版本、原始响应血缘、
独立单位依据、标准字段映射与测试；历史修复按精确批次预览和审计，不按日期统一乘100。
本次仅维护文档，没有修改采集代码、历史数据、快照或生产配置。
