# 2026-10-23 月前历史清理

关联 Issue #84、ADR-0069。所有者要求现在清理一个月以前的所有历史数据，优先保证集合竞价
和程序流畅；随后选择旧Raw先进入已验证独立离线归档，再删除线上副本。

边界：业务日期严格早于2026-09-23；时间戳使用上海时间该日零点。保留最近一个月、有效
证券/日历等基础资料、血缘及近期计算依赖。日K额外保留截止日前120个交易日窗口。
这是一次性授权，不改变03:00任务的既有规则，也不放宽数量单位认证。

## 工具与验证

`scripts/retire_history_before.py`默认preview；管理员手工执行archive/verify/apply/restore。
完整JSON行保存PostgreSQL原始数值文本，不经过float。每批1000条，固定表/谓词/实例/结构、
主键及全行一致性校验，拒绝隐式级联。归档持有验证过的文件句柄，并逐批复核原验证流的哈希，
避免路径替换和中途内容变更。恢复对相同已恢复行幂等，冲突则回滚当前批。
与Worker清理使用同一maintenance:data_cleanup锁，25秒语句/1秒锁超时；不TRUNCATE或VACUUM FULL。

独立只读审查的四项问题（归档替换、恢复续跑、实例绑定、分区删除谓词）已修正。
隔离PostgreSQL17聚焦7项通过，涵盖分区、跨批中断、日期边界、精度、幂等、漂移、损坏、
级联拒绝、实例与归档身份。完整本地2077通过、3项Windows软链接权限跳过；ruff/mypy通过。
后续删除请求仅发送主键，仍逐行比较完整原始行，减少大技术因子JSON反复入参解析；聚焦7项再次通过。

## 归档与恢复介质

第一阶段34表完整归档10,425,439行，所有内容/压缩SHA、行数及Manifest校验完毕。
数据库实际位于数据盘；封存归档在独立根盘，已核实设备不同并sync。
归档和原始行仅位于受限运维目录，不入Git。执行目录与封存副本分开，恢复需使用对应Manifest摘要，
按依赖反序还原；不向不同数据库实例自动恢复。

机外传输先后出现连接超时，未完成文件保留为partial，不能作为校验通过的机外副本。
本次已验证恢复依据是同主机不同磁盘的封存归档，不冒称已完成所有机外副本。
旧Raw既有独立机外备份另有逐文件校验证据，是否覆盖退役候选须再次逐项验证。

## 第一阶段早期检查点（后续结果见末尾）

- 竞价历史序列：删除1,675,232条，截止日前剩余0。
- 保留竞价历史序列2,661,571条，行数及全行64位诊断校验和之和不变；该校验和不冒称密码学证明。
- 实际首批1000条只读执行计划52.512ms，读取使用旧分区索引，未扫描近期分区。
- Worker/API持续active；其余33表开始分批执行，结果仍待最终汇总。
- 全部旧业务表尚未清完；股票池/120日新高等后续依赖组、Raw退役登记/移除及机外副本尚未完成。
- 本次清理不等同于历史竞价单位认证恢复；该恢复及全部端到端单位验收仍未完成。

数据库逻辑删除释放可复用页面，不承诺立即缩小数据文件。近期竞价内容、服务状态、实际删除量、
剩余保护范围和失败项在后续实际检查后补充，不能把当前检查点当作全量完成。

## 后续保护与 Raw 离线流程

清理复核发现股票池有效日与基础日可能跨越截止日。已从第一阶段归档恢复三张输入表：
`derived.daily_price_limit` 111,434行、`derived.price_limit_event` 2,514行、
`core.stock_daily_indicator` 5,550行；逐行比对归档完成，近期池成员必要输入缺失数为0。
新的清理谓词保护所有现存池成员的基础日输入；旧池若仍被任何竞价会话/报价引用，
其成员和计算质量记录一并保留。质量记录共享计算ID时，只要还有受保护池就不删除。
后续按先会话、再池、最后重新预览输入的顺序执行，不沿用过时的日期预览。
第一阶段完整执行10,425,439行、无失败表，但上述119,498行已恢复；净清理量须最终汇总。

旧竞价轮次的外键检查缺少(session_id,sample_seq)索引，触发25秒保护超时，事务回滚。
迁移20261023000700为两张引用表添加匹配索引，经受保护迁移流程部署后再续跑。
该检查不通过前不跳过外键、不强制级联，不把旧轮次作为已清理。

Raw退役使用迁移20261023000600的管理员登记表，保留原始Manifest和回放血缘。
Worker只能读取登记；压缩候选排除未恢复的离线对象，回放读取先给出明确离线原因。
数据库回放INSERT触发器和管理员登记共用逐Raw事务锁，拒绝在退役后插入新回放引用；
登记不锁整个ingestion_run表，避免与回放触发器形成反向锁等待。

`scripts/retire_raw_before.py`默认preview。必须提供已验证的独立归档逐文件索引、校验收据、
计划输出路径和实际Raw根目录。计划绑定数据库实例、截止日、归档身份、Raw根的规范路径摘要
及设备/inode；不会只凭目录存在认作正确存储。逐项比较原始内容SHA/字节数/行数，
保留当前事实UUID引用、已知JSON血缘引用和所有曾回放的原始Raw。
无归档证据或首次在线副本已缺失者分别列出，不自动登记成功。

apply必须提供独立保存并验证的计划SHA。每批先验证在线表示，短事务重新核对引用并登记，
随后再次校验文件身份、精确删除并同步目录，最后确认online_removed_at。
登记后中断保留待完成状态；同一计划可续跑，已删除表示只计0新增释放字节。
任何恢复过的登记不能由旧计划再次退役。所有文件路径均经过现有Raw Store边界校验。
本工具是一次性管理员操作，不属于Worker任务，不增加操作系统定时任务。

离线恢复应按登记的archive_ref和索引摘要找到独立归档，提取原始`raw/<object_path>`到受限
临时目录；先校验原始SHA/字节数/行数，再在正确Raw根原路径原子发布并同步目录。
不得覆盖不同内容，不修改不可变Manifest或伪造gzip表示。确认读取完全一致后，管理员才可
设置restored_at；若仍有历史压缩登记，应同时核对表示状态再恢复压缩任务。

ADR-0072定时退役启用后，上述任何恢复文件操作之前，管理员须先取得同一逐Raw事务
advisory lock和登记行锁，持有到文件发布、验证及restored_at提交；不得先写回文件再等待更新。

原始独立Raw归档SHA为`e37ebea5239d6191f0151033d83b53c55f2765183b1bca1516dcd3a4f615af88`；
逐文件v2索引SHA为`2e4c0f325352809765acb469f4559e8a0c3dbc4cb16966f38504e7ca261c637d`。
该索引重新读取原始归档计算，覆盖312,596个文件、16,107,740,616个原始字节。
此处记的是归档覆盖数，不是本次删除数；实际退役量以后续执行审计为准。


## 数据库完成审计与发布验证

运行提交`d268154558d1cd76cafccb1cff9dd35dc898e204`。
CI运行39938733397成功；受保护生产迁移运行39939392812成功，应用00600和00700后切换Worker/API。
原服务配置和环境文件摘要不变；启动期间首次HTTP检查发生连接拒绝，未计为成功，重试后
healthz/readyz均200，竞价查询实际返回1轮1条记录。后续清理期间该有界查询约0.03秒。

最新完整本地门禁：ruff format/check通过，mypy 170源文件通过，pytest 2088通过、3项因
Windows软链接权限跳过，耗时653.01秒。此前测试期间源代码更新导致旧调用签名的一次失败，
未当作通过；上述结果来自冻结提交后的重新完整执行。新增测试覆盖目录身份错误、归档证据、
首次缺失拒绝、引用变化、并发回放等待、幂等续跑、库存池依赖及备份/权限。

两阶段净删除10,889,357行（10,425,439 - 119,498恢复 + 583,416），48个目标表的
静态合格谓词剩余均0。第二阶段旧竞价轮次862行经新索引14.29秒清理完成。
保留的近期竞价历史2,661,571行，清理前后行数与诊断校验和一致；近期股票池必要输入缺失0。
以下为逻辑净删除量，不能视作文件系统释放量。

| 表 | 净删除行数 | 本次合格剩余 |
|---|---:|---:|
| `audit.quality_result` | 3,527,110 | 0 |
| `billboard.dragon_tiger_event` | 79,567 | 0 |
| `billboard.seat_trade` | 777,293 | 0 |
| `billboard.trading_seat_profile_daily` | 28,393 | 0 |
| `convertible_bond.daily_bar` | 0 | 0 |
| `core.board_index_constituent_snapshot` | 0 | 0 |
| `core.board_index_daily_bar` | 56 | 0 |
| `core.board_moneyflow` | 58,619 | 0 |
| `core.daily_bar` | 1,530,173 | 0 |
| `core.shareholder_count` | 437,116 | 0 |
| `core.stock_daily_indicator` | 5,486 | 0 |
| `core.stock_technical_factor_snapshot` | 968,430 | 0 |
| `core.top10_float_holder` | 10 | 0 |
| `derived.adjusted_daily_bar` | 0 | 0 |
| `derived.auction_quote_metric` | 9,400 | 0 |
| `derived.close_price_new_high_120d_member` | 1,120 | 0 |
| `derived.close_price_new_high_120d_snapshot` | 27 | 0 |
| `derived.daily_metric` | 0 | 0 |
| `derived.daily_price_limit` | 111,370 | 0 |
| `derived.market_capitalization` | 0 | 0 |
| `derived.price_limit_event` | 2,450 | 0 |
| `metrics.classification_daily_metric` | 0 | 0 |
| `realtime.auction_collection_round` | 163 | 0 |
| `realtime.auction_collection_session` | 4 | 0 |
| `realtime.call_auction_indicative_detail` | 172,170 | 0 |
| `realtime.call_auction_indicative_snapshot` | 5,477 | 0 |
| `realtime.call_auction_market_series_round` | 862 | 0 |
| `realtime.call_auction_market_series_session` | 27 | 0 |
| `realtime.call_auction_market_series_snapshot` | 0 | 0 |
| `realtime.call_auction_market_series_snapshot_history` | 1,675,232 | 0 |
| `realtime.call_auction_market_snapshot` | 98,828 | 0 |
| `realtime.call_auction_snapshot` | 0 | 0 |
| `realtime.eod_quote_snapshot` | 1,677 | 0 |
| `realtime.five_level_quote_snapshot` | 9,400 | 0 |
| `regulation.rule_result` | 720,001 | 0 |
| `regulation.status` | 82,811 | 0 |
| `regulation.warning` | 576,014 | 0 |
| `stock_pool.calculation_quality` | 3,523 | 0 |
| `stock_pool.member` | 2,336 | 0 |
| `stock_pool.snapshot` | 66 | 0 |
| `today_limit_down.calculation_quality` | 11 | 0 |
| `today_limit_down.member` | 9 | 0 |
| `today_limit_down.snapshot` | 4 | 0 |
| `today_limit_down.source_observation` | 5 | 0 |
| `today_limit_up.calculation_quality` | 282 | 0 |
| `today_limit_up.member` | 1,860 | 0 |
| `today_limit_up.snapshot` | 35 | 0 |
| `today_limit_up.source_observation` | 1,940 | 0 |


保留例外：

- `core.daily_bar`保留2026-04-01至2026-09-22的623,224行，作为截止日前120个交易日回看输入。
- 三张股票池输入表各保留64行2026-09-23之前的必要记录；有效日跨截止日不能机械按基础日删除。
- 股东人数保留截止日前两期及各期修订；前十大流通股东保留最近适用期间及修订。
- 证券/名称沿革、统一交易日历、板块与席位/原因字典、仍适用的监管规则/事件/监控链、财务
  比较资料保留；这些不等同于可按创建日删除的行情明细。
- 采集/计算/修复/质量归档/操作血缘保留；RawManifest与历史压缩记录原样保留。

8张主要大表完成普通VACUUM/ANALYZE（禁止页截断锁，`truncate false`）。质检首次受120秒
限制停止，调整限速后12.43秒补跑成功；其余7表完成。数据库页面可复用，不做VACUUM FULL，
不宣称数据库文件已同比缩小。

数据库归档机外整文件、流式和小块复制均出现连接超时；本机partial/探测文件不能作为有效
数据库备份。完整且已校验的数据库恢复依据仍是与数据库不同设备的根盘封存副本。
第一阶段归档需使用其对应的`bdc5b71`版本工具（冻结谓词不同）；第二阶段使用`d268154`工具。
旧归档恢复前核对Manifest摘要、数据库实例及依赖顺序，不能为通过检查改写归档谓词或摘要。

Raw最终只读计划：20,171个对象，逻辑5,539,505,472字节；155,882个旧对象受引用保护，
7,828个无匹配归档证据保留，已匹配但在线副本缺失数0。
计划SHA为`d4446d37a5e6ef2d655334f24995da36d7c084554a7b91ff4683d4e635821a1d`，
已另存本机独立介质并解压复核。实际文件移除、登记一致性及空间结果在收尾检查后记录。


## Raw最终执行与未完成范围

20,171项全部完成登记及online_removed_at确认，实际移除线上表示332,559,020字节
（332.56 MB；此前已有大量gzip，不能按5.54 GB原始内容计作本次空间释放）。
独立最终只读检查：线上明文/gzip表示剩余0，计划内全部原Manifest字段与计划一致，
重新扫描当前业务引用冲突0，restored_at记录0，Worker退役表写权限仍为false。
用实际Worker只读连接抽查3个回放均在创建Run/读取文件之前报告离线；全部退役项满足
压缩候选条件的数量0。本地任务页正确路径返回200（根路径404不是任务页失败）。
最后healthz/readyz与竞价查询均200；竞价查询返回1轮1条，耗时0.022秒。

最终磁盘可用：根盘10,168,287,232字节，数据盘10,308,046,848字节；二者约10 GB。
Raw最终登记与文件审计、磁盘/数据库审计、回放/压缩门禁、接口检查、完整测试日志及
执行回执分别保存于受限运维目录；Raw计划及异常清单另存独立本机归档并复核SHA。

7,828项无匹配归档证据的旧Raw记录没有进入删除/退役清单；另行核验时其线上表示也均缺失，
不能称这些文件仍被完整保留。本次保留其Manifest和血缘，不伪造成功、不猜测重建。
Manifest创建时间范围为上海时间2026-08-10 21:37:22至2026-09-22 09:33:18。
受限异常文件逐项记录Raw/采集ID、原路径、来源版本、原哈希、字节与行数、归档问题及在线存在性，
压缩文件SHA为`b1c89b38aa75a1500610cadfe840ccfe27b7c07a3453ac91f5ae983dd3f21e10`。

| 未验证来源/采集版本 | 对象数 |
|---|---:|
| eastmoney/call_auction_indicative_detail.v1 | 5,477 |
| pytdx/local_daily_bar.v2 | 1,889 |
| pytdx/remote_daily_bar.v1 | 456 |
| tushare/trading_calendar.v1 | 2 |
| tushare/stock_daily_indicator.v1 | 2 |
| baostock/trading_calendar.v1 | 1 |
| akshare_ths/board_index_daily_bar.live.v1 | 1 |

本次可清理数据库范围和有完整独立证据的Raw清单已完成；不能据此声称所有月前资料均已删除。
保护的滚动窗口、基础/监管/财务及血缘、上述缺失Raw记录，以及未完成的数据库机外归档
均明确保留为例外。定时保留策略仍沿用既有规则，不会自动变为全库只留一个月。
历史竞价单位认证、技术因子未知量纲、问题快照新版本重建及整个数据中心端到端数量单位
验收仍未完成，继续由Issue #92跟踪；本次清理不代表上述修复完成，也未通知下游群。
