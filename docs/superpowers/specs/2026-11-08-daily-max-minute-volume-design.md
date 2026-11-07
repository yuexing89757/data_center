# 每日最大一分钟成交量：最小设计

状态：ADR-0077已按2026-11-08所有者明确正式交付及部署指令接受；实施中，未据此宣称已上线。Issue #105。

早期本地隔离实验入口为
`scripts/experiments/max_minute_volume_probe.py`；只返回实验原值，标准股数字段保持null，
不加载生产配置、不连接数据库、不注册任务。以下为已接受、待实现验证的正式链路设计。

## 用户决定与目标

只维护 `max_minute_volume_shares`，由指定交易日的合法来源一分钟量柱取最大值。
消费项目次日按前一交易日精确读取，自行绘制竞价比较比例。
所有者已确认的消费端展示尺度见[本地实验说明](../../../scripts/experiments/README.md#消费端绘图口径所有者于2026-11-08确认)。
该尺度只由消费端计算，不能写回最大分钟量M，也不成为数据中心新指标或新计算步骤。
首尾量柱均保留，不额外加竞价量；不开放分钟明细，不新增其他业务指标。
当前已授权有序生产迁移及Worker/API部署；只做明确样本的上线验证，不执行全历史回填。

## 当前链路与最小接入

现有20:30任务调用 `cli.run_stock_daily_indicator_workflow`：同步日历、Tushare
`daily_basic`批次、Core一月保留清理。`core.stock_daily_indicator` 属于来源事实，
ADR-0014禁止把本地 Derived 计算冒充该来源指标。
FastAPI现有 `/api/v1/stock-daily-indicators/latest/query` 按每只股票独立选择最新日期，
不能充当本指标的精确日期读取。

建议在同一20:30工作流的日历之后增加独立 pytdx 输入/计算步骤，随后执行原步骤；
两者保留独立批次、错误与 Operations 结果，不互相阻止执行或撤回成功发布。
一个业务步骤失败、另一个成功时Workflow为partial，全部失败时为failed；
捕获失败后不能直接调用当前succeed()误标success。Tushare保留清理只在其采集前提满足时执行。
仅修改受控工作流步骤目录和已有Worker入口，不增加cron、timer或第二个每日任务。
本次新派生结果保存于 `derived`，不修改Tushare原表的来源归属。

## 来源与数量准入

- 股票宇宙由精确日期的Security生命周期及CN_A_SHARE日历决定；只允许普通股票。
- 池文件只读，候选来自其现存节点。当前经单位证据核实的peer仅为103.251.85.94:7709；
  其他peer不因quote/daily标志或协议连通而获得分钟单位认证。
- 每个市场批次固定一个实际peer；读取后不得将另一节点的部分响应拼入成功批次。
- 分钟协议须独立验证：category=8，沪市market=1、深/北market=0，
  时间日期编码正确，页长不超800，正文完整、无尾部多余字节，目标日标签合法唯一。
- 每页最多800，最多6页，连接/单次读超时3秒，最多2个已具单位证据的池候选；
  每股票总预算30秒、全步骤预算30分钟。到达目标日更早边界才停止；预算或页数耗尽时不能当完整。
- 当前已独立完成同一peer的SSE/market1及SZSE/market0实测，每市场两只普通股票、两个日期。
  BSE及其他peer没有准入证据，保留缺口，不能继承沪深样例的数量认证。
  品种、peer、schema、category、字段的单位证据缺失时保留明确缺口，不能借SSE日 K能力补证。
- 新Raw schema：`pytdx.stock_minute_volume.v1`，只保留时间、原生数量编码及必要源请求信息，
  不收集/公开分钟OHLC。保留不可变Raw、manifest、采集/数量认证引用供复算，执行现有保留规则。
- 新单位身份独立于daily：本peer/category8/普通股票/volume为股，按Decimal精确解码后须整数，
  不乘100、不舍入，不接纳bool/float、负数、非有限或bigint溢出。未知身份失败关闭。
  现有注册键/认证哈希不包含peer/category/market；正式接入必须显式扩展、版本化，
  将这些身份及单位证据版本写入Raw与认证，在新采集及Raw重放路径分别重新校验。

## 计算与质量

纯计算器只接收已准入的单股票/准确交易日/单位证据/原生时间量样本。
算法 `stock_max_minute_volume.v1`：240个合法结束时间标签完整时取最大整数股数；
并列时元数据记录最早峰值标签。真实完整全零输入得到0，其余缺失不补零。
来源的零值量柱保留，缺失标签不推造零；重复键、日期矛盾、非整数或未知单位不给部分最大值。
首根09:31与末根15:00照录；不是声称已从来源量柱中剥离开盘或收盘竞价。

单位与跨源值质量分开：只有单位通过和本来源时段完整才给标准值。
13:26差100是已知跨源差异，不从东财修补该根；它未改变09:58峰值。
元数据声明 `source_reported` 及来源原生时段边界，文档披露该核验差异。
不把“与另一来源逐根相同”设为日常任务隐含门槛，也不在日常任务增加跨源抓取。

缺口原因至少包括 `minute_capability_unavailable`、`quantity_units_unverified`、
`missing_minute_bar`、`source_response_invalid`、`collection_budget_exhausted`、`suspended`。
股数单位硬错误与重复冲突不能降为告警后输出正常值。单股票无效则结果为null并说明原因。
未知单位使对应市场标准入库批次整体失败且不签发认证，不能只把该股票置null而标绿该批。
派生服务可以记录这个市场的批次缺口；首次尝试若没有任何有效值则不发布快照，返回not_ready。
同日网络失败的重试不得撤回此前已有效的结果：只有新有效证券集包含原有效证券集时，
才允许移动该日发布版本；否则保留原发布并记录失败/partial尝试，不混接两批结果。

## 存储与版本

新增ordered migration，复用 `derived.calculation_run`：

- `derived.stock_max_minute_volume`：按calculation_id/symbol/trade_date的不可变结果，
  仅一个业务值max_minute_volume_shares，附peak_at、expected/observed_count、quality_reasons及受限输入引用。
- `derived.stock_max_minute_volume_snapshot`：准确日期/递增version，指向同一完整计算批次，
  保存候选/有效数量和当次证据哈希/水位。同输入及证据幂等，来源修订保留旧版本。

计算器无I/O，服务负责同日锁、输入快照、版本及发布事务；算法/来源schema变化显式版本化。
Worker使用最小权限，结果表不授予UPDATE/DELETE；API不能直读底表。
新表不擅自加入归档清理白名单；批次和Raw只按现有通用规范保留，新增删除需独立设计。

## 准确日期读取

新增 `POST /api/v1/stock-daily-indicators/minute-volume-peak/query`，沿用API-key。
请求 `symbols` 为1–500个标准股票代码，去重保留顺序；`trade_date` 必填ISO日期且不得未来。
新增有界只读 `api_v1.query_stock_max_minute_volume(text[], date)`，5秒statement timeout，
最多500证券；权限遵守API/受认证只读契约，无匿名底表权限。
FastAPI只调用该RPC，不访问Provider、Raw或内部表，不触发计算/采集。

返回请求的准确trade_date、同一发布version/calculation_id/algorithm_version及逐股票：
symbol、max_minute_volume_shares、peak_at、expected_count、observed_count、quality状态/原因。
peak_at用ApiTimestamp上海墙钟格式，并给有效日历示例；不存在结果仍返回原请求日期和明确missing。
没有目标日发布时返回 `not_ready`，不回退更早日期；有partial发布时返回null缺口及理由，
不能从另一个计算批次挑个别股票补满。保留原latest接口语义。
同步三份公开契约及中文说明；必要工具只公开此精确日期摘要，不提供分钟明细读取。

## 实现与验收范围

实现位置：独立minute-volume Provider/精确解码、纯max计算器、派生服务/持久化；
现有cli/scheduling_catalog/Operations接入；新migration、api_v1 RPC、FastAPI模型/查询/路由及契约。
测试必须覆盖：分类/市场协议、未知单位、字节截断、800/6页界限、过期/错误日期、
240/239条、午休、首末根为峰值、零与null、重复冲突、溢出、并列峰值、池不刷新、
中途不切节点、预算耗尽、两步骤独立失败、版本幂等、禁止覆盖旧有效发布、同日单批次读取、
API-key/日期必填/500上限、RLS及API只调用只读RPC。
使用隔离TEST_DATABASE_URL验证迁移、权限、原子回滚和公开契约，不以生产数据作测试fixture。
本设计不承诺本地隔离PostgreSQL当前可用；不能执行时报告准确命令/原因。
