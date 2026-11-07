# 每日最大一分钟成交量实现与发布记录

Issue #105；Accepted ADR-0077。2026-11-08所有者明确授权正式完成并部署此前约定的最小链路。
当前状态：2026-11-08正式上线，Worker/API代码版本`252ecac784484f14c18a2a1fcaf8011d654d61af`；
受保护迁移、完整检查及生产样本入库/HTTP验收通过。
后续获授权补算2026-11-02至11-06消费项目“今日涨停”精确清单351组，现已351/351完整，见历史补算验收。
消费项目11月2日重算的前一交易日依赖2026-10-30另有明确授权42组，现也已42/42完整。
早期实验脚本不等于正式HTTP能力。

## 数据边界

只维护指定普通股票、指定交易日的原生一分钟量柱最大值，标准整数股。
输入须为完整240个合法标签09:31–11:30、13:01–15:00，首尾原生量柱保留；
不额外加入或剔除竞价量。缺柱、重复、未读完分页或未认证单位时不给部分最大值，真实零保留。
并列峰值取最早时间作为追溯信息。

来源为显式配置的既有pytdx池，category8；不发现或刷新节点，不跨peer拼批。
只有经过[独立单位验证](minute-volume-unit-evidence-2026-11-08.md)的peer/市场准入；
当前为103.251.85.94:7709的SSE/market1与SZSE/market0普通股票，BSE及其他peer保留缺口。
来源、schema、category、peer、market、字段与证据版本在采集、Raw重放及认证时均验证，
完整源身份绑定认证哈希。240根完整不代表跨供应商逐股一致，已知来源差异保留披露。

数据中心不计算1.1倍或绘图标尺，不接收竞价撮合量/未匹配量，不修改选股策略。

## 实现与运行入口

- Provider负责来源解析、精确数量转换及源身份；纯计算器只负责240根完整输入的最大值。
- 服务负责有效普通股宇宙、30分钟总采集预算、不可变Raw、逐股票采集/认证与版本化发布。
- 存储：`derived.stock_max_minute_volume` 与 `derived.stock_max_minute_volume_snapshot`，
  复用 `derived.calculation_run`；输入血缘在 `ingestion` 和 `audit`，不建立分钟Core明细。
- 本指标新Raw首次即写无损gzip表示，逻辑Manifest仍为JSONL，哈希/字节/行数按解压内容校验。
  无现有Raw改写或删除，其他数据集及七天压缩/月度清除保持既有规则。
- 日常使用现有20:30 `stock-daily-indicators-daily` 工作流：日历、最大分钟量、
  Tushare每日指标、满足原前提的Tushare保留清理。两项业务步骤独立，错误不冒充成功。
- 发布不可变，数值相同而来源认证变化时保留新的发布证据；不以局部失败缩小旧有效集合。
  单次请求只读一个发布版本，认证撤销后不再公开其数量。

管理员显式日期入口：

```powershell
uv run market-data-center stock-max-minute-volume-build --trade-date 2026-11-03 --symbols SSE:600327
```

省略symbols表示精确日期有效股票宇宙。命令属于写入操作，不在HTTP请求时运行。
局部样本发布标记requested_symbols，不能冒称全市场完成。

## 对外读取

`POST /api/v1/stock-daily-indicators/minute-volume-peak/query`，使用既有`X-API-Key`。
请求symbols为1–500个标准代码、trade_date为必填准确交易日；去重保序，不接收未来日期。

```json
{"symbols":["SSE:600327"],"trade_date":"2026-11-03"}
```

HTTP只调用 `api_v1.query_stock_max_minute_volume(text[],date)`，最多500证券，
5秒SQL限制；不读取Provider、内部表或Raw，不触发采集或计算。
返回trade_date、status、version、calculation_id、algorithm_version、calculated_at、published_at，
items包含symbol、max_minute_volume_shares、peak_at、expected_count、observed_count、
quality_status及quality_reasons。时间为上海墙钟`YYYY-MM-DD HH:mm:ss`；缺失为null。
无当日发布为not_ready，局部覆盖为partial，不从其他交易日或其他批次补值。

生产大东方2026-11-03实际返回7,260,200股、09:58:00、240/240，详见下方生产验收。

## 发布与回退

数据库只通过受保护GitHub流程执行 `20261108000100_stock_max_minute_volume.sql`。
发行提交须同时到达Gitee origin/master及GitHub master，CI与隔离PostgreSQL验证通过后发布。
常规部署不创建PostgreSQL/Raw备份，不触发全历史回填、策略或群消息。
迁移只增不删，旧Worker/API代码仍兼容；保留现有运行发行目录，发生运行故障切回旧发行，
不删除新Raw、认证、历史发布或生产表。迁移故障由事务回滚，禁止临时ad-hoc DDL。

SSH此前在banner前关闭；所有者要求重试后已恢复，已核对现有Worker/API服务活跃、
实际池仍只有已核验peer。旧Worker发行b8cba2b、旧API发行d10fa45保留作为回退目标。
环境中的Raw/调度池/调度状态及API Raw路径均为绝对持久化路径，本次不移动或重建。
发布前磁盘可用约459MB，依赖文件与现有发行一致（忽略换行差异）；新代码独立发行、
各自只读复用对应现有依赖环境，不修改旧环境或执行额外依赖安装，不复制Raw。

隔离实测：使用一次性PostgreSQL17数据库、合成证券生命周期/日历fixture及实际有界来源，
正式服务落地1个来源认证、1个结果和1个发布；HTTP无key为401，带key为200。
大东方2026-11-03返回7,260,200股、09:58:00、240/240，局部发布状态为partial，
item为complete。一次性数据库及临时Raw已清理；该结果不是生产验收。
审查已修复：握手分片总超时、市场后续单位失败时禁止前面股票认证、零有效重试误标partial、
日期自动转换epoch/时间戳；独立复核未剩P1/P2。

## 2026-11-08生产验收

- 发行代码`252ecac`已到达Gitee与GitHub master；[CI 42398581363](https://github.com/yuexing89757/data_center/actions/runs/42398581363)成功：
  Ruff格式654文件、Ruff检查、mypy195源文件通过；普通测试2359 passed/246 skipped，隔离PostgreSQL 245 passed。
  本机普通测试2358 passed/247 skipped；隔离PostgreSQL 244 passed/1 skipped（本机未安装pg_dump），该用例已在CI通过。
- [受保护生产流程42399228905](https://github.com/yuexing89757/data_center/actions/runs/42399228905)成功，
  完成ordered migration `20261108000100`、schema/RLS/迁移历史检查及数据库/PostgREST smoke。
- Worker发行`/home/releases/market-data-center-252ecac-minute-worker`，PID24444；
  API发行`/home/releases/market-data-center-252ecac-minute-api`，PID24484。
  `/healthz=ok`、`/readyz=ready`、API只读角色/RPC权限检查、HTTP smoke及Worker只读健康检查均通过。
  `/home/project-worker`与`/home/project-api`稳定链接已切换；前述b8cba2b/d10fa45旧发行保留回退。
- 只执行一次`SSE:600327/2026-11-03`明确样本：1个来源run、1个certified数量认证、1条结果、1个发布。
  `calculation_id=8c4ab8ac-d17c-47e5-a9c1-b207c33ee3a2`，version1，算法`stock_max_minute_volume.v1`，
  calculated_at/published_at均为`2026-11-08 02:04:59`上海时间。
  真实HTTP返回7,260,200股、峰值`2026-11-03 09:58:00`、240/240、item complete；
  顶层为partial，因为coverage_scope=requested_symbols，不能视为全市场完成。
- 使用现有API进程配置的既有密钥实测：新路径HTTP200、无密钥401、缺trade_date为422；
  只读查询前后来源run数不变。未变更API密钥或增加查询时采集。
- **首次上线时的日期覆盖缺口：当时仅有2026-11-03上述样本发布，2026-11-06没有任何发布。**
  对`SSE:600327`和`SZSE:002088`查询11月6日均HTTP200、status=not_ready、数值null。
  当时消费端11月9日06:00若读取前一交易日11月6日，该字段会缺失，应按既有null语义处理。
  首次上线未补算11月6日或其他股票/日期；后续所有者另行授权的351组补算已完成，见下一节。
- 未创建PostgreSQL/Raw备份，未复制或删除旧Raw/备份，未触发策略或群消息。
  安全验收摘要在本机产物`max-minute-volume-production-acceptance-20261108.json`，不含凭证或Raw量柱。

## 2026-11-08五交易日指定清单补算验收

[Issue #106](https://github.com/yuexing89757/data_center/issues/106)。所有者先授权11月2日至6日五交易日，
随后明确收窄为消费项目“今日涨停”的准确日期/股票清单，合计351组。
只读导出清单的days规范化SHA256为`fbab63d3c54f1e030c2adf0ad238cec4429fa093a207ecfd0775f91020e495cf`；
未修改消费项目的rows_before或任何业务数据。

2026-11-08 02:22:36上海时间，真实生产HTTP逐日验收结果：

| 交易日 | 发布版本 | 请求 | 完整/认证 | 缺失 |
| --- | --- | --- | --- | --- |
| 2026-11-02 | 2 | 108 | 108 | 0 |
| 2026-11-03 | 2 | 47 | 47 | 0 |
| 2026-11-04 | 1 | 96 | 96 | 0 |
| 2026-11-05 | 1 | 71 | 71 | 0 |
| 2026-11-06 | 1 | 29 | 29 | 0 |
| 合计 | — | 351 | 351 | 0 |

所有item均complete、240/240、整数股，缺失股票清单为空。
顶层status=partial、coverage_scope=requested_symbols，表示只发布指定清单，不能解释为全市场完成；
它不表示这351组中仍有缺失。现有API密钥逐日HTTP200，无密钥401，读取前后采集run数不变。
逐项数量认证、来源run的准确股票/日期、accepted_rows=240、Operations手工步骤终态与发布版本均已核对。
Worker/API健康正常，实际运行代码仍为252ecac，本次无schema或运行代码变更。

关键样本（股；上海时间结束标签）：

| 股票 | 交易日 | 最大一分钟成交量 | 峰值时间 |
| --- | --- | --- | --- |
| 大东方 SSE:600327 | 2026-11-03 | 7,260,200 | 09:58 |
| 大东方 SSE:600327 | 2026-11-06 | 16,218,600 | 09:58 |
| 鲁阳节能 SZSE:002088 | 2026-11-03 | 4,080,800 | 09:40 |
| 鲁阳节能 SZSE:002088 | 2026-11-04 | 8,327,500 | 09:31 |
| 鲁阳节能 SZSE:002088 | 2026-11-05 | 6,172,700 | 09:31 |
| 鲁阳节能 SZSE:002088 | 2026-11-06 | 4,381,600 | 09:31 |

不可变发布审计：11月3日旧version1/单只样本及其7,260,200股结果原样保留，
新version2含47只。11月2日首版107/108，SSE:605287首次minute_capability_unavailable；
仅重试该日原定108只后，version2达到108/108，旧version1保留。
没有跨日期或跨计算批次挑选数值填补当前快照。

范围变更审计：先前全市场尝试尚未发布即停止；仅终结本次3个手工WorkflowRun及其JobExecution，
原因owner_narrowed_backfill_scope。其1,058个Raw对象完整保留，使用正式持久化边界登记为失败/取消来源批次及Manifest，
accepted_rows=0，不发数量认证、不发布标准峰值；未留下不完整Raw对象，未删除任何Raw。
后续仅对351组执行正式服务；来源适配、市场单位门禁、同日锁、完整性、认证和版本保护均未绕过。

本次未扩展其他日期/指标，未执行消费端盘前、选股、竞价或群消息任务，未做备份。
消费端可按原计划只读拉取这些精确日期，不需要因本次中心操作触发其他任务。
本机完整351组HTTP验收产物为`max-minute-volume-backfill-20261102-06-acceptance.json`，
包含各日calculation_id、逐只结果、版本及空缺失清单，不含密钥、数据库URL或Raw量柱。

## 2026-10-30指定42组依赖补算

[Issue #107](https://github.com/yuexing89757/data_center/issues/107)。消费项目11月2日至6日竞价重算中，
11月2日必须使用准确前一交易日2026-10-30。所有者另行授权仅补其42只股票，
范围取只读导出auction-nov2-6-scope.json的oct30_rows.code；未修改消费项目导出或业务数据。
日期与排序代码清单规范化SHA256为`d2e7b579e3c2cde52caf912f9a6b41d10bdfd17c5c1fe4b80d27ea7fc60da3b9`。

2026-11-08 02:46:13上海时间生产验收：请求42、完整42、缺失0、数量认证42，
发布version1，calculation_id=`9767b701-e2fb-4407-ad68-dcf2b59d3743`。
所有item均complete、240/240、整数股，缺失清单为空；顶层partial表示requested_symbols范围。
使用正式pytdx池/适配器、同日锁、Raw/认证/版本发布流程，一次手工步骤完成，
WorkflowRun=`a4c79511-3da4-4ffb-8a48-b2d6b1f92a60`，Operations终态partial准确保留局部范围语义。

既有API密钥真实HTTP200、无密钥401；只读查询不增加采集run。
API健康/就绪正常，其他日期全部发布记录的日期、版本、calculation_id在执行前后完全一致。
因此11月2日至6日既有351组发布不变，没有使用其他日期替代10月30日。
本次没有schema/运行代码变更，代码仍252ecac；未扩展全市场、其他日期或指标，
未计算1.1倍，未触发消费端竞价/盘前/选股或群消息，未创建备份或删除历史数据。
完整42组API验收产物为`max-minute-volume-backfill-20261030-acceptance.json`。
