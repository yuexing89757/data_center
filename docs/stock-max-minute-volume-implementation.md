# 每日最大一分钟成交量实现与发布记录

Issue #105；Accepted ADR-0077。2026-11-08所有者明确授权正式完成并部署此前约定的最小链路。
当前状态：2026-11-08正式上线，Worker/API代码版本`252ecac784484f14c18a2a1fcaf8011d654d61af`；
受保护迁移、完整检查及生产样本入库/HTTP验收通过。
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
- **启动日期覆盖缺口：当前仅有2026-11-03上述样本发布，2026-11-06没有任何发布。**
  对`SSE:600327`和`SZSE:002088`查询11月6日均HTTP200、status=not_ready、数值null。
  因此消费端11月9日06:00若读取前一交易日11月6日，该字段仍缺失，应按既有null语义处理。
  本次未补算11月6日或其他股票/日期；后续沿用20:30日常任务，未触发全历史回填。
- 未创建PostgreSQL/Raw备份，未复制或删除旧Raw/备份，未触发策略或群消息。
  安全验收摘要在本机产物`max-minute-volume-production-acceptance-20261108.json`，不含凭证或Raw量柱。
