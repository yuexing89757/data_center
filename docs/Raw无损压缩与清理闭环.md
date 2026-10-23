# Raw 无损压缩与质检清理闭环

2026-10-23 本地实施记录，关联 Issue #84、Accepted ADR-0058 及已批准的存储生命周期设计。
生产迁移、部署与首次执行的结果应另记运行手册；本文件不表示生产已压缩或整个 ADR 已完成。

## 清理工作流

沿用 Worker APScheduler 的 `data-cleanup-daily`，上海时间 03:00 启动，共用
`maintenance:data_cleanup` 锁。步骤依次为竞价快照清理、`compress_raw`、质检归档。
在线三个已完成交易日、历史六个自然月、质检白名单及30天期限均不改变。

质检候选先用现有部分索引分页取最多200个 ingestion/rule 键，再统计这些组。
扫描游标与可归档组分别返回：本页全是近期或未终态组，也必须推进到下一页。
达到100,000原始行预算或下一完整组超出剩余预算后停止，已提交工作保留，记录 partial；
超过单元上限的组保留并记录错误，其他独立组可继续。收尾不再启动全表精确计数。

预览的 `counts_complete=false` 表示只检查一页，`groups`、`rows`、`objects`、
`original_bytes` 是该页合格候选的下界，不能当成全库总量，零也不表示没有后续候选。
执行报告的剩余候选不完整时明确标记，不能据此宣称全量处理完毕。

## Raw 契约与操作顺序

只处理已登记的本地 JSONL；Manifest 创建时间及采集完成时间都必须严格早于
执行日上海零点减7个自然日，采集状态为 succeeded/partial/failed。未登记文件、
Parquet、运行中或时间不符的对象不转换。优先补完已登记而未确认明文移除的对象，
之后优先大文件，同大小按创建时间、Raw ID 排序。

原 Manifest、原始字节、SHA-256、字节数、行数、Schema版本和血缘均不修改，
不创建第二份 Manifest，不重新编码 JSON 或数值，不修改单位认证绑定的逻辑 Raw 哈希。

1. 在维护报告登记本次 UUID 临时文件归属。验证路径及文件类型，拒绝越界、软链接、junction。
   Raw 每页最多200项一次登记，避免10,000次逐文件扩写报告；登记不表示文件已经创建。
2. 流式核对原始哈希、字节数、行数，写独占临时 gzip，level=6、mtime=0，无原始文件名头。
3. fsync 文件，完整有界解压回读并再次核对原 Manifest，以同目录硬链接无覆盖原子发布 `.gz`，
   fsync 目录（Windows 不支持目录 fsync）。临时文件仅由创建它的本次操作清理。
4. 数据库事务锁父采集记录、重读 Manifest 和资格，幂等登记 `ingestion.raw_compression`。
   事务提交前不移除明文；Worker 无 RawManifest UPDATE/DELETE 权限。
5. 再核对 gzip 与已登记的压缩哈希及大小，验证明文内容和文件身份后移除明文，fsync 目录，
   最后确认 `plaintext_removed_at`。压缩元数据不可更新，Worker 仅能更新确认列。
   明文完整扫描后、移除前再次核对 gzip 的已验证文件身份；期间变化则保留明文。

发布后登记失败保留明文与 gzip，下次完整校验后复用；登记后移除失败保留登记并重试；
移除后确认失败仍可用旧 Manifest 读取 gzip，重跑完成确认。损坏或冲突表示不覆盖、不猜测修复。
权限、文件错误只记录安全错误类别，数据库异常停止后续写入，独立完成事实不抹去。

读取优先原路径，只有 FileNotFound 才尝试原路径加 `.gz`；明文损坏或权限错误不降级。
解压受 Manifest 字节数+1约束，之后照常校验哈希和行数。重放、数量修复、来源比对及龙虎榜
恢复继续使用同一兼容 reader；逻辑路径已有 gzip 时写入器拒绝重复写入。

## 预算、预览与恢复

总预算最多30分钟，Raw 子步骤最多15分钟，单次最多10,000对象，每页200，单对象256 MiB。
SQL lock_timeout=2s、statement_timeout=30s。自动启动窗口 [03:00,04:00)，
手工避开 [09:10,09:40)，窗口和锁丢失会停止新操作并报告部分完成。
预算为协作式检查，不保证中断底层阻塞系统调用。

工作单元写入前至少保留512 MiB且满足两倍原文加1 MiB。80%告警，90%或不足2 GiB严重告警。
报告分开记录原始字节、gzip字节、明文已移除字节、完成数与错误；这些逻辑字节不等于文件系统
实际可用空间增量。重复运行不重复累计上次运行的删除量。

```text
uv run market-data-center data-cleanup
# 仅在授权的生产执行范围内使用双标志：
uv run market-data-center data-cleanup --execute --confirm
```

默认预览只读，不创建根目录、报告、锁或临时文件。生产先通过受保护流程应用
`20261023000500_raw_compression_registry.sql`，核验服务目标与磁盘、独立 Raw 备份及差异覆盖后，
再部署兼容 Worker 并检查预览；迁移本身不删除 Raw。

备份需同时覆盖逻辑 Raw 对应表示、质量归档及其登记表。数据库恢复清单已包含
`raw_compression`，备份角色仅获 SELECT。恢复后用原 Manifest 检查 gzip，不能只恢复数据库
而宣称恢复完成。回退版本必须具有 gzip reader，禁止将已压缩数据交给只识别明文的旧版本。
需要展开时应在受控恢复流程核对原 Manifest，不能覆盖现有文件或批量猜测删除表示。

此范围不新增其他表清理，不运行 VACUUM FULL，不改变新采集质量记录的构造，
不实现通用陈旧临时文件或旧发布物自动删除，也不触发消息通知。

## 授权离线退役与回放

ADR-0069另行规定2026-10-23所有者授权的一次性旧数据清理，不替代上述定时保留规则。
`ingestion.raw_retirement`记录已验证机外归档和线上移除确认；原Manifest保持不变。
Worker只读此表，压缩候选排除未恢复退役项；回放读取显式报告离线，数据库触发器与管理员
登记共用逐Raw事务锁，避免资格读取与回放创建之间的竞态。恢复后必须验证原内容，再由管理员
确认restored_at。数据库备份和恢复清单包含该登记表。操作工具默认预览，首次移除前必须有
匹配归档证据、正确存储根身份和在线表示验证；缺失/未认证的归档不猜测补齐。
具体执行范围、恢复依据及保留例外见[月前历史清理记录](runbooks/month-retention-production-2026-10-23.md)。


## ADR-0070：新增历史结果清理（2026-10-23）

所有者要求把建议表加入任务。新步骤 `archive_history_results` 排在既有三个步骤之后；
新范围按上海执行日减一个自然月、严格小于截止日，以下表共 32 张。
原竞价序列历史仍保留六个月，不以一个月替换；质量规则白名单及Raw退役范围不变。

| 原子单元根表 | 同事务归档删除的子表 |
|---|---|
| `realtime.call_auction_indicative_snapshot` | `realtime.call_auction_indicative_detail` |
| `realtime.call_auction_market_snapshot` | 无 |
| `realtime.call_auction_snapshot` | 无 |
| `realtime.eod_quote_snapshot` | 无 |
| `realtime.auction_collection_session` | `derived.auction_quote_metric`、`realtime.five_level_quote_snapshot`、`realtime.auction_collection_round` |
| `realtime.call_auction_market_series_session` | `realtime.call_auction_market_series_round` |
| `core.stock_technical_factor_snapshot` | 无 |
| `core.board_moneyflow` | 无 |
| `billboard.dragon_tiger_event` | `billboard.seat_trade` |
| `billboard.trading_seat_profile_daily` | 无 |
| `regulation.status` | `regulation.warning`、`regulation.rule_result` |
| `stock_pool.snapshot` | `stock_pool.member` |
| `stock_pool.calculation_quality` | 无 |
| `today_limit_up.snapshot` | `today_limit_up.member`、`today_limit_up.calculation_quality` |
| `today_limit_down.snapshot` | `today_limit_down.member`、`today_limit_down.calculation_quality` |
| `today_limit_up.source_observation` | 无 |
| `today_limit_down.source_observation` | 无 |
| `derived.close_price_new_high_120d_snapshot` | `derived.close_price_new_high_120d_member` |

股票池仍被任何竞价会话/报价引用时保留全部成员；共享质量记录只有所有引用快照已清除且
自身超过一个月才归档。竞价会话必须结束；序列会话仍有在线或六个月历史引用时保留。
监管结果的计算ID若仍在monitor_context链中则保留。日K、每日指标、涨跌停基础事实、
股东比较输入、全部基础资料/采集计算血缘/单位修复证据不加入此次日期删除。

每个依赖组先锁父行、读完整子事实，将原始PostgreSQL JSON文本连同结构、主键、数据库
身份、工作流和截止日写入`RAW_DATA_ROOT/_history_archive/<UUID>.jsonl.gz`。
归档经fsync和重新解压SHA校验后，在同一事务登记`audit.history_archive`并精确删除。
数据库仅暴露锁定和完整组删除函数，Worker不能直接调用逐子表删除助手。
数据库再次检查完整子集合、父项存在及所有外部FK，不依靠CASCADE删除未归档事实。

新步骤最多10分钟/100000行；单组10000行/32MiB。超过组上限保持原组不变并记录partial，
按主键游标继续后续组，不让一个超大会话挡住全表。不同表组轮转，共用原30分钟预算和
maintenance:data_cleanup锁；Raw最多15分钟，质检最多5分钟，为历史步骤预留10分钟。
若前置快照步骤占用预算，后续步骤按剩余时间提前结束。错误报告只含安全错误码。
已提交进度保留；下次自动重新扫描剩余数据。没有成功登记的归档文件可能是失败事务残留，
不得当成已清理记录，不自动删除这些恢复证据。

`market-data-center data-cleanup`默认预览，新增每组最多201个候选根的只读统计，
`counts_complete=false`表示下界；不是全库精确计数。实际执行仍需`--execute --confirm`。
线上归档目录必须可写、容量满足保护门槛，并纳入现有独立备份；部署检查记录磁盘位置。
本地实现不代表线上已迁移/启用，以发布运行记录为准。

恢复工具：管理员通过标准PG环境变量连接精确目标，运行
`uv run python scripts/restore_history_archive.py --archive-id <UUID> --root <Raw根>`。
默认只验证；加`--execute --confirm`才恢复。恢复会校验登记的压缩/内容SHA、数据库实例、
表结构、主键及冲突，按父到子顺序在单事务还原；已相同行幂等，不同内容拒绝。
执行持有同一个维护锁，保存命令输出作为运维回执；不修改原归档和Raw。
恢复工具只恢复数据库归档行，不自动恢复更早被退役的Raw或变更数量认证。

独立审查发现并修复：超大第一组导致整表饥饿；Worker可单独删除子表绕过原子组。
验收须包含这两个回归、数值/null、日期边界、损坏归档拒绝、回滚、重复执行、权限和
实际完整schema兼容。数据库删除是页面复用，不承诺数据文件立即缩小。
