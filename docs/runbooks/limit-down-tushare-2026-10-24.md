# 跌停来源切换与2026-10-23恢复

依据Issue #93、ADR-0056/0068，项目所有者明确要求使用limit_list_d。

## 只读发现与实现范围

- 原22:10批次fac67e0c-d47d-481f-aebb-fb7b68ea58a6保留6条Raw，因AKShare封单资金单位门禁失败；旧快照version1 failed/0成员。
- Tushare limit_list_d(trade_date=20261023,limit_type=D)返回6条，fd_amount有值。
- 新亚制程fd_amount=190983232元，MuMu同花顺卖一显示1.91亿元；字段/单位证据见source-quantity-units.md。
- 规范主板跌停池候选8只；来源不统计ST，缺来源成员保持null/partial。
- 全部来源富化使用一个Tushare成功批次，不拼接AKShare或静默回退；旧Raw/旧快照保留。
- 有序迁移20261024000200只扩充来源约束和注释，权限与API结构不变。
- 来源Raw schema独立，真实provider、endpoint、方向、日期、单位及采集版本保留审计；金额证书绑定新Raw哈希。
- 新快照算法today_limit_down_snapshot_v2，输入哈希覆盖provider/schema/算法版本；规范成员、卖一计算规则和22:10调度不变。

## 执行方式

先运行完整本地检查及隔离PostgreSQL集成测试，再通过受保护production工作流应用迁移。
按宪法v6不创建数据库备份；Raw独立副本和校验按既有规范保留。
切换并确认Worker/API健康后，使用既有明确日期命令：

```
market-data-center today-limit-down-snapshot --trade-date 2026-10-23
```

先核对精确范围及依赖；恢复应创建新不可变版本，验证数据库/API角色RPC/HTTP、
金额/股数/时间、默认与显式版本、分页、旧失败快照不变。不能声称ST来源字段已补齐。
下列为实际完成后的执行与验收结果。


## 实际发布与验收

发布提交`8f98d576354ccf8f715cf90de9c31a30627f2e48`。
format、ruff、mypy通过；136项聚焦回归通过。最终提交重新完整本地测试1945 passed、217 skipped，235.80秒；本机无TEST_DATABASE_URL，隔离数据库验证由CI执行。
[最终CI 40048549076](https://github.com/yuexing89757/data_center/actions/runs/40048549076)
全部通过：普通测试1946 passed、216 skipped；隔离PostgreSQL集成测试215 passed。
新集成用例使用真实Worker/API角色验证Tushare金额认证、成员保留、幂等、新旧版本及来源血缘。
两条Python警告来自既有第三方pytdx包。

首次本地构建因Windows临时PE资源拒绝访问失败，使用uv复制链接模式重试成功；
没有改变依赖、锁文件或系统安全设置。源码SHA256
`708a80936c0dee6dc879766516ce62d40e03bff9a45c00f5b170fd5bd69bc592`，wheel SHA256
`7f0b9ee3d174424156dff3efae96e3c926986c6567f09b48402b2e97d228b72e`。

发布前核验301792个Raw文件、7683160669字节；296495个由已验证基线覆盖，
5297个差异/新增对象109871301字节另存独立增量，归档及逐对象SHA256均校验通过。
[受保护生产apply 40048997440](https://github.com/yuexing89757/data_center/actions/runs/40048997440)
成功应用20261024000200并通过schema/smoke检查，按宪法v6未创建数据库备份。
CI日志整包首次下载超时，按job读取日志重试成功，未因此重复执行生产操作。

上海2026-10-24 09:16:43左右，Worker/API从5c18b70切换到8f98d57，旧版本保留。
服务、配置、调度单实例及近期竞价检查通过：active、NRestarts=0、healthz/readyz/任务页200、
23个任务和1个调度锁；600000的09:25:20仍为640900股、6210738元。

既有精确日期CLI正式执行一次，exit=0，得到：

| 项目 | 实际结果 |
| --- | --- |
| 交易日/版本 | 2026-10-23，version=2，today_limit_down_snapshot_v2 |
| 新快照 | `73a3c553-6c79-41ac-9854-b494d067c40c` |
| 状态/范围 | partial；candidate=8，member=8，rejected=0 |
| 新采集批次 | `aa347d7c-3a04-4ebc-a9f0-43c72af90179`，tushare，succeeded，6 fetched/accepted，0 rejected |
| 新Raw | `91a3b8ef-4132-4108-8ffe-d7bd0b143223`，6条1078字节 |
| 新Raw SHA256 | `6a1a6976bd8457c068d29dbd1404da52c72cc9d229eec786578ef40db48edb8d` |
| 金额认证/快照门禁 | certified，绑定新Tushare Raw/schema/字段 |
| HTTP | 默认及显式version=2均200，8条，相同成员与快照 |
| 全量字段对照 | 数据库、受限API角色RPC及HTTP，8×34=272个字段逐项一致 |
| 分页 | limit=3，3页拼接与全量相同 |
| 旧版本 | version=1继续failed/0成员，ID与input/content哈希不变，显式读取可见原失败状态 |
| 旧Raw | 原AKShare Raw读取及哈希校验通过，未改写或追认其单位 |
| 新Raw独立副本 | 另存并校验归档及对象SHA256，复制恢复依据保留 |

六条来源资金均按元原值冻结：001368=5709862、002388=190983232、
002578=15602478、002811=32359677、002913=33028450、600202=38345209。
每项与本次Tushare Raw、数据库及接口一致，不使用limit_amount替代。

缺来源的规范成员为000059（快照历史名华锦股份）、002785（万里石）；
来源封单资金及收盘卖一派生金额均null。尚未核实它们为什么未在来源榜单出现，
不直接归因为ST，不修改成员资格或猜测金额。

涨停接口仍返回64条，技术因子3只样本均为v3/source_native，竞价检查通过。
22:10 Worker任务已注册，下一次为上海2026-10-26 22:10，固定使用Tushare新适配器。
本次补数及端到端验证完成，下一交易日自动采集实际结果尚未发生；
未执行全历史回填、未适配其他未认证领域、未逐一核实外部调用方，未发送下游群消息。
生产没有PostgREST HTTP网关，验收覆盖实际FastAPI入口及受限SQL RPC，未宣称网关HTTP验证。
