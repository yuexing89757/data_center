# 2026-10-23竞价数量与金额恢复

关联Issue #92、Accepted ADR-0068、ADR-0052。所有者要求补今天竞价，优先09:25:20。

## 只读检查与实施范围

今天会话`58ed48e0-a4e8-43cc-b4ee-72b956a39950`共32轮，每轮两个partial尝试，
每尝试Raw5225行、标准事实5223行；选中批次合计167136条。64个尝试均缺少单位认证。
09:25:20选中尝试`24bc6b9f-df3c-435c-b1b2-780af9f11d38`已存在，
另一个尝试为`921e2bc2-796b-4a61-a6f8-a7b0f8b774a9`。
实际FastAPI查询600000/092520返回200，但成交量和金额均为null，
明确原因`quantity_units_unverified`；不是整轮未采集或金额未落库。

Raw v2保存原生`pytdx_hq.security_quotes.v1`包络，数量为手；v1才是股的标准归档。
修正原目录将v2错误描述为标准股的条目，v1不变。当前适配器在Provider层精确转股。
认证只准入完整批次中已经正确的原事实，不重新乘100，不启用通用竞价Raw回放。
全部原事实字段须与Raw重建结果一致；Decimal按既有PostgreSQL numeric(30,4)
存储规则对照，不修改金额精度和交易语义。

所有已检查尝试均缺SZSE:000016原始记录；SSE:600839有两条重复原始记录，
原采集按重复证券拒绝该股。保留两个缺口及partial状态，不拼接尝试、择一去重或冒填其他时点。
09:25:20的`cumulative_amount`是来源开盘成交累计金额，单位元；
09:25前的指示金额沿用既有买一价乘买一数量语义。两者不互相替代。

## 工具与安全边界

默认预览（环境变量提供数据库与Raw根目录，路径仅用于私有计划，不提交计划或Raw）：

```powershell
uv run python scripts/certify_auction_series.py --ingestion-id <精确UUID> --plan <新私有计划路径>
uv run python scripts/certify_auction_series.py --ingestion-id <同一UUID> --plan <同一计划路径> --apply
```

每计划只含一个已终结尝试，最多6000 Raw行。哈希绑定目标数据库、原会话、冻结证券集、
轮次、endpoint、采集时刻、Raw字节/行数/SHA256、全部字段投影及热/历史副本。
执行重新读取Raw并复核完整事实，行锁保护原血缘和事实；25秒单SQL及3秒锁等待保护保留。
只有完全一致时，在同一事务中写入认证和`audit.quantity_unit_repair_run`计划审计。
不使用仅允许日Kvolume的`quantity_unit_repair_item`表，不写事实或原Raw。
多个计划逐个提交，失败批次不认证；按同一封存计划重试即为断点续跑，已完成返回already_applied。

Worker对不可变表没有UPDATE权限，因而不能取得FOR SHARE行锁。生产apply使用已授权的
维护管理员连接；Worker角色的权限不扩大。预览仍可使用只读权限。
回退仅针对本工具新建认证：核对计划数据库身份、精确ingestion、字段组、evidence_hash和
normalizer版本后，在维护事务中将认证改为quarantined并将对应计划标记rolled_back。
不得删除事实、改Raw或覆盖别的认证；认证后产生的下游不可变版本须另核查，不能盲目解除隔离。

## 验证与执行结果

聚焦74项通过（含隔离PostgreSQL两项），覆盖接口数量/金额准入、热/历史一致、
幂等、Worker权限拒绝、计划篡改、Raw损坏和历史副本变更。完整本地门禁：
ruff format/check通过，mypy175源文件通过；隔离PostgreSQL17运行全套
`uv run pytest -q -p no:cacheprovider --tb=short --show-capture=no`，
2114通过、3项Windows软链接权限跳过，497.88秒。独立审查及补充审查无Critical/Important。

09:25:20两个尝试已分别完成5223行认证。真实FastAPI序列及末轮快照接口各18个有界请求，
分别覆盖选中尝试全部5223条，对照成交量、金额和十档盘口数量一致，缺失代码均仅000016/600839。
查询600000例：640900股、6210738元，状态certified；100股就是100股，不再额外乘除100。
末轮原事实/Raw/选中尝试未变。其余62个尝试已完成只读预览，正在逐批执行，完成结果随后补记。

64个封存计划已有本地独立副本；64个Raw内容哈希全部命中既有独立离线归档索引，
合计348664726字节。再次核对归档文件SHA256为
`e37ebea5239d6191f0151033d83b53c55f2765183b1bca1516dcd3a4f615af88`。
本轮无Raw修改、删除或重新采集，也没有触发下游群消息。
本记录不表示整个数据中心或最近一个月全部竞价已经恢复。
