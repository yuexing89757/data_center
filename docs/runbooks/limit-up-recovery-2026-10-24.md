# 2026-10-23 涨停列表恢复记录

关联 Issue #92、ADR-0029、ADR-0068。项目所有者于2026-10-24明确要求解决该日期列表为空，
并允许使用 Tushare `limit_list_d`。本次仅恢复该日既有主板涨停契约，不扩大品种或修改规则。

## 写入前检查

- 22:00 工作流及来源批次失败，接口默认/显式version=1均404，该日无today_limit_up快照。
- 来源批次`39a3c676-d528-4a8e-a9fc-7ae5b371be92`已完整保留82条Raw；
  阻断原因是“封板资金”未登记单位，而非上游空榜单。
- 同日规范主板涨停池version=1有66候选；日K收盘价、成交量、流通股数和买一数量均无缺失。
- daily_market、stock_daily_indicator工作流为partial；恢复后仍保留其质量状态，不冒称ready。
- Tushare limit_list_d同日82条封单金额与Raw逐代码全部相等；
  同花顺金额基准、来源映射和人工展示核验详见[来源单位目录](../source-quantity-units.md)。

## 修复及操作范围

将既有AKShare v1封板资金精确身份登记为cny，金额原值不缩放。Tushare只作核验，
不伪装来源或混合批次。未知schema及跌停资金契约仍阻断。

部署前完成普通/隔离数据库测试、受保护production check和独立Raw备份校验。
按现有命令 `market-data-center today-limit-up-snapshot --trade-date 2026-10-23`
补建新不可变快照，记录新来源Raw及ingestion；保留原失败批次和原Raw。
验收逐项对照数据库、受限RPC及HTTP，核对默认/显式版本、分页、华数传媒金额和质量状态。
若上游内容变化则记录实际差异，不直接改旧快照。

本次无schema变更，不创建数据库备份，不发送下游群消息。
以下为实际完成后的结果。

## 测试与发布

修复提交`82221d9`；新增集成用例首次因测试上游批次仍为running而失败，
补齐测试完成状态后提交`e337f5050bda8a88ff5eca885f6d7c779ebbc297`，不放宽生产门禁。
本地format、ruff、mypy通过；普通测试1912 passed，215 skipped（本机无测试数据库）。
[最终CI 39992886058](https://github.com/yuexing89757/data_center/actions/runs/39992886058)
全部通过：普通测试1913 passed、214 skipped；隔离PostgreSQL测试213 passed。
CI的两条警告来自既有pytdx包的`is not ''`，未修改第三方依赖。
测试覆盖真实适配器、零/null/非法金额、大数Decimal精度、Raw重复标准化、
未知版本拒绝、Worker角色写入及快照单位认证。

[受保护生产check 39992970475](https://github.com/yuexing89757/data_center/actions/runs/39992970475)
成功；没有新增migration，未执行apply。源码SHA256
`1217604025d261b5d4574c2bfea00ffa227cf0a0078df4b7885ea8f48d8a28e1`；wheel SHA256
`1ad1ed3ad3882fb4e0635bd9a24ed78aeeeaaddb60477efb7f7a3b1756beff1b`。

发布前完整核对297712个Raw文件、7981303092字节，全部由既有独立备份覆盖，
差异/新增文件0。配置摘要核验一致，无运行中Workflow/Ingestion，且不在临近调度窗口。
上海时间2026-10-24 01:16:41左右，Worker/API由e062f51切到e337f50，旧版本保留。
启动期任务页首次连接尚未就绪，随后复验成功：两服务active、重启计数0，
healthz/readyz及任务页200，23项目录任务、1个调度锁，配置未变。
竞价09:25:20冒烟仍返回600000的640900股、6210738元。

## 恢复结果与端到端验收

写入前用新版本只读标准化原Raw，82条通过；按symbol与规范池核对实际覆盖64/66。
缺少来源的两只为`SSE:600187`（*ST国中）、`SSE:600745`（*ST闻泰）；
同日core日K的is_st均为null，既有主板候选规则未改动，本次不处理该独立数据缺口。

按上述既有CLI明确补建2026-10-23，结果：

| 项目 | 实际结果 |
| --- | --- |
| 新来源ingestion | `7b91bcf9-950f-4f14-8ad2-9232a587d1df`，succeeded，82 fetched/accepted，0 rejected |
| 新快照 | `c9eadc16-4a42-4a6e-a2c0-d29f6bef4ef3`，version=1 |
| 范围/状态 | candidate_count=66，member_count=64，rejected_count=2，partial |
| 快照数量门禁 | certified=true |
| 原/新来源Raw | SHA256相同，均为`60d6b03be73eb5cf8bff9b866ce792aa1c233360d78c4821bff857b66fc48e70` |
| HTTP默认/显式version=1 | 均200、64条、相同快照与成员 |
| 分页 | limit=17，4页拼接与全量有界查询一致 |
| 受限API角色RPC | 64条全部与数据库及HTTP一致 |
| 华数传媒 | 来源封单资金39029562元；收盘买一派生金额39029562元；成交量35306100股 |
| 质量缺口 | missing_source_observation共2只，保持可见 |

逐项对照字段：成交量、自由流通股数、买一至买五股数（7项）；来源封单金额、
收盘买一派生封单金额、成交额、自由流通市值（4项）。所有比较通过。
首次验收SSH连接超时，重连后完整验证成功；没有因连接失败重复执行写入。
对外字段、OpenAPI结构和算法版本未改变，原契约的CNY/股口径恢复可读。
金额为元的定点字符串；调用方不得再乘除100或10000。

本次恢复精确日期接口有值，不宣称全82只、全部历史或全数据中心单位整改完成。
未新增Tushare生产路由、未改变ST规则、未修复跌停资金或其他未认证来源。
生产无PostgREST HTTP网关，本次验证其受限SQL RPC及实际FastAPI入口；未验收不存在的网关。
外部调用方去补偿签收及下一交易日22:00实际自动运行尚未发生。
