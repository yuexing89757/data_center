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
本节是执行方案，实际测试、上线、补数及验收完成后补记结果。
