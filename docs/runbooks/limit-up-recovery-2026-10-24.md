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
执行结果将在实际完成后补记；本文前述方案不代表已部署或已恢复。
