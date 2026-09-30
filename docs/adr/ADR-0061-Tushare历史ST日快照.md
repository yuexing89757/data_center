# ADR-0061：Tushare 历史 ST 日快照

- 状态：Accepted（项目所有者于 2026-09-30 确认书面设计并要求实施异动监控）
- 日期：2026-09-30
- 关联 Issue：#85
- 补充 ADR-0048、ADR-0060；不改变普通股票日 K 来源。

## 背景

监管异动测算排除 ST/*ST。现有 pytdx 日 K 的 `is_st` 为空，
`core.security_name_history` 在 2026-07-06 起算阶段缺少历史覆盖。
把未知当成非 ST 或用当前名称倒填历史，都会产生错误适用性。
Tushare 官方 `stock_st` 接口按交易日提供历史 ST 名单，适合保存为独立来源事实。

## 决策

1. 新增 Tushare `stock_st` 按交易日全量采集，不混入 pytdx 日 K，保留原始响应为不可变 Raw，
   并在 PostgreSQL 保存该交易日的完整 ST 标准代码集合、来源批次及数量。
2. 只在请求成功、交易日与每行日期一致、代码唯一且合法、行数非空且未达到官方单次上限时，
   发布该日完整覆盖。失败、超时或疑似截断不发布，也不把空集合当成“全市场无 ST”。
3. 监控按目标日读取完整覆盖：名单内为 ST；完整覆盖且不在名单内为非 ST；覆盖缺失为
   `INSUFFICIENT_DATA`。证券名称仅用于展示，不再作为 ST 判定依据。
4. 新增采集使用现有 Worker、Raw、IngestionRun 和 Raw replay 机制；日终步骤复用既有监管工作流，
   不新增操作系统定时任务。历史补采按精确交易日幂等运行。
5. 按 ADR-0060 的版本规则处理结果语义变化：已有批次保持不变，新的 ST 输入及其血缘进入
   监控输入哈希，连续历史结果须从 2026-07-06 顺序重算；不能把旧批次原地改写。
6. 不更改 `core.stock_daily_indicator` 一个月热数据保留策略。历史 `daily_basic` 仍走现有采集器；
   代理全市场响应未稳定返回前，不发起无人值守的大范围补采。历史换手事实与监控补算按独立
   运维步骤执行，不以零填缺。
7. 不新增公开 ST 接口，不改变既有 FastAPI/PostgREST 契约和普通 Daily Bar 路由。
8. 适用性输入变更使监管计算算法升为 `regulation-calculator.v3`、独立监控升为
   `regulation-monitor.v2`；公开监控响应结构版本仍为 `regulation-monitor.v1`。
9. 监控批次保存 ST 来源水印并在查询链时与现行逐日快照比对；更正即使旧链失效，
   不在 Domain 记录中直接放入采集 ID。

## 官方来源

- [Tushare ST 股票列表](https://tushare.pro/document/2?doc_id=397)：`stock_st` 可按交易日获取历史名单，
  单次最多 1000 行，包含代码、名称、交易日与风险警示类型。
- [Tushare 每日指标](https://tushare.pro/document/2?doc_id=32)：`daily_basic` 可按交易日获取换手率，
  单次最多 6000 行。

## 实施门禁

书面设计、Issue #85、迁移、Provider/Raw replay/监控测试及隔离 PostgreSQL 验证完成后才能发布。
生产迁移、Worker 部署、历史补采与连续补算分别核对授权；本 ADR 的接受不等于已在生产执行。
