# Market Data Center 项目宪法 v3

> 状态：有效  
> 初版日期：2026-07-24  
> v3 修订日期：2026-07-29

## 1. 项目愿景

Market Data Center 是一个独立的证券数据产品，负责数据的采集、标准化、校验、存储、客观计算和服务，为研究脚本、看板、回测平台及 AI Agent 提供一致、可靠的数据基础。

一句话宗旨：**让数据回归数据，让业务专注业务。**

## 2. 系统边界

数据中心只维护：

- 可追溯的来源原始数据；
- 证券、交易日、行情等客观事实；
- 给定相同输入即可重算的客观派生与统计；
- 面向消费者的稳定查询契约。

以下内容不进入数据中心：

- 情绪周期、冰点、高潮等主观解释；
- 主线题材和交易策略；
- 回测撮合与资金管理；
- 只服务某个上层产品的业务判断。

## 3. 核心原则

1. **事实优先**：先落事实，再做计算。
2. **可追溯**：标准事实可以追溯到 Provider、采集批次和 Raw 对象。
3. **可重放**：标准事实和派生结果可由 Raw 重新生成。
4. **统一契约**：来源差异在 Provider 边界内消化。
5. **职责单一**：Provider 采集和适配，Validator 校验，Calculator 纯计算，Persistence 负责存储。
6. **计算版本化**：复权、因子和统计规则必须可解释、可回溯。
7. **Schema 即代码**：数据库结构只能通过版本化 migration 演进。
8. **默认最小权限**：数据库、API、CI 和运维身份分别授权。
9. **渐进开发**：只实现当前阶段有验收标准的领域和能力。
10. **文档服从事实**：Wiki 和设计文档不得把未实现提案写成当前能力。

## 4. 当前阶段

第一阶段 MVP 已完成以下基础闭环：

- Security；
- Trading Calendar；
- 未复权 Daily Bar；
- BaoStock 默认 Provider，以及 Accepted ADR 明确批准的显式可选 Provider；
- Raw Store 与采集审计；
- 数据质量校验；
- 自托管 Supabase PostgreSQL/PostgREST；
- migration、测试、CI/CD 和备份流程。

当前进入 MVP 后的增量阶段。新增领域必须先有 Issue、Accepted ADR、领域详设、migration 和测试；BoardIndex、Capital、Classification、Derived/Metrics 已分别由 ADR-0003、ADR-0007、ADR-0008、ADR-0009 接受，稳定 PostgREST/Agent 查询契约由 ADR-0010 接受。当前仍不建设 FastAPI、MCP 和分钟行情。Provider 自动路由仅按 ADR-0005 的确定性策略实现。

## 5. 文档优先级

发生冲突时按以下顺序处理：

```text
项目宪法
  → Accepted ADR
  → 股票数据中心技术方案
  → 当前阶段领域详设
  → Issue / PR
  → Wiki 与其他说明
```

若实现需要偏离 Accepted ADR，必须先创建新 ADR 替代旧决策，不得让代码静默偏离。
