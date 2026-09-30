# 异动监控本期起算 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 按已批准的2026-09-01起点只补算最近一个月，接口注明本期内次数。

**Architecture:** 现有输入携带起算日期，纯计算和投影共用该范围。沿用已有不可变source、检查点和RPC，以v3隔离新口径，保留旧哈希。

**Tech Stack:** Python 3.12、SQLAlchemy、PostgreSQL有序migration、FastAPI、pytest。

**Spec:** docs/superpowers/specs/2026-09-30-regulation-monitor-period-design.md

## Global Constraints

- 起点2026-09-01；本次历史区间2026-09-01至09-29；之后接续，不能自动月度清零。
- 旧v1/v2哈希及正式公告不改；新版本regulation-monitor.v3。
- 无数据不能填0；不同版本/起点检查点不能混用。
- 不新增表、依赖、公开写接口或OS调度；迁移只走受保护流程。
- 保留工作区已有无关改动；直接在master开发。

## Review Focus

1. 本期首日前两日大涨、首日平盘：不能算出本期价格事件；Task1。
2. 首日后缺检查点：不能重新清零；Task1/2。
3. 改起点但版本相同：输入哈希改变，父链不可混用；Task1/2。
4. 回放旧v2批次：不因新增字段改变哈希或历史阈值；Task1。
5. API显示10日计数：同时带起算日期和准确范围标签；Task2。

### Task 1：输入、纯计算和顺序服务

**Files:** domain/regulation.py、regulation_calculator.py、regulation_monitor.py、regulation_monitor_codec.py、regulation_service.py，相关calculator/projection/codec/service测试。

**Interfaces:** RegulationCalculationInput.monitor_start_date默认规则生效日；RegulationMonitorService默认本期2026-09-01；calculate_monitor_day和_threshold按source起点。

- [ ] 先增加首日空状态、期前涨幅排除、次日拒绝缺checkpoint及哈希兼容测试，运行focused pytest确认失败。
- [ ] 加输入范围字段和校验；旧v1/v2哈希删除新增字段；服务装配v3与本期日期；纯计算与反解替换监控初始化边界。
- [ ] 运行focused tests，确认换手缺口与价格规则独立、旧回放正确。

### Task 2：持久化、RPC和返回契约

**Files:** persistence/regulation_postgres.py；追加20260930000300_regulation_monitor_period.sql；public_api/regulation_monitor.py；三个contract；相关PostgreSQL/API测试。

**Interfaces:** checkpoint以algorithm_version和source.monitor_start_date匹配；RPC metadata返回monitor_start_date与count_scope_label。

- [ ] 先加隔离数据库跨版本/跨起点拒绝、首日发布、scope元数据测试；API测试覆盖新字段。
- [ ] 有序migration按同版本/同起点验证链；RPC可读旧v2和新v3，public schema形状保持兼容新增字段。
- [ ] 同步元数据模型和三份契约；旧fixture明确补起点与标签，运行API与数据库tests。

### Task 3：验证与补算交付

**Files:** docs/runbooks/regulation-monitor-period.md；本次执行ledger。

- [ ] 跑Ruff、mypy、完整pytest和隔离数据库验证，记录跳过项真实原因。
- [ ] 仅提交本任务变更、推送；受保护流程迁移；部署同版本API/Worker并冒烟。
- [ ] 核对无7月补算进程，标记确认为孤立的7月8日RUNNING失败。
- [ ] 本期逐日预检再补算，日志只记录日期/覆盖/状态；不依赖SSH连接存活。
- [ ] 核对20日连续批次、scope、覆盖与代表API查询；不足事实列明。
