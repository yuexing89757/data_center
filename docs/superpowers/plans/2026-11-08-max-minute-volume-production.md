# 最大一分钟量正式实现与发布计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox syntax for tracking.

**Goal:** 在既有每日指标工作流维护原始最大一分钟量，按股票及准确交易日只读查询并部署。

**Architecture:** 独立pytdx数量适配器与严格单位身份，纯计算器；不可变Raw、来源采集/认证血缘、Derived结果与发布版本。HTTP仅调用api_v1 RPC。现有20:30工作流独立步骤，失败如实聚合。

**Tech Stack:** Python3.12/uv、PostgreSQL ordered migrations、FastAPI、APScheduler、既有pytdx池。

**Spec:** docs/superpowers/specs/2026-11-08-daily-max-minute-volume-design.md；ADR-0077。

## Global Constraints

- master原地开发；保留docs/evidence及tinker_classN等无关未跟踪文件。
- 不计算1.1倍/竞价尺度，不接收撮合或未匹配量。
- 不推断未经证实的peer/category/market单位；不跨peer拼批，不刷新池。
- 不将Raw、凭证、配置或数据库URL提交版本库。
- 常规发布不备份数据库/Raw；保护迁移、健康检查及旧版本回退保留。
- 只做明确样本SSE:600327/2026-11-03上线验证，不全历史回填。

## Review Focus

- 未知单位与其他市场错误借证：准入失败且无标准值/认证。
- 分页未完成、重复、缺柱：不返回部分最大值。
- 发布重试及局部采集：不可缩小旧有效集合，单一发布批次读取。
- 失败步骤被捕获：工作流partial/failed真实，既有Tushare步骤仍执行。
- API读不存在准确日期：not_ready，不触发采集或回退。

## Task 1: 来源、单位、领域与纯计算

- [x] 在providers/stock_minute_volume.py实现PytdxMinuteVolumeProvider(pool_path)，fetch(symbol, trade_date)返回MinuteVolumeSource。
- [x] 在domain/stock_max_minute_volume.py定义MinuteVolumeBar(timestamp,volume_shares)、MinuteVolumeSource(symbol,trade_date,bars,raw_rows,metadata,quality_reasons)、MinuteVolumePeak(symbol,trade_date,max_minute_volume_shares,peak_at,expected_count,observed_count,quality_reasons)、MinuteVolumeBuildSummary(status,trade_date,calculation_id,version,candidate_count,complete_count)。
- [x] 纯函数calculate_minute_volume_peak(source)完整240根才返回峰值，真实零/并列/首尾/午休/异常有测试。
- [x] 单位身份绑定peer/category/market/schema/field与证据版本，新采集及Raw重放均验证。多证券/日期/市场交叉证据不足的市场不得准入。
- [x] 聚焦测试先失败后通过，复核真实只读样本；不生产写入。

## Task 2: 存储及公开只读接口

- [x] persistence/stock_max_minute_volume_postgres.py实现is_trading_day、candidates(date,symbols=None)、build_lock、record_source(run,manifest,certificate)、publish(date,peaks,evidence)和版本/血缘/旧覆盖保护。
- [x] ordered migration 20261108000100_stock_max_minute_volume.sql：dataset约束、Derived结果/发布表、最小权限RLS、api_v1.query_stock_max_minute_volume(text[],date)；复用calculation_run。
- [x] public_api/stock_max_minute_volume.py新增POST /v1/stock-daily-indicators/minute-volume-peak/query；symbols1..500、明确日期、ApiTimestamp、仅RPC，不采集。
- [x] 三份公开契约同步；unit/API及隔离PostgreSQL验证自然键/权限/回滚/版本/准确日期。

## Task 3: 编排、记录及日常接入

- [x] stock_max_minute_volume_service.py：加载有效stock宇宙，逐市场固定peer，Raw/ingestion/certificate之后发布；新dataset为stock_minute_volume，不建立分钟明细Core。
- [x] cli新增明确日期/可选symbols的手工指标入口；20:30日历之后独立指标步骤，Tushare独立继续，保留清理条件不放宽。
- [x] operations与scheduling_catalog目录同步，失败聚合覆盖测试；新步骤无OS任务。

## Task 4: 审查、完整验证与部署

- [ ] 独立spec/quality审查，解决硬问题；ruff format/check、mypy src、完整pytest、隔离integration通过。
- [ ] Issue105更新；所有相关文件按明确名单commit，推送Gitee origin及GitHub master，等待CI。
- [ ] GitHub protected production migration；部署Worker/API新发行，旧发行路径保留可回退，不备份。
- [ ] 只核对明确样本并从真实HTTP精确日期读取结果；记录版本、schema、计数、任务及健康结果于实现/运行文档，不扩写Wiki。

## Execution ledger

- Ruling: 在master开发，依据AGENTS明确指令，不创建worktree。
- Ruling: 现行用户部署授权已明确接受此前最小方案；单位证据是独立技术门禁，不因授权免除。
- Ruling: 两个实现任务独立写文件；Task1产出领域接口供Task2/3使用，接口固定如上；共用文件由root整合，禁止并发改cli/operations/registry。
- Ruling: 当前正式状态为实施中；本地实验20项通过不等于新链路验证通过。

- Task1/2/3 complete: 源适配/单位门禁、版本存储/RPC、HTTP与每日编排均接通；正式聚焦测试120 passed，隔离真实来源HTTP样本7,260,200股@09:58。
- 独立来源及全链路审查完成，四项P2均修复并复核；新回归25 passed。
- Ruff格式/检查与mypy195源文件通过；全量2601测试含本地隔离PG正在执行。
- 运维入口已恢复；现存节点仍为已核验peer。原Worker/API版本b8cba2b/d10fa45及各自依赖环境保留。
