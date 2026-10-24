# 竞价快照历史读取 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 两个现有快照接口按精确日期读取保留的竞价历史事实。
**Architecture:** 内部unit-safe统一视图按完整attempt选择hot/history；有序迁移仅重定向两个既有RPC的数据来源，保留选择规则、单位门禁、ACL及响应。
**Tech Stack:** Python3.12、PostgreSQL、现有FastAPI。
**Spec:** `docs/adr/ADR-0074-竞价快照在线与历史统一有界读取.md`（Accepted），Issue #97。

## Global Constraints

- 直接在master开发，保留用户`tinker_classN/`；后续“修复生产数据”已授权通过受保护流程生产修复。
- 单日1～500代码、最多32轮、5秒timeout；禁止跨attempt或物理副本逐证券拼补。
- Raw与历史事实数值不改；认证操作独立于新增读取代码。

## Review Focus

- 在线只剩一行而历史完整：返回在线缺失，不逐代码补历史。
- 热历史重叠：同一attempt只返回一次。
- 历史独有未认证：价格可读，量额null并保留质量；认证后原值可读。
- 新会话无事实：保持既有两个RPC各自候选规则。
- 全32轮及500代码预算、分区裁剪、消费者无内部SELECT权限。

### Task 1: 统一读取迁移及数据库回归

**Files:** create `tests/test_auction_history_read_postgres.py`、`supabase/migrations/20261024000700_read_auction_snapshot_history.sql`。
**Interfaces:** 原两个RPC签名不变；内部`audit.unit_safe_auction_snapshot_read`只供definer读取。

- [x] 写history-only/overlap/hot-partial/no-attempt-mix/quality/batch/权限回归；隔离数据库运行确认RED。
- [x] 实现整attempt选择视图、严格白名单RPC定义重定向；14项集成GREEN，包含预算及查询计划验证。

### Task 2: API契约与交付

**Files:** modify `src/market_data_center/public_api/app.py`、`contracts/{postgrest-openapi-v1,agent-tools-v1,fastapi-openapi-v1}.json`、领域详设、数据库导航、运行手册、ADR索引；新增运行记录。
**Interfaces:** 两个既有HTTP路径不变；文档声明精确日期历史范围及实际上线状态。

- [x] 同步端点说明和契约，添加契约断言；保留其他竞价接口原语义。
- [x] 运行format、ruff、mypy、完整pytest及隔离PostgreSQL测试；独立最终审查。
- [x] 核验64个数据认证及事实/Raw证据不变，完成受保护迁移、独立API切换和真实HTTP验收。

## 执行记录

- 已观察历史读取RED（404/空轮次），初版11项集成GREEN。
- 聚焦认证和读取22项通过，三份契约测试19项通过；format/ruff/mypy通过。
- 最大500代码×32轮触发性能RED：重复字段认证路径约39秒；逐行lateral proof仍约23秒。
  内部attempt reader先绑定原轮次及请求证券，再按同一质量函数一次认证并投影，保留精确类型。
- Ruling: 新迁移沿用质量函数与字段白名单实现按attempt的unit-safe投影，以避免复用旧视图导致重复调用；
  认证语义不变，由认证/未认证量额及最大请求回归验证。若错误将影响历史量额读取，故上线前独立审查。
- 新增盘口零/37股夹具最初违反价格为空且数量为0的既有CHECK，已补真实价格；不放宽数据库约束。
  修正后14项集成通过，单日分区裁剪/索引和私有helper权限均验证。
- 最终CI40080689174及生产apply40081366000通过；静态检查已区分函数定义与引用。
  本地全量2309通过/4跳过，隔离库恢复补跑通过，剩余3项Windows软链接权限限制。
- 64认证/审计完成，334336事实指纹不变；72个有界HTTP请求逐项匹配全部序列及末轮。
  API17:27:04安全切换，Worker未重启。实际结果与证据见对应runbook。
- 最终独立审查无P0/P1/P2，无延期minor；按项目明确要求直接master提交，不新增分支或worktree。
