# 北交所日 K 定向路由设计

- 日期：2026-09-30
- 状态：待书面审阅
- Issue：#86
- ADR：ADR-0062（Proposed）

## 目标与边界

修复生产自动采集中的北交所日 K 长期失败，同时不触碰已经稳定的沪深日 K、证券目录、
交易日历和其他数据集。依据 2026-09-23 至 09-29 的只读审计：pytdx BSE 失败 1,732 次，
沪深合计成功 20,889 次；Tushare `daily` 的单只 BSE 样本可返回记录。

## 最小实现

现有自动路由只按数据集选择 Provider，需要在日 K 的单股入口和批量逐股入口按标准
`symbol` 识别市场。BSE 的候选仅为已有 `tushare`；SSE/SZSE 的候选仍仅为 `pytdx`。
沿用同一日常 Worker 工作流、逐股准备与批量写入、Raw 保存、Validator 和 Persistence。
显式 `--provider`、Security、Trading Calendar 及其他数据集不变。

Tushare `daily` 按现有单股日期范围请求，价格/成交量/成交额继续在 Provider 边界转为
Decimal、股、元。每个成功 IngestionRun 只有 Tushare 一个实际来源，Raw 使用已存在的
`tushare.daily_bar.v1`，Core 维持未复权自然键及现有幂等策略。无需新表、公开接口、
调度任务或迁移；已有 `20260802000100_allow_tushare_provider.sql` 支持该来源。

## 缺口与失败

Tushare 报错、限流、超时、缺字段时沿用 ProviderError 语义。空响应不能成为“已补齐”的
成功批次：对请求范围内尚缺日 K 的交易日，仍显式记录不可用/失败；对停牌等确实无
交易记录的日期也不填造 K 线。BSE 不回退 pytdx，不合并两个来源的部分响应。
现有缺口查询继续决定下次是否尝试；历史数据只对明确缺口定向补采，不整批覆盖。

## 验收与交付

先以 Mock 验证单股及批量路径的三市场路由、Tushare 空响应与异常、显式来源、
单位与代码映射、Raw replay；再用隔离 PostgreSQL 验证血缘、幂等和缺口计数。
运行 Ruff、mypy、pytest 完整本地门禁。生产发布和定向补采需要另行授权，发布后
只读核对 BSE 覆盖率及沪深原来源不变。

不建设通用按数据质量评分的路由器、自动回退链或新配置项。其他不稳定采集路径
需分别有故障证据，再另行决策。
