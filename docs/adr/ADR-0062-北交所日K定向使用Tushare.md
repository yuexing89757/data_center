# ADR-0062：北交所日 K 定向使用 Tushare

- 状态：Proposed（待项目所有者书面确认）
- 日期：2026-09-30
- 关联 Issue：#86
- 局部替代：ADR-0005、ADR-0024、ADR-0013 中普通股票 Daily Bar 自动路由仅 pytdx 的决定；仅限 BSE

## 背景

生产只读审计显示，2026-09-23 至 09-29 的 pytdx 日 K 采集：BSE 失败 1,732 次，
SSE 成功 9,280 次，SZSE 成功 11,609 次。2026-09-29 的 BSE 样本通过现有 Tushare
`daily` 适配器可返回未复权日 K。用户要求稳定的采集保持原样，只将不稳定路径改用 Tushare。

## 提议决策

1. `--provider auto` 的普通股票 Daily Bar 按标准 `symbol` 的市场选择：
   `BSE` 仅尝试 `tushare`，`SSE` 和 `SZSE` 仍仅尝试 `pytdx`。
   Security 与 Trading Calendar 仍为 `baostock → akshare`；显式 Provider 选择不变。
2. 复用 ADR-0013 的 Tushare `daily` Adapter、Raw schema、Decimal 与单位转换，
   不引入第二个日 K 适配器、跨来源混合或新采集任务。
3. BSE 的 Tushare 超时、限流、无权限、非法响应或空结果必须留下可诊断的失败或
   `ProviderRequestUnavailable` 与日 K 缺口；不得回退到当前持续失败的 pytdx，
   也不得把空批次报告为已补齐。停牌无成交日仍保留缺口，不合成 K 线。
4. 成功批次只有一个实际 Provider；Core 行、IngestionRun 和不可变 Raw 保留既有血缘。
   不覆盖已有历史事实。新路由只影响后续自动采集和明确指定的缺口补采。
5. 不改变表结构、公开查询契约或 Worker 调度时间。现有迁移
   `20260802000100_allow_tushare_provider.sql` 已允许 Tushare 来源，因此不新增空迁移。

## 验收与风险

- 覆盖单股和批量自动路径：BSE 走 Tushare，沪深仍走 pytdx；显式来源行为不变。
- Mock 覆盖 BSE 成功、来源错误、空响应、无权限、Raw replay、单位及代码映射；
  缺口不计为成功，且不发生跨源回退。
- 隔离数据库验证 Core/Raw/IngestionRun 来源血缘与已有事实不被覆盖。
- 上线后核对 BSE 成功率及缺口，同时确认沪深和 Security/Calendar 路由未变化。
- Tushare 是外部单一来源，其故障会留下可见 BSE 缺口；不以伪造成功掩盖故障。

本 ADR 被接受前，不更改自动路由，也不执行生产采集、迁移或部署。
