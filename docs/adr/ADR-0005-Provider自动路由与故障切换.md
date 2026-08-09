# ADR-0005：Provider 自动路由与故障切换

- 状态：Accepted
- 日期：2026-07-28
- 实现澄清：2026-08-09 股票 Daily Bar 固定使用 ADR-0024 的远程 pytdx；不回退其他 Provider
- 决策者：项目所有者
- 替代：ADR-0002、ADR-0004 中“禁止自动路由或回退”的局部决策

## 背景

系统已经有 BaoStock、AKShare 和本地 pytdx 三个 Adapter。显式选择便于复现，但批量采集会因单一来源限流、连接失败、本地文件缺失或陈旧而整体降级。项目需要一个位于 Provider 与 Pipeline 之上的路由模块，按数据集能力自动选择来源，并在来源故障时切换。

## 决策

1. 新增 `ProviderRouter` 编排模块，不把 Router 伪装成数据源，也不新增 `source_code=router`。
2. 默认确定性路由顺序为：
   - Security：`baostock → akshare`；
   - Trading Calendar：`baostock → akshare`；
   - Daily Bar：仅使用本地 `pytdx`，不再通过 BaoStock 或 AKShare 补缺口。
3. CLI 默认使用 `--provider auto`；显式指定 `baostock`、`akshare` 或 `pytdx` 时完全绕过路由。
4. 自动模式的股票输入使用标准 `symbol`（例如 `SSE:600000`）。Router 在每次尝试前调用具体 Adapter 的 `source_symbol`，来源代码不得越过 Adapter 边界。
5. 只有 `ProviderError` 可触发自动切换，包括来源连接失败、上游错误码、字段或文件格式异常、本地文件缺失和数据陈旧。数据库、权限、Raw Store、编程错误等其他异常立即向上抛出，禁止用切源掩盖系统故障。
6. 单次操作按固定顺序逐个尝试，每个来源最多一次；一个来源连续发生 3 次 `ProviderError` 后，在当前 Router 生命周期内熔断。成功一次即清零该来源连续失败计数。
7. 发生 Provider 错误后释放该 Adapter 会话；下一次操作在未熔断时重新建立会话。Router 关闭时必须释放所有已创建的 Adapter。
8. 每个 Pipeline 尝试使用具体 Provider 创建 IngestionRun。成功批次记录实际 `provider_code/source_code`；已经进入 Pipeline 的失败尝试记录为 failed ingestion run。创建或连接阶段失败至少输出结构化路由尝试信息。
9. 一个成功批次只来自一个 Provider，不合并多个来源的部分结果。数据库仍只按领域自然键去重，来源不参与唯一键。ADR-0024 生效后，Daily Bar 以单个远程 TDX endpoint 的实际响应为准；停牌、节点缺数或市场不支持时保留缺口，不伪造 K 线，也不触发其他 Provider 补数。
10. Router 不修改 Provider 优先级来追逐偶然速度，也不在运行时发现未知来源；新增来源或改变默认顺序必须修改策略并补充测试/ADR。

## 后果

- 单一 Provider 故障不再必然中断采集；
- 默认选择和切换顺序确定、可测试，显式模式仍可完整复现；
- 实际来源继续由现有采集批次和事实字段追溯，无需扩展数据库来源枚举；
- 若所有候选来源均失败，Router 汇总尝试并使当前操作失败；
- 需要所有 Adapter 将来源侧异常规范化为 `ProviderError`。

## 验收

- 单元测试覆盖数据集路由顺序、成功选择、ProviderError 回退、非 Provider 错误不回退、熔断和资源释放；
- CLI 支持默认 `auto` 以及三个显式 Provider；
- 自动模式的成功 IngestionRun 记录实际 Provider，不出现 `router/auto` 来源；
- Ruff、mypy 和 pytest 通过。
