# ADR-0002：AKShare 第二数据源接入

- 状态：Accepted
- 日期：2026-07-28
- 决策者：项目所有者
- 替代：ADR-0001 中“第一阶段唯一 Provider”和“第二数据源暂缓”的局部决策

## 背景

BaoStock 第一阶段闭环已经落地。项目需要接入 AKShare，以验证 Provider 边界并提供可人工选择的替代采集来源。自动切换会引入来源优先级、静默覆盖和结果不可复现问题，因此本次不同时引入 Router。

## 决策

1. AKShare 覆盖 Security、Trading Calendar 和不复权 Daily Bar，与 BaoStock 输出相同标准 DTO。
2. CLI 使用 `--provider baostock|akshare` 显式选择来源，默认仍为 `baostock`。
3. 不实现自动回退或健康路由；一个 ingestion run 只属于一个 provider。
4. AKShare 来源字段只存在于适配器和 Raw 文件；Core 仅接受标准字段及 `source_code=akshare`。
5. 日线调用必须显式使用 `adjust=""`。价格和金额由字符串构造 `Decimal`，不得用 `Decimal(float)`。
6. `stock_info_a_code_name` 无法可靠提供 IPO、退市日期和上市状态，AKShare Security 将这些字段映射为 `None`/`unknown`，不得猜测。
7. `tool_trade_date_hist_sina` 只返回交易日；Provider 必须补齐请求范围内的全部自然日。
8. Core 仅按领域自然键判断重复：Security 使用 `symbol`，Trading Calendar 使用 `(market, trade_date)`，Daily Bar 使用 `(symbol, trade_date)`。`source_code` 不参与唯一性判断，只记录当前事实的来源；相同自然键按标准 UPSERT 更新，并以最新成功批次的 `source_code`、`ingestion_id` 保留追溯关系。
9. 所有来源实现统一 `MarketDataProvider` 适配器契约，并通过 provider registry 创建。Pipeline 和 CLI 不依赖具体适配器类；新增来源只扩展适配器与注册表。

## 后果

- 两个来源可以独立采集、追溯和测试。
- 数据源创建集中在注册表，未知来源和注册代码不一致会立即失败。
- 既有 PostgREST API 契约不变。
- 数据库约束允许 `baostock` 和 `akshare`；重复数据身份只由领域自然键确定。
- 生产切换来源属于显式运维动作；多源交叉校验与自动降级仍不在本次范围。

## 验收

- 三类数据集都有 AKShare Provider 契约测试；
- Pipeline 的 provider、锁键、Raw 路径和 ingestion run 来源一致；
- 新 migration 从既有数据库向前扩展来源约束；
- Ruff、mypy、pytest 和 migration 检查通过。
