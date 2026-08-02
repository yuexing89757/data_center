# 领域详设：DeductedProfit

`DeductedProfitRecord` 是 provider-neutral 的财务盈利点时事实，字段包括标准 symbol、报告期、
公告日期、实际公告日期、累计扣非净利润、单季度扣非净利润、更新标识、修订键与来源。
金额为 CNY Decimal；零是有效值，缺失保持 `None`。

Core 自然键为 `(symbol, report_period, revision_key)`。revision key 由全部标准事实字段确定性
计算，同一 Raw 重放幂等，数值或公告信息变化形成新版本。`first_observed_at` 由数据库首次
插入时记录，冲突不更新。as-of 查询必须同时满足有效公告日和首次观察时间边界，避免后来
修订进入历史视图。

增量任务每天 20:00（包括周末）扫描最近五个报告期的 `disclosure_date`，只对 actual date
或 modify date 当日变化的 symbol/report period 调用 `fina_indicator`。两类响应存入同一个
不可变 Raw 批次；金额事实只取自 `fina_indicator`。当前 Token 不具备 `fina_indicator_vip`
权限，因此不使用全市场 VIP 接口，不执行历史回填。

稳定查询为 `api_v1.query_deducted_profits_as_of`，最多返回 2000 个证券，提供累计与单季度
金额及其是否大于零。它不暴露 ingestion、revision key、source code 或首次观察内部字段。
