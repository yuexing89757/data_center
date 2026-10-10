# 专业版股票技术因子日快照

依据 ADR-0066、Issue #88。现有 daily_basic 每日指标继续独立运行。

## 上线前提

使用受保护的迁移流程应用 `20261010000100_create_stock_technical_factor.sql`；
Worker 的专用角色取得新表的最小写权限，FastAPI 角色只取得 api_v1 有界 RPC 执行权限。
既有 `market_data_backup` 角色仅取得新表 SELECT，以维持应用数据备份范围。
本地代码实现和测试不代表生产已经应用该迁移或启动新任务。
Worker 使用既有 `TUSHARE_TOKEN` 与 Tushare endpoint 配置；不要将凭据写入命令或报告。
账户需拥有 stk_factor_pro 权限；受控请求默认不超过 30 次/分钟。

## 每日运行

部署更新后重启既有常驻 Worker，任务目录自动注册 `stock-technical-factor-daily`。
默认周一至周五上海时间 21:00 执行，先同步并确认当日 CN_A_SHARE 日历；
休市跳过，采集精确当天全市场技术因子。Worker 本地任务页及 operations 执行记录
显示任务状态。操作系统仅监管 Worker 常驻，不添加操作系统采集计划任务。
21:00 是初始运维时间，不代表来源承诺该时刻数据就绪。

失败或来源数据未就绪时留下失败与缺口，不发布空快照，不用较旧日期替代。
无权限不重试；瞬时连接错误最多尝试三次。
如获准重试一个明确交易日，使用：

```powershell
uv run market-data-center --provider tushare stock-technical-factor-daily --as-of-date 2026-10-09
```

此命令会调用来源并写 Raw、审计和新快照，属于生产采集操作，须有明确授权。
新快照使用相同交易日整体修订；缺失证券保留为缺口，不残留旧批次的单股事实。
失败批次不改变此前成功快照；业务读取必须处理 missing_codes。

## 读取

FastAPI API-key 保护的 `POST /v1/stock-technical-factors/query` 接受：

```json
{"codes": ["600000", "000001"], "trade_date": "2026-10-09"}
```

最多 100 个六位股票代码，由 Security 解析交易所；返回 `items` 和 `missing_codes`。
每项含 `unadjusted`、`forward_adjusted`、`backward_adjusted`、`common` 以及
`factor_catalog_version`、`source_code`。前三组数值为精确十进制字符串或 null；
共同计数项为整数或 null。不回退日期，不触发采集。
PostgREST 标准 symbol 查询为 `api_v1.query_stock_technical_factors`，日期必填，
限 1～100 个 symbol；只能使用被授权的公开读角色。

v1 覆盖 74 个指标的三种复权口径及四个计数项，共 226 个值；来源字段命名在 Adapter
边界转成标准名，例如 kdj_qfq → forward_adjusted.kdj_j，rsi_qfq_6 →
forward_adjusted.rsi_6。保留负值、零及缺失值，不能互相替代。
接口的 OHLC、股本、估值等非技术字段仅随响应保存在不可变 Raw，不复制进新快照。
这不是项目自主计算的版本化 Derived 算法。

## 校验、重放与保留

空响应、达到单次 10,000 行上限、错交易日、未知/非活跃证券、非法数字、
目录结构缺失或冲突重复键均失败。低于前一个成功快照行数 90% 的市场覆盖失败；
首次以当日活跃 Security 作为基线。停牌等缺失不补值，必要时先排查来源覆盖和主数据。
硬不变量不降级为警告。Raw 的 schema 为 `tushare.stock_technical_factor.v1`。

既有 `raw-replay --ingestion-id ... --dry-run` 可先只读验证 Raw；正式重放属于写操作，
需明确授权。重放复用同一目录和原请求的覆盖下限，核对日期及当日证券生命周期；
不请求网络、不复制 Raw。当前保留每日历史快照，不自动清理、不默认全历史回补。
