# 涨停池集合竞价五档切换 pysnowball 设计

## 目标

仅将 Worker 任务 `opening-auction-limit-up-quotes` 的实时五档来源从
`pytdx_hq` 切换为 `pysnowball`，以改善竞价时段买卖二档等档位的可用性。
沪深全市场竞价序列、09:26 快照、收盘快照和普通 Daily Bar 均保持现有
PYTDX 路径。

## 已接受边界

- 上级决策为 ADR-0012；其已定义 `pysnowball` Provider 身份、Cookie Secret
  和五档标准化语义。
- 执行任务仍由 ADR-0022 管理：09:15:00–09:25:00，每 30 秒一轮，
  冻结当日精确 ready 的涨停池。
- 任务只使用 `pysnowball`，不回退 `pytdx_hq`，不在一轮内混合来源。
- 每次上游请求只携带一个 SSE/SZSE 股票。单股失败记录为该轮缺失，
  不影响已成功股票入库。
- 仅消费 `bp1..bp5/bc1..bc5` 和 `sp1..sp5/sc1..sc5`；六至十档不进入当前
  `FiveLevelQuoteSnapshotRecord` 和数据库。
- `PYSNOWBALL_TOKEN` 是服务器 Secret，不进入 Git、Raw、请求参数、日志或 API。
- `.env` 不接受任务时间或节奏配置；现有 `AUCTION_COLLECTION_ENABLED`
  仍只负责启用/停用。

## Provider 设计

新增 `PysnowballQuoteProvider`，实现现有
`RealtimeQuoteProvider.fetch_five_level_quotes()` 契约。Provider 把标准代码
`SSE:600000`/`SZSE:000001` 映射为 `SH600000`/`SZ000001`，逐只请求与
`pysnowball.pankou()` 相同的 `realtime/pankou.json` 端点。

上游 pysnowball 便捷函数未设置 HTTP timeout，所以生产 Adapter 使用可注入的
有界 HTTP client 访问同一 URL/Schema，并以 `json.loads(..., parse_float=Decimal)`
防止价格经过 `float`。Cookie 只进入 HTTP header，异常文本不带响应体或
header。

映射规则：

- `current` 转为 `last_price`；该接口不提供的昨收、开高低、累计量额保持
  `None`。
- `timestamp` 为合法 Unix 毫秒时转为 `source_timestamp`，否则保持 `None`。
- 价格和数量同时为有效正价/非负整数时保留；价格为零表示缺档，
  价量对统一规范化为 `(None, None)`。
- Raw 保留上游业务字段的字符串表示，不包含 Token 和 HTTP header。
- HTTP/授权失败、超时、响应非 JSON、symbol 不匹配和字段不合法均只把
  对应 symbol 放入 `failed_symbols`；已成功记录保留。

## 编排与存储

`run_auction_collection_job()` 只构造 `PysnowballQuoteProvider`。
`AuctionCollectionService` 从 Provider 的 `source_code` 生成 `IngestionRun`、Raw 分区和
session 身份，不再硬编码 `pytdx_hq`。这个 Service 仍只被涨停池任务使用，
全市场 Service 不引用新 Provider。

有序 migration 为 `ingestion.ingestion_run.provider_code`、
`realtime.auction_collection_session.provider_code` 和
`realtime.five_level_quote_snapshot.source_code` 增加 `pysnowball` 合法值，保留旧
`pytdx_hq` 事实可读。既有五档列、主键、RPC 和 FastAPI 契约不变。

## 验收

- Provider 单元测试覆盖 SSE/SZSE 映射、Decimal、股数单位、五档、缺档、
  Token 缺失、超时/授权失败、非法 Schema 和单股失败后继续。
- Scheduler 测试证明涨停池任务构造 pysnowball，全市场任务仍构造 PYTDX。
- 迁移集成测试证明 pysnowball session/run/quote 可写入，pytdx 历史行仍合法。
- 不执行真实全量雪球网络测试；上线前仅做 Token 预检和小样本冒烟。
