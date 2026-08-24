# ADR-0044：腾讯批量实时五档 Provider 与只读契约

- 状态：Accepted
- 日期：2026-08-23
- 关联 Issue：#62
- 决策者：项目所有者
- 扩展：ADR-0011、ADR-0012

> 2026-08-23：第 8～10 条及 FastAPI 数据库读取结论已由 ADR-0045 替代。Provider 字段、
> 单位、Raw replay 和显式 Worker 采集能力继续有效。

## 背景

项目所有者确认已取得腾讯 `qt.gtimg.cn` 股票批量行情数据的访问、缓存和 API 再分发授权，
并要求将该来源接入外部 API。只读实测确认响应为 GBK 文本，每只股票当前包含 88 个以 `~`
分隔的字段；接口没有可依赖的公开版本协商，因此必须保留 Raw 并将字段漂移作为显式失败。

FastAPI 的只读数据库边界继续有效。API 请求不得直接访问腾讯、创建 IngestionRun、写 Raw
或触发采集。

## 决策

1. 新增 Provider 身份 `tencent_quote`，仅实现现有 `RealtimeQuoteProvider` 能力，不加入
   Security、Calendar、Daily Bar 或自动路由。
2. 首版只支持 SSE/SZSE 普通股票，一次显式命令接受 1～500 个唯一标准 symbol；Adapter
   每个 HTTP 请求最多 50 只。没有 APScheduler、cron、Windows Task Scheduler 或持续轮询。
3. HTTP 地址固定为 `https://qt.gtimg.cn/q=`，总超时 1～10 秒，响应最大 2 MB，按 GBK
   严格解码。未知代码、缺行、重复行、编码错误和不足 49 个字段均不得伪装成成功。
4. 字段映射固定为：代码 2、最新价 3、昨收 4、今开 5、累计成交量 6、买一至买五
   9～18、卖一至卖五 19～28、来源时间 30、最高 33、最低 34、复合字段 35。下标 29
   和其他未验证尾部字段不进入标准 Record。
5. 累计成交量和五档量由“手”乘 100 转为“股”。累计成交额只取字段 35 的第三段并按
   CNY 保存；字段 37 的万元展示值因截断不使用。全部价格和金额直接由字符串构造 Decimal。
6. 字段 30 按 `yyyyMMddHHmmss Asia/Shanghai` 解析为必需的 `source_timestamp`。
   `observed_at` 是完整收到 HTTP 批次后的 UTC 时间。周末或闭市时返回旧行情属于陈旧事实，
   不因刚完成 HTTP 请求而变成实时数据。
7. 新增 append-only `realtime.stock_quote_snapshot`。每批先登记 IngestionRun，Raw 使用 JSONL
   保存来源 symbol 和完整分隔字符串，再经领域 Validator 原子写入快照、Manifest 和质量结果。
8. 新增 `api_v1.query_latest_stock_quotes(p_codes,p_max_age_seconds)`，代码数 1～500、最大时效
   1～86400 秒。只有 `observed_at` 与 `source_timestamp` 同时满足时效的最新行才返回；未知、
   缺失和陈旧代码进入 `missing_codes`。跨交易所六位代码歧义继续返回 `P0003`。
9. FastAPI 发布 `POST /api/v1/realtime-quotes/latest/query`，只代理上述 RPC 并复用
   `X-API-Key`。它不直接读取 `realtime`、不访问腾讯、不写库、不触发显式采集。
10. 任何持续采集计划仍需独立 Issue 明确交易时段、cadence、全集、容量、保留期和上游预算。

## 后果

- 腾讯字段和单位被限制在 Adapter 边界，消费者只看到 provider-neutral 五档契约。
- API 的“最新”严格表示最近且来源时间也未陈旧的已持久化快照，不承诺请求时抓取。
- 调用方必须通过 Worker/CLI 显式采集，或等待未来获批的 Worker 任务；无新快照时接口会明确
  返回缺失而不是旧值。
