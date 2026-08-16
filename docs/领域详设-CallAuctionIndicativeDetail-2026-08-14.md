# CallAuctionIndicativeDetail 领域详设

`trade_date` 是 Asia/Shanghai 当前交易日；`observed_at` 由该日期与来源 `HH:mm:ss` 组合为带时区
时间。自然键为 `(ingestion_id, source_sequence)`，快照版本键为 `(symbol, trade_date, version)`。

持久化字段：标准 symbol、交易日、观测时间、Decimal 元/股的 `indicative_price`、由来源“手”
精确乘 100 得到的 `displayed_volume_shares`、来源顺序，以及枚举化但明确不可信的
`source_display_classification`。来源 auxiliary 值和原始 display code 只存在 Raw，不进入事实表。

质量规则：仅 SSE/SZSE 六位代码；日期必须为当天且日历标记开市；时间严格落入
09:15:00–09:25:59；价格正数；量为非负整手并转换为股；顺序非负；响应达到 5000 行视为可能
截断并失败。空窗口、格式错误、超时、HTTP/JSON/反爬异常不可发布 succeeded。部分结果只能以
`partial` 暴露并携带质量状态，不能被 API 描述为完整。

API 按 `observed_at, source_sequence` 确定排序，offset 0–5000，limit 1–500。响应固定声明
`is_exchange_trade_tick=false`、`is_order_by_order=false`，不提供历史回退。

HTTP 接口唯一必填输入是六位 `code`：`6xxxxx` 映射 SSE，`0xxxxx`/`3xxxxx` 映射 SZSE，
其余号段拒绝。服务自动使用 Asia/Shanghai 当日，先查询最新 succeeded/partial 数据库快照；命中
即返回，只有 RPC 明确 P0002 时才实时访问东方财富。数据库异常不得触发回源。实时结果先保存 Raw、
直接返回并异步登记数据库；响应通过 `data_origin` 和 `persistence_status` 区分两条路径。
