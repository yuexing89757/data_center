# 领域详设：Trading Calendar v2

> 状态：第一阶段有效  
> 修订日期：2026-07-27  
> 依据：`adr/ADR-0001-第一阶段架构基线.md`

## 1. 领域职责

Trading Calendar 提供市场时间基准，回答某个市场在某一天是否交易。它不包含证券停牌、行情、股本或统计信息。

第一阶段只维护统一 A 股市场日历：

```text
market = CN_A_SHARE
timezone = Asia/Shanghai
```

不分别维护 SSE、SZSE、BSE 日历。

## 2. 实体：TradingDay

数据库表：`core.trading_calendar`

| 字段 | 类型 | 约束与含义 |
| --- | --- | --- |
| `market` | text | 第一阶段固定 `CN_A_SHARE` |
| `trading_date` | date | 市场本地日期 |
| `is_trading_day` | boolean | 是否开市 |
| `previous_trading_day` | date | 可重算派生字段 |
| `next_trading_day` | date | 可重算派生字段 |
| `source` | text | 来源 |
| `ingestion_id` | uuid | 采集批次 |
| `created_at` | timestamptz | 创建时间 |
| `updated_at` | timestamptz | 最近更新时间 |

主键：`(market, trading_date)`。

## 3. 日期覆盖

日历必须包含请求范围内的所有自然日，而不是只保存开市日。这样才能明确区分：

- 已知休市：存在记录且 `is_trading_day=false`；
- 未知日期：不存在记录；
- 正常交易：存在记录且 `is_trading_day=true`。

如果来源只返回开市日，Provider 必须结合请求闭区间补齐自然日并将非返回日期标记为休市；该适配规则需要契约测试。

## 4. 前后交易日

`previous_trading_day` 和 `next_trading_day` 由同一市场完整日历确定性计算：

- 对交易日和休市日均填写最近的前后开市日；
- 边界外数据不足时允许为空；
- 补数扩大日期范围后重新计算受影响边界；
- Calculator 只接收日历序列并返回结果，不直接访问数据库。

## 5. 标准 DTO

```text
TradingDayDTO
├── market
├── trading_date
├── is_trading_day
├── source
└── ingestion_id
```

前后交易日不由 Provider 提供，而由领域 Calculator 计算。

## 6. 幂等与校验

- 按 `(market, trading_date)` upsert；
- 同一采集批次重跑结果一致；
- 请求范围不得出现日期缺口；
- 每条记录的日期必须位于请求闭区间；
- `previous_trading_day < trading_date < next_trading_day`；
- 前后日期自身必须是开市日；
- 来源冲突进入质量结果，不静默选择。

## 7. API 契约

`api_v1.trading_calendar` 只读 View 公开：

```text
market, trading_date, is_trading_day,
previous_trading_day, next_trading_day
```

## 8. 第一阶段验收

- 指定闭区间同步后没有自然日缺口；
- 周末与已知节假日可区分为明确休市；
- 未同步日期查询结果与休市不同；
- 前后交易日可从事实序列重算；
- 客户端不能写 `core.trading_calendar`。
