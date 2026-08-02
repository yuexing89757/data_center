# 领域模型总纲 v3

> 状态：有效  
> 修订日期：2026-08-02
> 上级文档：`项目宪法-MarketDataCenter-2026-07-24.md`

## 1. 当前领域范围

当前已实现九个业务领域和一个采集审计边界：

| 边界 | 职责 | 当前实体 |
| --- | --- | --- |
| Ingestion | 采集批次、Raw 清单和质量结果 | `IngestionRun`、`RawManifest`、`QualityResult` |
| Security | 证券身份、生命周期和名称历史 | `Security`、`SecurityNameHistory` |
| Trading | A 股市场交易日历 | `TradingDay` |
| Market | 不复权日频量价事实 | `DailyBar` |
| StockDailyIndicator | 来源发布的每日估值、换手率、股本、市值和涨跌停状态快照 | `StockDailyIndicatorSnapshot` |
| Capital | 股本和公司行为输入事实 | `ShareCapital`、`Distribution`、`RightsIssue` |
| Classification | 分类目录和成员历史 | `ClassificationCatalogSnapshot`、`ClassificationMemberSnapshot`、`MemberInterval` |
| BoardIndex | 第三方板块指数定义、不复权日 K 和逐日动态成分快照 | `BoardIndex`、`BoardIndexDailyBar`、`BoardIndexConstituentSnapshot` |
| Derived | 版本化证券级客观派生 | `CalculationRun`、`AdjustedDailyBar`、`DailyMetric`、`MarketCapitalization` |
| Metrics | 分类横截面客观统计 | `ClassificationDailyMetric` |

BoardIndex、Capital、Classification、Derived/Metrics 和 StockDailyIndicator 分别由 ADR-0003、ADR-0007、ADR-0008、ADR-0009、ADR-0014 进入实现。

## 2. 依赖方向

```text
Ingestion ───────────────┐
                        ▼
Security ────────────► Market
Trading ─────────────► Market
Security ────────────► Capital
Security + Trading ──► StockDailyIndicator
Security ────────────► Classification
Security + Trading ──► BoardIndex
Market + Capital ────► Derived
Derived + Market + Classification ──► Metrics
```

- Ingestion 提供来源追溯，不包含业务事实语义。
- Security 和 Trading 不依赖 Market。
- Market 通过 `symbol` 关联 Security，通过 `(market, trade_date)` 关联 Trading。
- Capital 通过 `symbol` 关联 Security，不依赖 Market，也不发布复权行情。
- StockDailyIndicator 通过 Security 和 Trading 保证引用完整性，不依赖或替代 DailyBar。
- Classification 通过 `symbol` 关联 Security；分类定义不是 Security，也不依赖 Market。
- BoardIndex 定义不是 Security；其日 K 关联 Trading，其逐日成分只引用已知 Security。
- Derived 只依赖客观输入事实，Calculator 不访问数据库。
- Metrics 只发布可重算的客观横截面统计，不包含主观市场解释。
- 禁止基础领域反向依赖行情或未来统计领域。

## 3. 数据流

```text
External Source
      │
      ▼
Provider Fetch
      │
      ├──► Raw Store ──► Raw Manifest
      │
      ▼
Standard Record DTO
      │
      ▼
Validator
      │
      ├──失败──► Quality Result
      │
      ▼通过
IngestionEnvelope(record + ingestion_id)
      │
      ▼
Persistence ──► Core Facts ──► api_v1 Views ──► PostgREST
```

后续出现派生计算时，在 Core Facts 后增加：

```text
Core Facts → Calculator → Versioned Derived Facts → Statistics
```

Calculator 必须是纯计算，不直接访问数据库或 Raw Store。

## 4. Provider 边界

Provider 按数据集能力拆分接口：

- `SecurityProvider`；
- `TradingCalendarProvider`；
- `DailyBarProvider`；
- `StockDailyIndicatorProvider`；
- `CapitalProvider`；
- `ClassificationProvider`。
- `BoardIndexProvider`。

Provider 同时承担来源适配，输出标准 Record DTO。以下内容必须在 Provider 内完成：

- 来源字段映射；
- `symbol` 转换；
- 日期、时区和枚举转换；
- 价格、成交量、成交额单位转换；
- 缺失值语义转换。

Pipeline、Validator、Persistence 和 API 不允许出现第三方专用字段名。

Provider DTO 不包含 `ingestion_id`。Pipeline 在创建采集批次后，将通过校验的 Record DTO 包装为 `IngestionEnvelope[T]`，附加 `ingestion_id` 后交给 Persistence。

## 5. 标识与时间

- 证券统一标识：`SSE:600000`、`SZSE:000001`、`BSE:920000`；
- A 股统一市场日历标识：`CN_A_SHARE`；
- 时区：`Asia/Shanghai`；
- 交易日使用本地 `date`，不使用 UTC 日期替代；
- 所有审计时间使用带时区时间戳，数据库统一保存 `timestamptz`。

## 6. 存储边界

| Schema | 用途 | 是否直接暴露 PostgREST |
| --- | --- | --- |
| `ingestion` | 采集、Raw 清单和运行记录 | 否 |
| `core` | 标准事实 | 否 |
| `capital` | 股本与公司行为输入事实 | 否 |
| `classification` | 分类目录、成分快照和有效区间 | 否 |
| `derived` | 计算批次、复权行情、日指标和市值 | 否 |
| `metrics` | 分类横截面客观统计 | 否 |
| `audit` | 数据质量和审计 | 否 |
| `api_v1` | 稳定只读 View/RPC | 是 |

上层应用只能依赖 `api_v1`，不能依赖内部表结构。

根据 ADR-0010，研究脚本、看板、回测和 Agent 使用 `api_v1` 稳定 View/RPC；版本化查询必须保持单一 CalculationRun，Agent 工具 Schema 只是 PostgREST RPC 的只读契约。当前不建设 FastAPI 或 MCP。

## 7. 一致性规则

- 相同自然键写入必须幂等；
- 所有 Core 事实必须带 `ingestion_id` 和 `source_code`；
- Raw 对象必须记录 SHA-256；
- 严重校验失败的数据不得进入 Core；
- 生产 Schema 只能通过 migration 修改；
- 删除或改变 API 字段需要新 API 版本或明确弃用窗口。

## 8. 扩展纪律

新增数据源时，先实现现有 Provider 契约和契约测试；新增领域时，先定义边界、事实、自然键、时间语义和 ADR。不得为了未来可能需求提前创建空领域包或空数据库表。

## 9. 已接受、待实现领域

ADR-0012 已接受 `RealtimeQuote` 股票实时五档领域。事实是带观察时间、来源时间和买卖
一至五档的 append-only 快照；它不等于逐笔、十档或交易所 Level-2。当前尚未实现，
首版只允许显式单次采集；在容量与数据授权完成前不得启动持续采集。
