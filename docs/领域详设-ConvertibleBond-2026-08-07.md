# 领域详设：可转债（ConvertibleBond）

> 状态：设计稿（待 ADR-0023 接受后实施）
> 日期：2026-08-07
> 关联 ADR：ADR-0023-可转债领域（待创建）
> 依据：宪法 v5、AGENTS.md、ADR-0001/0005/0006/0010/0013

## 1. 边界与目标

可转债（Convertible Bond）是 A 股市场具备"债底 + 股性"的混合证券。本领域负责采集和持久化可转债的**客观事实**：基础条款、每日行情、转股价调整历史、赎回/回售事件。

**不进入本领域**（属于消费方）：转债策略、套利信号、条款博弈判断、回测。

### 1.1 首版范围（完整集）

| 事实 | 表 | 来源 | 自然键 |
|---|---|---|---|
| 可转债基础条款 | `convertible_bond.bond` | Tushare `cb_basic` + `cb_issue` | `symbol` |
| 可转债每日行情 | `convertible_bond.daily_bar` | Tushare `cb_daily`（主）/ pytdx（备）/ AKShare（补） | `(symbol, trade_date)` |
| 转股价调整历史 | `convertible_bond.convert_price_revision` | Tushare `cb_share` | `(symbol, effective_date)` |
| 赎回/回售事件 | `convertible_bond.call_event` | Tushare `cb_issue` 条款 + AKShare `bond_cb_redeem_js` | `(symbol, event_type, announcement_date)` |

### 1.2 与现有领域的关系

- **复用 `core.security`**：可转债以 `security_type='convertible_bond'` 存在（枚举已预留）。`list_date` → `core.security.ipo_date`，`delist_date` → `core.security.delisting_date`。
- **不复用 `core.daily_bar`**：可转债行情字段集不同（含转股价值/溢价率/剩余规模；volume 单位是"张"不是"股"；无 is_st）。独立表。
- **正股关联**：`underlying_symbol` 外键指向 `core.security` 中一条 `security_type='stock'` 的记录。
- **转股对正股股本的影响**：由 `capital.share_capital` 权威记录（`change_reason='convertible_bond_conversion'`），本领域不重复维护正股股本。

## 2. Schema 设计

新建独立 schema `convertible_bond`：

```sql
create schema if not exists convertible_bond;
revoke all on schema convertible_bond from public;
grant usage on schema convertible_bond to market_data_worker;
```

扩展 `ingestion.ingestion_run.dataset_code` 和 `audit.quality_result.dataset_code` 的 check 约束，新增：`convertible_bond`、`convertible_bond_daily_bar`。

### 2.1 `convertible_bond.bond` — 基础条款（1:1 快照）

| 字段 | 类型 | 说明 |
|---|---|---|
| `symbol` | text PK | 转债标识 `SSE:113527`，FK→core.security |
| `bond_code` | text | 转债代码（保前导零），如 `113527` |
| `bond_short_name` | text | 简称，如 `中信转债` |
| `bond_full_name` | text | 全称 |
| `underlying_symbol` | text FK→core.security | 正股标识，如 `SSE:600030` |
| `exchange` | text | `SSE`/`SZSE` |
| `par_value` | numeric(18,4) | 每张面值（固定 100） |
| `issue_size` | numeric(24,2) | 发行总额（元） |
| `issue_date` | date | 发行日 |
| `value_date` | date | 起息日 |
| `maturity_years` | smallint | 期限（年，通常 5-6） |
| `maturity_date` | date | 到期日 |
| `convert_price_initial` | numeric(18,4) | 初始转股价 |
| `convert_price` | numeric(18,4) | 当前转股价（最新快照） |
| `convert_start_date` | date | 转股起始日 |
| `convert_end_date` | date | 转股截止日 |
| `coupon_rate` | numeric(24,10) | 代表性票面利率（2000 积分） |
| `redeem_clause` | text | 赎回条款全文 |
| `sell_back_clause` | text | 回售条款全文 |
| `lifecycle_status` | text | `pending_list`/`listed`/`in_conversion`/`called`/`matured`/`delisted` |
| `source_code` | text | `tushare` |
| `ingestion_id` | uuid FK→ingestion_run | |
| `created_at`/`updated_at` | timestamptz | |

**约束**：`par_value > 0`；`issue_size >= 0`；`convert_price_initial > 0`；`maturity_years > 0`；`convert_end_date >= convert_start_date`；`bond_code ~ '^[0-9]{6}$'`。

**覆盖语义**：`on conflict (symbol) do update set ...`——最新批次覆盖（幂等）。

### 2.2 `convertible_bond.daily_bar` — 每日行情

| 字段 | 类型 | 说明 |
|---|---|---|
| `symbol` | text PK 组成 FK→core.security | |
| `trade_date` | date PK 组成 | |
| `market` | text | 固定 `CN_A_SHARE`，FK→trading_calendar |
| `open`/`high`/`low`/`close`/`previous_close` | numeric(18,4) | OHLC（元） |
| `volume` | bigint | 成交量（**单位：张**，非股） |
| `amount` | numeric(24,2) | 成交额（元） |
| `pct_chg` | numeric(24,10) | 涨跌幅（%） |
| `convert_value` | numeric(18,4) | 转股价值 = 正股价/转股价×100 |
| `convert_premium_pct` | numeric(24,10) | 转股溢价率（%） |
| `convert_price` | numeric(18,4) | 当日转股价（冗余便于按日查询） |
| `remain_size` | numeric(24,2) | 当日剩余规模（元） |
| `trade_status` | text | `trading`/`suspended`/`halted_limit`/`unknown` |
| `source_code` | text | `tushare`/`pytdx`/`akshare` |
| `ingestion_id` | uuid FK | |
| `created_at`/`updated_at` | timestamptz | |

**约束**：OHLC 非负；`low <= high`；`volume >= 0`；`amount >= 0`；`remain_size >= 0`。
**触发器**：复用 `core.ensure_daily_bar_trading_day`（须交易日）。
**覆盖语义**：`on conflict (symbol, trade_date) do update set ...`。

> pytdx 来源只填 OHLCV + trade_status=unknown，转股价值等债券字段为 null（由 tushare 补）。

### 2.3 `convertible_bond.convert_price_revision` — 转股价调整历史

| 字段 | 类型 | 说明 |
|---|---|---|
| `symbol` | text PK 组成 FK→core.security | |
| `effective_date` | date PK 组成 | 调整生效日 |
| `convert_price_before` | numeric(18,4) | 调整前转股价 |
| `convert_price_after` | numeric(18,4) | 调整后转股价 |
| `revision_reason` | text | `dividend`/`bonus_share`/`rights_issue`/`downward_revision`/`other` |
| `announcement_date` | date | 公告日 |
| `source_code`/`ingestion_id`/`created_at` | | |

**约束**：`convert_price_after > 0`；`convert_price_before is null or convert_price_before > 0`。

### 2.4 `convertible_bond.call_event` — 赎回/回售事件

| 字段 | 类型 | 说明 |
|---|---|---|
| `symbol` | text PK 组成 FK→core.security | |
| `event_type` | text PK 组成 | `forced_redemption`/`sell_back`/`maturity_redemption` |
| `announcement_date` | date PK 组成 | 公告日 |
| `trigger_date` | date | 触发日 |
| `record_date` | date | 登记日 |
| `call_price` | numeric(18,4) | 赎回/回售价（元/张） |
| `status` | text | `announced`/`executed`/`cancelled` |
| `source_code`/`ingestion_id`/`created_at` | | |

**约束**：`call_price is null or call_price >= 0`。

## 3. 领域 Record（`domain/convertible_bond.py`）

采用 Classification/DeductedProfit 风格：Record + 校验 + 自然键同文件。所有 Record `@dataclass(frozen=True, slots=True)`，价格/比例用 `Decimal`，绝不用 float。

```python
type ConvertibleBondRecord = (
    ConvertibleBondBasicRecord
    | ConvertibleBondDailyBarRecord
    | ConvertibleBondConvertPriceRevisionRecord
    | ConvertibleBondCallEventRecord
)
```

### 自然键

- `bond`：`(symbol,)` — 单字段
- `daily_bar`：`(symbol, trade_date)`
- `convert_price_revision`：`(symbol, effective_date)`
- `call_event`：`(symbol, event_type, announcement_date)`

`source_code`/`ingestion_id` 不参与自然键（同 capital）。

### 校验（`validate_convertible_bond`）

仿 `validate_capital`：按自然键分组 → 未知 symbol 产生 `convertible_bond.unknown_symbol` → 同键不同值产生 `convertible_bond.conflicting_duplicate` 整组拒绝。

## 4. 数据源与 Provider 适配

### 4.1 Tushare（主源，2000 积分）

| 接口 | 字段 | 用途 |
|---|---|---|
| `cb_basic` | ts_code, bond_id, bond_short_name, bond_full_name, list_date, delist_date, maturity_date, par, issue_size, value_date, maturity, convert_price_initial, convert_price, stock_ts_code, redeem_clause, sell_back_clause | → bond 表 |
| `cb_issue` | issue_date, online_offline_date, win_date | → bond 表补充 |
| `cb_daily` | trade_date, pre_close, open, high, low, close, pct_chg, vol, amount, convert_value, convert_pct, convert_price, remain_size | → daily_bar 表 |
| `cb_share` | publish_date, end_date, convert_price, convert_vol, acc_convert_vol, remain_size, total_shares | → convert_price_revision + bond 转股结果快照 |

**字段映射要点**（adapter 内完成）：
- `ts_code`（`113527.SH`）→ 标准 `symbol`（`SSE:113527`）
- `stock_ts_code`（`600030.SH`）→ `underlying_symbol`（`SSE:600030`）
- `vol`（手）→ `volume`（张，1 手 = 10 张，需 ×10）
- `pct_chg`/`convert_pct` 保持 Decimal
- `schema_version = "tushare.convertible_bond.v1"`

### 4.2 pytdx（备选，OHLCV only）

本段关于本地 `vipdoc` 的兼容性记录已被 ADR-0024 取代。远程 TDX 普通 Daily Bar 路径
仍不承担可转债领域采集；可转债继续使用其专用 Provider/命令。
- 只产出 OHLCV，转股价值/溢价率/剩余规模为 null
- `trade_status` = `unknown`（.day 文件无此字段）
- `volume` 单位语义为"张"（adapter 不换算，保持源语义）
- **不作为可转债日K的主路由**，仅作 Tushare 不可用时的备选

### 4.3 AKShare（补充源）

`bond_zh_cov`（东财一览表）提供纯债价值/YTM 等补充字段；`bond_cb_redeem_js` 提供强赎事件。作为 Tushare 的交叉校验和字段补充，非主源。

### 4.4 路由（`router.py`）

```python
DatasetCode.CONVERTIBLE_BOND: ("tushare",),
DatasetCode.CONVERTIBLE_BOND_DAILY_BAR: ("tushare",),  # pytdx 不进自动路由
```

## 5. 持久化（`persistence/postgres.py`）

仿 `commit_capital_batch`：
- `commit_convertible_bond_batch(run, manifest, records, quality_results)` 单事务
- 按子类型 dispatch 到 4 个 UPSERT（`on conflict (natural_key) do update set ...`）
- `_ensure_envelope_ids` 联合加 `ConvertibleBondRecord`
- `PipelinePersistence` Protocol 加抽象签名

## 6. CLI 与调度

### CLI

```
market-data-center convertible-bond                    # 全量同步基础条款
market-data-center convertible-bond-daily-bar --source-symbol SSE:113527 --start-date ... --end-date ...
market-data-center convertible-bond-daily-bars-bulk --start-date ... --end-date ...
```

显式 `--provider tushare|pytdx` 绕过路由。

### 调度（`scheduling_catalog.py`）

新增 job `convertible-bond-daily`（cron 周一至周五，时间可配 `CONVERTIBLE_BOND_HOUR`/`_MINUTE`，建议 18:35，紧跟 daily-run 之后），workflow `convertible_bond` 步骤：`convertible_bond_basic → convertible_bond_daily_bar → convertible_bond_convert_price_revision`。

## 7. 公共查询契约（api_v1）

| 视图/RPC | 说明 |
|---|---|
| `api_v1.convertible_bonds` | 基础条款视图（剥离 source_code/ingestion_id） |
| `api_v1.convertible_bond_daily_bars` | 每日行情视图 |
| `api_v1.query_convertible_bond_daily_bars(symbol, date_range, limit)` | 有界 RPC（5 秒超时，≤5000 行） |

同步 `contracts/postgrest-openapi-v1.json`、`contracts/agent-tools-v1.json`。

## 8. 代码段约定（校验参考）

- 沪市可转债：`113xxx`（主流）、`110xxx`（早期）
- 深市可转债：`123xxx`（创业板）、`128xxx`（主板）
- **公募可交债（118xxx/117xxx）不纳入首版**（业务不同，未来单列 `security_type='exchangeable_bond'`）

## 9. 实施清单（ADR 接受后）

1. **ADR-0023**（决策：范围、schema、来源、覆盖语义、不复用 daily_bar）
2. **migration** `20260807XXXXX_create_convertible_bond.sql`（4 表 + schema + check 扩展 + 索引 + 触发器 + RLS + api_v1 视图 + 授权）
3. **domain/convertible_bond.py**（4 Record + 联合类型 + 自然键 + 校验）
4. **providers/tushare.py**（cb_basic/cb_issue/cb_daily/cb_share 适配 + Raw 重放）
5. **persistence/postgres.py**（commit_convertible_bond_batch + UPSERT）
6. **pipeline.py**（ingest_convertible_bond）
7. **cli.py**（3 子命令）
8. **scheduling_catalog.py + settings.py + scheduler.py**（job 注册）
9. **测试**（领域校验、provider mock、pipeline、PG 集成、CLI、契约）
10. **文档同步**（领域模型总纲、数据库导航、ADR README）
