# 领域详设：BoardMoneyflow

- 状态：已接受，已实现
- 日期：2026-10-06
- ADR：ADR-0065（前置 ADR-0047、ADR-0064）

## 1. 边界与依赖

`BoardMoneyflow` 表达东方财富板块（概念/行业/地域）在某交易日的资金流向统计。它依赖
Ingestion，不依赖 Security、Trading Calendar 或板块主数据；板块标识自包含于本域。

```text
Ingestion ────────► BoardMoneyflow
```

首版不包含个股资金流、板块成分、板块指数行情或同花顺板块资金流。

## 2. 领域记录

```python
@dataclass(frozen=True, slots=True)
class BoardMoneyflowRecord:
    board_code: str  # DC:BK0145
    trade_date: date
    board_type: BoardMoneyflowType  # concept | industry | region
    name: str  # 公告日名称快照
    pct_change: Decimal  # 涨跌幅 %
    close: Decimal | None  # 板块收盘点位
    net_amount: Decimal  # 主力净流入额（元，可负）
    net_amount_rate: Decimal | None  # 主力净流入率 %
    extra_large_amount: Decimal | None  # 超大单净流入额（元）
    extra_large_rate: Decimal | None
    large_amount: Decimal | None  # 大单
    large_rate: Decimal | None
    medium_amount: Decimal | None  # 中单
    medium_rate: Decimal | None
    small_amount: Decimal | None  # 小单
    small_rate: Decimal | None
    top_small_buy_stock: str | None  # 小单净流入最大成分股
    rank: int | None  # 当日排名（>=1）
    source_code: str
```

标准记录不包含 `ingestion_id`。Pipeline 通过 `IngestionEnvelope` 附加采集血缘，
Persistence 首次插入时生成 `first_observed_at`。

不变量：

- `board_code` 匹配 `DC:BK[0-9]{4}`；
- `board_type` 属于枚举 concept/industry/region；
- `name` 非空白；
- `pct_change`、`net_amount` 为 Decimal（可负可零，零与缺失语义不同）；
- `rank` 为空或 >= 1；
- 首期 `source_code = 'tushare'`；
- 批内 `(trade_date, board_code)` 唯一，冲突对整体隔离为
  `board_moneyflow.invalid_record`（ERROR/FAILED），不静默择一。

## 3. Provider 与 Raw

Tushare `moneyflow_ind_dc`（经 `TUSHARE_ENDPOINT` 配置的兼容端点）按 `trade_date`
单日全市场查询，18 列映射：

| 来源字段 | 标准字段 |
| --- | --- |
| `trade_date` | `trade_date` |
| `content_type`（概念/行业/地域） | `board_type` 枚举 |
| `ts_code`（BK0145.DC） | `board_code`（DC:BK0145） |
| `name` | `name` |
| `pct_change` | `pct_change` |
| `close` | `close` |
| `net_amount` | `net_amount` |
| `net_amount_rate` | `net_amount_rate` |
| `buy_elg_amount`/`buy_elg_amount_rate` | `extra_large_amount`/`extra_large_rate` |
| `buy_lg_amount`/`buy_lg_amount_rate` | `large_amount`/`large_rate` |
| `buy_md_amount`/`buy_md_amount_rate` | `medium_amount`/`medium_rate` |
| `buy_sm_amount`/`buy_sm_amount_rate` | `small_amount`/`small_rate` |
| `buy_sm_amount_stock` | `top_small_buy_stock` |
| `rank` | `rank` |

`pct_change` 或 `net_amount` 缺失的行完整保留在 Raw，不生成 Core 记录，不使批次失败；
其余金额/比率/排名/成分股字段允许缺失。行日期与请求不一致、未知 `content_type`、
非 `BK*.DC` 代码抛 ProviderError 硬失败。数值经 Decimal 解析（中转站 JSON float 与
字符串均兼容）。Raw schema 固定为 `tushare.board_moneyflow.v1`；每个实际请求各保存
一份 JSONL Raw、SHA-256、请求参数和 manifest；Token 不属于请求参数。

单日响应约 1031 行，上限 6000 行；满额视为可能截断并显式失败（首期实际不会触达）。
合法空响应（非交易日）成功并产生零记录。请求节奏复用股东接口限速器
（`TUSHARE_SHAREHOLDER_COUNT_MAX_CALLS_PER_MINUTE`，默认 180，共享计数与 keep-alive
连接）。

## 4. 采集应用服务

每日同步以执行日及此前 4 个自然日（共 5 日）为窗口逐日请求，使盘后修正可见。手动
补采 `market-data-center board-moneyflow --start-date --end-date` 使用同一
`sync_range`，默认窗口为当日回溯 5 日；`--end-date` 不得早于 `--start-date`，也不得
晚于当前上海自然日。

每个真实请求分别创建 IngestionRun 和 Raw manifest；窗口内全部请求准备完成后在一个
数据库事务中发布 manifests、质量结果、全部事实并终结运行。任一请求失败时已准备批次
统一置 `failed`、登记 Raw 与 `board_moneyflow.batch_aborted`（ERROR/FAILED）但不写
Core。汇总（`BoardMoneyflowSyncSummary`）校验计数非负、
`accepted + rejected <= fetched`。

## 5. 调度与开关

工作日 17:00 Asia/Shanghai 运行 Job `board-moneyflow-daily`
（`day_of_week='mon-fri', hour=17, minute=0`），Workflow `board_moneyflow_daily`，
step `collect_board_moneyflow`。开关 `BOARD_MONEYFLOW_DAILY_ENABLED` 默认 false，
部署后显式启用。所有调度只在 Worker 进程内，不注册任何操作系统级触发器。

## 6. 存储

`core.board_moneyflow` 字段：

```text
trade_date date
board_code text
board_type text
name text
pct_change numeric(10, 4)
close numeric(20, 4)
net_amount numeric(20, 2)
net_amount_rate numeric(10, 4)
extra_large_amount numeric(20, 2)
extra_large_rate numeric(10, 4)
large_amount numeric(20, 2)
large_rate numeric(10, 4)
medium_amount numeric(20, 2)
medium_rate numeric(10, 4)
small_amount numeric(20, 2)
small_rate numeric(10, 4)
top_small_buy_stock text
rank integer
source_code text
ingestion_id uuid
first_observed_at timestamptz
```

主键为 `(trade_date, board_code)`。CHECK 约束镜像领域不变量（代码格式、类型枚举、
非空名称、名次为空或 >=1、`source_code='tushare'`）。表启用 RLS；Worker 只有
SELECT/INSERT；持久化对主键冲突 `ON CONFLICT DO NOTHING`，追加而不覆盖。索引为：

```text
(board_code, trade_date desc)
(ingestion_id)
```

## 7. 公共查询

首期不新增 `api_v1` 视图或 RPC。公开只读契约待消费方分析需求明确后按宪法流程另行
决策；在此之前 `core.board_moneyflow` 不是稳定公共契约。

## 8. 质量与测试

测试覆盖领域不变量与冲突对隔离、来源字段/类型/空值映射与 Raw 重放一致、未知类型/
日期不匹配/非法代码硬失败、每周窗口与手动 CLI、多请求单事务发布、自然键/manifest/
信封预检、abort 不发布事实、Scheduler/Operations、数据库约束/RLS/最小授权清单与
追加幂等（重复批次不重复不覆盖）。

实施不执行生产 migration、生产采集或凭据变更。PostgreSQL integration 只使用隔离的
`TEST_DATABASE_URL`。
