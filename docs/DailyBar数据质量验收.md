# Daily Bar 数据质量验收

本文定义第一阶段 Daily Bar 基线的重复审计和报告归档方式。审计只读取数据库，不修改 Core、Ingestion 或 Audit 表，也不输出数据库凭据和原始行情明细。

## 审计命令

数据库连接必须通过环境变量从密钥管理器注入。日期使用闭区间，生产验收必须显式指定，不使用随时间变化的默认值。

```bash
export DATABASE_URL='从密钥管理器注入的只读连接'

uv run python scripts/audit_daily_bars.py \
  --start-date 2024-07-29 \
  --end-date 2026-07-28 \
  --format markdown \
  --output reports/daily-bar-2024-07-29_2026-07-28.md

uv run python scripts/audit_daily_bars.py \
  --start-date 2024-07-29 \
  --end-date 2026-07-28 \
  --format json \
  --output reports/daily-bar-2024-07-29_2026-07-28.json

unset DATABASE_URL
```

报告文件默认不覆盖已有文件，`reports/` 已被 Git 忽略。将报告、对应 commit SHA、运行时间和文件 SHA-256 保存到受控验收存储或 CI Artifact；不要提交原始行情文件。

## 审计口径

### 覆盖率

- 股票范围来自 `core.security.security_type = 'stock'`；
- 交易日来自 `core.trading_calendar` 的 `CN_A_SHARE` 开市日；
- 上市前和退市后的证券日从应覆盖范围中排除；
- `trade_status = 'suspended'` 的记录是已存在事实，计入覆盖并单独统计；
- 上市区间内缺失的证券日只标为待核对，不能在缺乏来源证据时自动解释为停牌；
- 缺口报告按缺失数量降序列出证券、IPO/退市日期、停牌记录和首末缺失日。

### 事实约束

必须为零：

- `(symbol, trade_date)` 重复自然键；
- `low > high`，或 `open/close` 超出 `[low, high]`；
- 价格、成交量或成交额负值；
- 非交易日 Daily Bar；
- IPO 前或退市后的 Daily Bar。

`trade_status = 'unknown'` 作为警告，不与错误混淆。

### 来源和追溯

报告统计各 `source_code` 行数，并验证：

- 每条事实可以关联 `ingestion.ingestion_run`；
- 事实 `source_code` 与 Run 的 `provider_code` 一致；
- 每个被事实引用的 Run 至少存在一个 `raw_manifest`；
- 同一 Run 的 Raw Manifest 行数合计与 `fetched_rows` 一致；
- 报告列出相关 Run、Manifest 和历史失败质量记录数量。

空事实集、空股票范围、空交易日范围、追溯断链、来源不一致或 Raw 行数不一致会使报告状态成为 `FAIL`。覆盖缺口和未知交易状态使状态成为 `WARNING`。其他情况为 `PASS`。使用 `--fail-on-warning` 可让覆盖警告也返回非零退出码。

## 验收证据

第一阶段正式验收需归档：

1. Markdown 和 JSON 两种报告；
2. 报告文件 SHA-256；
3. 仓库 commit SHA 和 migration versions；
4. 审计日期范围和数据库环境名称；
5. `PASS`，或每个 `WARNING` 的停牌、上市时间、退市时间或来源差异解释；
6. 对应 ingestion run 和 Raw Manifest 的抽查记录。

生产验收不得使用 CI 的样例数据库报告代替。CI 只证明审计查询、边界解释和失败判定可以重复执行。
