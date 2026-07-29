# Worker 日常采集与调度

第一阶段使用 `daily-run` 完成每天的功能闭环，不由 GitHub Actions 调度生产采集。一次运行按固定顺序执行：

1. 同步证券主数据；
2. 同步截至运行日的统一 A 股日历；
3. 找出最近窗口内缺少至少一个有效交易日事实的上市股票；
4. 只对这些股票执行 Daily Bar 采集，并通过自然键幂等写入。

Provider 的原始响应先写入不可变 Raw 对象，再执行 Record DTO 标准化。若标准化失败，采集批次、Raw Manifest 和阻断级 `QualityResult` 会一起落库，Core 不写入该批次，因此错误来源仍可追溯和重放。

默认 Daily Bar 修复窗口为最近 7 个自然日，日历窗口为最近 14 个自然日。周末或节假日运行时，如果窗口内数据已经完整，不会重复调用 Daily Bar Provider。IPO 前和退市后的日期不参与完整性判断。
Daily Bar 请求的结束日取窗口内最近的实际交易日，因此周末和节假日不会把本地 pytdx 的正常休市误判为行情文件过期。

## 手工运行

数据库连接和本地 Raw 根目录由环境变量注入：

```bash
market-data-center daily-run
market-data-center daily-run --as-of-date 2026-07-29
market-data-center daily-run --bar-lookback-days 14 --calendar-lookback-days 30
```

默认使用 ADR-0005 的自动路由。为了可复现诊断，可以显式使用支持全部三个数据集的 `baostock` 或 `akshare`：

```bash
market-data-center --provider baostock daily-run --as-of-date 2026-07-29
```

`pytdx` 只支持 Daily Bar，不能单独承担完整的 `daily-run`；自动路由仍可在 Daily Bar 步骤优先使用本地 pytdx。
北交所代码、本地缺少单只股票文件或请求区间没有本地记录属于“当前请求不可用”，Router 会回退到 BaoStock/AKShare，但不会因此熔断 pytdx；只有 Provider 整体错误才累计连续失败。

首次全量回补继续使用 `daily-bars-bulk`。批量续跑现在按整个有效交易区间判断完整性，不会因为区间内已有一条记录就跳过仍有缺口的股票。
如果请求范围的自然日日历尚未完整同步，批量命令会直接失败并提示先同步日历，不会把缺失日历误判为“没有待采集股票”。局部日历刷新会读取已存范围外最近的前后交易日，保持窗口边界链接连续。

## systemd 部署

仓库提供：

- `deploy/systemd/market-data-center.service`
- `deploy/systemd/market-data-center.timer`

默认安装约定：

- 程序目录：`/opt/market-data-center`；
- Raw 目录：`/var/lib/market-data-center/raw`；
- 环境文件：`/etc/market-data-center/worker.env`；
- 系统身份：`market-data-center`。

`worker.env` 至少配置 `DATABASE_URL` 和 `RAW_DATA_ROOT=/var/lib/market-data-center/raw`。环境文件不得提交到 Git。
`DATABASE_URL` 可以使用 `postgresql://` 或 `postgresql+psycopg://`；程序会在 psycopg 和 SQLAlchemy 边界选择正确的驱动格式。

安装并验证：

```bash
sudo install -m 0644 deploy/systemd/market-data-center.service /etc/systemd/system/
sudo install -m 0644 deploy/systemd/market-data-center.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now market-data-center.timer
systemctl list-timers market-data-center.timer
sudo systemctl start market-data-center.service
journalctl -u market-data-center.service --since today
```

Timer 每天北京时间 18:30 后运行，并设置最多 5 分钟随机延迟。`Persistent=true` 会在主机错过调度后补跑；Pipeline advisory lock 阻止同一 Provider、数据集和证券的并发重复执行。

## 生产 migration 与 smoke check

`.github/workflows/production.yml` 提供人工触发的生产工作流。`check` 只核对 migration 版本、Schema、RLS、数据库事实和 PostgREST；`apply` 先应用尚未执行的 migration，再完成同一组检查。

基础检查要求 Security、Trading Calendar、股票 Daily Bar、Raw 和成功批次非空，并探测当前全部 `api_v1` 视图。完成 THS 初始化后，使用严格模式验证板块定义、日 K 和成分快照也已进入数据库与 API：

```bash
uv run python scripts/smoke_check.py --require-board-index
```

GitHub `production` Environment 需要配置 `MIGRATION_DATABASE_URL`、`DATABASE_URL`、`SUPABASE_URL` 和 `SUPABASE_PUBLISHABLE_KEY`。仓库只引用 Secret 名称，不保存值。Environment 审批、备份确认和凭据轮换可在功能闭环完成后继续加固。
