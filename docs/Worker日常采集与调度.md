# Worker 日常采集与调度

第一阶段使用 `daily-run` 完成每天的行情入库，不由 GitHub Actions 调度生产采集。一次运行按固定顺序执行：

1. 同步证券主数据；
2. 同步截至运行日的统一 A 股日历；
3. 确定截至运行日的最近交易日；
4. 只对该交易日尚无事实的上市股票读取本地 pytdx Daily Bar，并通过自然键幂等写入。

`daily-run` 不回看、不修复历史日 K，也不自动计算 Derived/Metrics。停牌或本地文件在当日没有记录的证券记为 `unavailable`，不会导致整批失败；文件损坏、市场哨兵陈旧、数据库错误仍会使任务失败。

Provider 的原始响应先写入不可变 Raw 对象，再执行 Record DTO 标准化。若标准化失败，采集批次、Raw Manifest 和阻断级 `QualityResult` 会一起落库，Core 不写入该批次，因此错误来源仍可追溯和重放。

默认日历窗口为最近 14 个自然日。周末或节假日运行时取窗口内最近的实际交易日，因此不会把本地 pytdx 的正常休市误判为行情文件过期。兼容参数 `--bar-lookback-days` 仅保留命令行兼容性，不再触发历史修复。

## 手工运行

数据库连接和本地 Raw 根目录由环境变量注入：

```bash
market-data-center daily-run
market-data-center daily-run --as-of-date 2026-07-29
market-data-center daily-run --calendar-lookback-days 30
```

默认使用 ADR-0005 的自动路由。为了可复现诊断，可以显式使用支持全部三个数据集的 `baostock` 或 `akshare`：

```bash
market-data-center --provider baostock daily-run --as-of-date 2026-07-29
```

`pytdx` 只支持 Daily Bar，不能单独承担完整的 `daily-run`；自动模式在 Daily Bar 步骤固定使用本地 pytdx。个股停牌、本地文件未更新或文件缺失产生的缺口保持可见，不通过 BaoStock/AKShare 补数。

pytdx 还可从通达信 `T0002/hq_cache` 读取行业和概念完整快照，但 Security 和 Trading Calendar 仍由 BaoStock/AKShare 提供。分类成员引用尚未进入 Security 的代码时，整个成员快照按质量规则阻断，不能静默删掉未知成员。

`daily-bars-bulk` 仍保留为显式人工工具，但不进入日常调度。当前项目决策是不再补历史日 K；除非项目所有者再次明确要求，不运行该命令。

## Windows 任务计划部署

pytdx 读取 `D:\new_tdx64` 的本地通达信目录，因此生产采集 Worker 必须运行在能访问该目录的 Windows 主机。旧 Linux systemd 单元已删除，仓库提供：

- `deploy/windows/run-daily.ps1`
- `deploy/windows/register-daily-task.ps1`

项目根目录的未提交 `.env` 至少配置 `DATABASE_URL`、`RAW_DATA_ROOT` 和 `PYTDX_VIPDOC_PATH`。先安装依赖，再注册每天 18:30 的任务：

```powershell
uv sync --all-groups
powershell -ExecutionPolicy Bypass -File deploy/windows/register-daily-task.ps1
Start-ScheduledTask -TaskName MarketDataCenter-Daily
Get-ScheduledTaskInfo -TaskName MarketDataCenter-Daily
```

任务每天本地时间 18:30 运行，错过计划时在主机恢复后尽快启动，最长运行 4 小时。Pipeline advisory lock 阻止同一 Provider、数据集和证券的并发重复执行。通达信客户端仍负责在任务开始前更新本地行情文件。

## 生产 migration 与 smoke check

`.github/workflows/production.yml` 提供人工触发的生产工作流。`check` 只核对 migration 版本、Schema、RLS、数据库事实和 PostgREST；`apply` 先应用尚未执行的 migration，再完成同一组检查。

基础检查要求 Security、Trading Calendar、股票 Daily Bar、Raw 和成功批次非空，并探测当前全部 `api_v1` 视图。完成 THS 初始化后，使用严格模式验证板块定义、日 K 和成分快照也已进入数据库与 API：

```bash
uv run python scripts/smoke_check.py --require-board-index
```

GitHub `production` Environment 需要配置 `MIGRATION_DATABASE_URL`、`DATABASE_URL`、`SUPABASE_URL` 和 `SUPABASE_PUBLISHABLE_KEY`。仓库只引用 Secret 名称，不保存值。Environment 审批、备份确认和凭据轮换可在功能闭环完成后继续加固。
