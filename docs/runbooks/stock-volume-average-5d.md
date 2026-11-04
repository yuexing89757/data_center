# 股票五日均量（包含当日）

依据 ADR-0076 与 [Issue #103](https://github.com/yuexing89757/data_center/issues/103)。
本地实现不代表生产数据库已建表或任务已启用。

## 日常维护

受保护流程应用 `20261104000100_stock_volume_average_5d.sql` 后，由既有常驻
`market-data-center worker` 的 APScheduler 注册 `stock-volume-average-5d-daily`。
代码固定周一至周五上海时间 21:45；默认启用，可以通过
`STOCK_VOLUME_AVERAGE_5D_ENABLED=false` 停用，不通过环境变量改执行时间。
操作系统只负责守护 Worker，不注册其他采集计划任务。

休市成功跳过；日历未知、五日窗口中间缺日历、股票生命周期未知或同日
daily_market 未达到 succeeded/partial 终态时失败。21:45 是计划时间，不承诺输入已就绪。
任务不访问网络、不补采日 K，也不自动回填历史。失败后按精确交易日获准重算。

算法 1.0.0：当日及此前四个 CN_A_SHARE 市场交易日的已认证整数成交股数之和 ÷ 5。
单位股，最多一位小数；例如 100、200、300、400、501 股均量为 300.2 股。
真实零可参与；缺日、null、未认证量、停牌和上市不足五日保留 null 及原因。
仅判断 daily_bar.volume 认证；金额未认证不影响已认证成交股数。

Worker 本地只读任务页和 Operations 显示工作流 `stock_volume_average_5d`、
步骤 `build_stock_volume_average_5d`。只要任一股票均量为空，执行统计为 partial，
accepted_rows 表示完整股票数，rejected_rows 表示不完整股票数；不把部分均值冒充完整结果。

## 表与版本

- `derived.stock_volume_average_5d`：股票/日期/计算批次结果、覆盖数、缺失原因和受限输入证据。
- `derived.stock_volume_average_5d_snapshot`：同交易日递增的不可变发布版本及当次认证证据/水位。
- `derived.calculation_run`：算法、输入哈希、水位、计算时间和成功/失败状态。

排查时先按精确 trade_date 找发布表最大 version，再用它的 calculation_id 读取该日全部结果。
不能按股票分别挑最新版，也不能用最近非空日期替代请求日期。
同输入、同认证证据且当前发布相同返回 unchanged；输入 A→B→A 时复用旧 A 的结果并追加发布版本，
返回 published/reused，保留原计算时刻。旧版本不覆盖，失败不移动当前发布。
数值相同但来源认证被替换时同样追加发布版本，并保存本次的新来源证明；
审计当前发布的准入依据应读取发布表 input_evidence，结果表保留原计算当时的证据。
新表首期不加入自动归档清理白名单，不开放底表给 API 或客户端角色。
本期没有新增公共查询接口，消费者不得直接依赖 derived；公共读取需后续有界 api_v1 契约。

## 精确日期重算

只有获准执行该日期的维护时，才运行：

```powershell
uv run market-data-center stock-volume-average-5d-build --trade-date 2026-11-04
```

该命令会写计算批次、均量结果、发布版本和 Operations，不是只读诊断。
CLI 与定时任务复用相同计算服务、单位门禁、日期锁和原子发布，不触发来源采集。
结果状态可为 succeeded、unchanged、published/reused、skipped；输出明确目标日期和版本。
最初功能验收仅执行本地开发；补算与部署以之后的明确授权和实际执行记录为准。

## 2026-11-04 单日补算前只读检查

所有者要求只补算2026-11-03一次，不改变定时任务。只读核验目标日为交易日，
已有5228条日 K，但目标库尚无两张新派生表；未执行建表或写入补算。
全市场一次传入5578个股票键时，查询计划选择daily_bar全表扫描，试算触发45秒超时。
读取已改为同一Repeatable Read快照内每批最多100个证券；100键计划使用主键位图索引，
分页回归覆盖100/100/5的完整证券键及同一连接，不改变任何算法或单位门禁。
真实全市场试算与最终补算状态以Issue #103的后续记录为准，不能将只读预览称为已入库。

分页后2026-11-03的只读试算已完成：窗口为10月28、29、30日及11月2、3日，
5578只候选中5223只得到完整五日均量，355只保持null；354只有日 K 缺口，
2只上市不足五日，原因可重叠。结果导出为本地JSON预览，没有数据库写入。
目标表仍未创建，实际入库须先明确授权通过受保护流程应用迁移；只补算该日的请求
不改变原定时任务，也不自动扩展为全历史重算或Worker部署。

## 验证

聚焦测试：

```powershell
uv run pytest tests/test_stock_volume_average.py tests/test_stock_volume_average_service.py tests/test_stock_volume_average_worker.py tests/test_stock_volume_average_postgres.py
```

数据库测试仅连接隔离 TEST_DATABASE_URL；覆盖真实迁移、独立量认证、A→B→A发布、
原子回滚、RLS/最小权限和结果完整性约束。未配置时跳过，不能宣称集成验证通过。
完整本地门禁仍为 Ruff 格式/静态检查、mypy src 和 pytest；生产状态须单独验收。

2026-11-04 本地验收：`uv run ruff format --check .`、`uv run ruff check .`、
`uv run mypy src` 均通过；分页修复后 `uv run pytest` 为 2244 passed、239 skipped。
分页修复前单独 `uv run pytest -m integration` 为 1 passed、236 skipped、2245 deselected；
未设置 TEST_DATABASE_URL，新增五项 PostgreSQL 测试均跳过，未验证真实数据库迁移。
独立代码复核发现的认证替换证据丢失问题已由先失败后通过的回归测试验证修复。
未执行生产迁移、重算、采集、部署或推送。
