# 股票五日均量生产发布与单日补算验收

关联 [Issue #103](https://github.com/yuexing89757/data_center/issues/103)、ADR-0076。
所有者明确要求“上线入库”，并限定历史补算仅执行 2026-11-03 一次。
运行提交为 `b8cba2b3773806dbca8a8f2deeb741e498b31ab0`，包含功能提交 `080063a`。

## 发布门禁与迁移

- 本地格式、Ruff、mypy 通过；全量 pytest 为 2244 passed、239 skipped。
- 同一运行提交的 [GitHub CI](https://github.com/yuexing89757/data_center/actions/runs/41811253242)
  成功：quality 为 2245 passed、238 skipped；隔离 PostgreSQL 集成为 237 passed。
  本地跳过的五项新增 PostgreSQL 测试已在真实隔离数据库执行。
- [生产基线只读检查](https://github.com/yuexing89757/data_center/actions/runs/40852531430)
  的第二次执行成功；模式为 check。
- [受保护生产迁移](https://github.com/yuexing89757/data_center/actions/runs/41815003118)
  以 apply 模式成功，应用 `20261104000100_stock_volume_average_5d.sql`，
  后续 migration/schema 与数据库/PostgREST smoke 均成功。未执行临时 DDL。
- 使用干净的 Gitee 提交副本打包。源码归档 SHA256：
  `72d5d999a2a6a52db4266ec2bc51acdcc8f5c2430451babe46c84d534f98ce67`。
- 生产目标与先前只读预览的数据库实例指纹一致。Worker 使用无超级用户、建库、建角色或
  bypass-RLS 权限的 `market_data_worker_login`，继承 `market_data_worker` 权限。

本次按当时有效的宪法 v7 校验全部 204618 个在线 Raw 文件（4017510199 字节）：
77056 个由已验证独立基线覆盖；127562 个增量文件复制到异机并逐文件核验大小和 SHA256。
增量归档为 2134139643 字节，SHA256：
`357235a29ab86867089cdbfa1705680dc740346fde76560195d3c12270e79d09`。
异机副本验证后移除服务器临时传输归档，保留独立副本和原始 Raw。没有备份 PostgreSQL。

所有者随后明确规定未来常规线上部署不需要备份 Raw，已写入宪法 v8 第12项原则。
以上备份是本次已发生的历史事实，不构成未来部署门禁。

## Worker 上线

上海时间 2026-11-04 18:36:20 完成 Worker 切换。依赖锁与原运行版本一致，
新源码复用原版本虚拟环境；原 `892829c` 版本属于依赖闭包及回退目标，必须保留。
切换前无 running 工作流或采集；停止后确认旧 PID 和单例锁均释放，Raw 清单未变化。
随后启动单一新 Worker，外置环境与 systemd 配置内容均保持一致。

- Worker PID 为 208710，NRestarts 为 0，健康检查通过，数据库 scheduler 单例锁为 1。
- 任务页只监听 `127.0.0.1:8765`，已注册 24 项启用任务。
- 新任务 `stock-volume-average-5d-daily` 已注册；下一次计划为上海时间
  `2026-11-04 21:45:00`，之后按周一至周五 21:45 的代码目录执行。
- API 版本、链接和 PID 保持不变；`/healthz`、`/readyz` 均通过。
- 没有新增 OS 采集计划，没有手工补采来源日 K，没有调整每日任务为“算昨日”。

本记录验证了任务注册及手工计算链路；验收时首个后续定时批次尚未到达计划时间。

## 2026-11-03 实际入库

仅运行一次 `stock-volume-average-5d-build --trade-date 2026-11-03`。
上海时间 18:40:08 完成回读校验，18:40:17 完成最终服务验收。

| 项目 | 实际结果 |
|---|---|
| 结果表 | `derived.stock_volume_average_5d` |
| 发布表 | `derived.stock_volume_average_5d_snapshot` |
| 目标日期 / 发布版本 | 2026-11-03 / 1 |
| 计算批次 | `66c80dff-1676-4fab-afd5-880fbe7ef79c` |
| 算法版本 | 1.0.0 |
| 五日窗口 | 2026-10-28、10-29、10-30、11-02、11-03 |
| 全部股票结果 | 5578 条，证券键唯一 |
| 完整均量 | 5223 条，逐条以输入五日整数股数之和除以5重新核对 |
| 均量为空 | 355 条，保留缺失原因 |
| 缺失原因 | missing_daily_bar 354；insufficient_listing_history 2，原因可重叠 |
| Operations | manual；WorkflowRun 与 JobExecution 均为 partial；accepted 5223、rejected 355 |

输入哈希与获准的只读预览完全相同：
`9e23669a011caef21d05cb9976a38fabb13264b055872f0b7676901eaefcb2f4`。
计算批次成功、输出5578条；Operations 的 partial 如实反映355只输入不完整，未补零或改用更早日 K。
两张新表只存在 2026-11-03 的发布结果，没有扩展补算日期范围。

生产检查确认两表 RLS 开启，API、anon、authenticated 均无底表 SELECT 权限；
Worker 无结果表 UPDATE/DELETE 权限。未新增公共 API 契约。
最终 Worker PID 未变化且无异常重启，API PID 未变化，健康和就绪检查通过。
