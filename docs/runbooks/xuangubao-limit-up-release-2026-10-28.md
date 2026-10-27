# 选股宝最新涨停池API发布验收

项目所有者于2026-10-28明确要求提交、推送和部署，并再次确认继续部署。
任务：[#101](https://github.com/yuexing89757/data_center/issues/101)。
功能提交：`9fe6462660c77ccc67b97c41ac7ca6d13ee7d026`。
上海时间2026-10-28 00:43:51完成生产验收。

## 发布范围与门禁

仅新增`GET /api/v1/realtime/xuangubao/limit-up`，沿用原`X-API-Key`和固定来源实时读取模式。
只发布API；没有生产迁移、采集、Worker重启或计划修改，也没有修改凭据与外置配置。
暂存README、ADR和OpenAPI时排除了工作区原有东财日线`trade_date`变更；
这些原有变更以及未跟踪目录保留，没有随本次发布。

- 原工作区完整本地门禁：格式、Ruff、mypy通过，pytest2174通过234环境跳过。
- 从独立暂存快照核对契约与源码一致，全部既有路径/响应模型保持原样；
  98项选股宝、旧来源及契约测试通过。新接口35项测试包含其中。
- [功能提交CI](https://github.com/yuexing89757/data_center/actions/runs/40542503100)成功：
  quality为2172通过、233跳过，格式611文件、mypy182源码文件通过；
  独立隔离PostgreSQL任务232通过。两任务各有2条警告，无失败。
- [生产只读check](https://github.com/yuexing89757/data_center/actions/runs/40542506391)成功，
  没有运行apply或执行DDL。
- 干净提交副本打包759个跟踪文件；不含未提交变更、凭据、Raw或虚拟环境。
  归档SHA-256：`5284854c06caed73b66bc8b25eb9253f39667f84a318e839d765a1e8b7dbbf00`。
- 2455个既有API Raw文件独立备份、逐文件核验并下载异机验证摘要一致。
  未备份PostgreSQL，遵循宪法第12项；没有保存本次上游涨停池原文。

## 切换与验收

正式目标为既有`market-data-center-api.service`，保留loopback绑定、systemd unit/drop-in
和原API密钥。`/home/project-api`由：

```text
/home/project/market_data_center-0.2.0-cbf3281/market-data-center-0.2.0
```

原子切换到：

```text
/home/project/market_data_center-0.2.0-9fe6462/market-data-center-0.2.0
```

`RELEASE_COMMIT`记录完整功能提交。新旧依赖文件文本一致，新目录复用旧API虚拟环境；
旧版本属于依赖闭包，必须保留。切换前只读角色及RPC权限预检通过。

首次切换的操作验收脚本错误地把健康响应与仅含status的对象比较，忽略既有service/version字段；
健康及就绪实际均为200，但断言触发了自动回退。旧API恢复健康后，修正临时验收脚本，
按既有HealthResponse检查状态和服务标识，再次发布同一提交成功。运行时代码未变。

最终验收：

| 检查 | 结果 |
|---|---|
| API服务 | active，PID2021806，NRestarts=0，进程目录为新版本 |
| /healthz、/readyz | 均200，分别ok、ready |
| 完整/openapi.json | 与发布契约完全一致 |
| 新接口无API Key | 401 |
| 新接口带原API Key | 200，source=xuangubao，42条 |
| 来源日期 | 2026-10-27，date_status=verified_from_events |
| 实际观测时间 | 2026-10-28 00:43:50，Asia/Shanghai |
| 官方API smoke | 只读预检、健康/就绪及有界证券查询均通过 |
| Worker | PID1525586保持不变，未重启 |
| 外置API配置 | 内容校验一致，权限仍为root:600 |

上述为生产本机HTTP真实验收，未额外验证外部网关链路；一次成功不承诺第三方持续可用。
闭市后最新池仍为上一来源日期符合接口约定，不以观测日期覆盖来源日期。

需要回退时仅将API链接切回保留的旧目标并重启API，再重复健康/就绪和只读检查。
没有数据库回滚或Worker变更步骤；后续回退仍按明确运维授权执行。
