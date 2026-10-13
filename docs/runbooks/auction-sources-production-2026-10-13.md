# 竞价九合一来源API生产发布记录

## 授权和版本

- 项目所有者于2026-10-13明确要求“data_center 部署生产”。
- 生产验收时间：2026-10-13 10:14，Asia/Shanghai。
- 功能提交：`34d74a2702ef77a3ced7fdf58784a8353eac24e1`。
- 任务：[#89](https://github.com/yuexing89757/data_center/issues/89)；决策：[ADR-0067](../adr/ADR-0067-竞价九合一来源实时只读接口.md)。
- 发布范围为12类实时来源API及其契约；只切换API，不重启Worker，不运行生产迁移或采集。

## 发布门禁

本地当次格式、ruff、mypy通过，接口及契约59项测试通过；此前完整本地1605通过、153环境跳过。
[功能提交的CI](https://github.com/yuexing89757/data_center/actions/runs/38335379183)两个任务均成功：

- quality：1608 passed、150 skipped；跳过的数据库测试在独立任务运行。
- migrations：隔离PostgreSQL 150 passed，未连接生产库执行测试。

从独立干净clone打包638个Git跟踪文件，未加入本地未跟踪目录、配置、凭据或Raw。
归档：`market-data-center-0.2.0-linux.tar.gz`。
SHA256：`d724aeff616c33870270b142e66f91151627e0e023d05b07e7037340f8bc2d56`。
上传后同时核对校验文件与本地预期摘要，检查归档路径和文件类型后解压。

## 生产切换

API链接`/home/project-api`从：

```text
/home/project/market_data_center-0.2.0-f2f004a/market-data-center-0.2.0
```

原子切换至：

```text
/home/project/market_data_center-0.2.0-34d74a2/market-data-center-0.2.0
```

新版本写入RELEASE_COMMIT，值为上述完整功能提交。uv.lock与旧API版本相同，
生产fastapi/pydantic/sqlalchemy/uvicorn版本与本地一致，复用旧API虚拟环境。
新目录`.venv`依赖旧版本目录中的`.venv`，因此回滚版本和这一依赖闭包必须保留，不能清理旧目录。

切换前新版本`check_fastapi_release.py --require-loopback`通过，确认只读数据库角色及既有RPC权限。
只重启`market-data-center-api.service`，保留systemd unit、restart drop-in、外置api.env和原API Key。
外置环境文件保持root:600，前后摘要相同；不公开任何配置值或摘要。
按项目长期规则，没有执行PostgreSQL备份或恢复演练。本次没有任何数据或schema改动。

## 线上验收

- API active，PID=1663130、NRestarts=0；进程工作目录解析为新版本。
- `/healthz`返回ok；`/readyz`返回ready。
- 线上`/openapi.json`与发布包中的完整契约完全相同。
- 12个新增路由均在“实时接口”，每个未带API Key请求均被401拒绝。
- 下表为一次小样本真实来源验收，不是长期可用性保证，不保存证券Raw或用户凭据。

| 公共路由（省略/api/v1/realtime） | 验收参数 | HTTP | 返回条数 |
|---|---|---|---|
| /eastmoney/quotes | page_size=2 | 200 | 2 |
| /eastmoney/limit-up | trade_date=2026-10-13,page_size=2 | 200 | 2 |
| /eastmoney/limit-down | 同上 | 200 | 2 |
| /eastmoney/broken-limit | 同上 | 200 | 2 |
| /eastmoney/klines | symbol=SSE:600000,limit=10 | 200 | 10 |
| /tencent/klines | symbol=SSE:600000,limit=10,server=web | 200 | 10 |
| /sina/klines | symbol=SSE:600000,limit=10 | 200 | 10 |
| /tencent/minute | symbol=SSE:600000 | 200 | 45 |
| /tencent/index | 固定sh000001 | 200 | 1 |
| /eastmoney/themes/today | 固定第一页100 | 200 | 5 |
| /eastmoney/themes | page_size=2 | 200 | 2 |
| /eastmoney/themes/00647/stocks | page_size=2 | 200 | 2 |
| /tencent/klines备用主机 | symbol=SSE:600000,limit=10,server=ifzq | 200 | 10 |

三类池和分时均明确回显来源日期2026-10-13。响应persisted=false，来源网站、用途和请求参数均存在。
官方`api-smoke-check.sh`再次通过release preflight、健康/就绪及带鉴权的有界证券数据库查询。
Worker仍active，PID=904880与发布前相同；没有手工触发Worker任务或修改计划。

## 回退

旧版本仍完整保留。需要回退时仅将`/home/project-api`原子切回前述旧目标并重启API，
然后重复健康、就绪及既有接口检查。外置配置不需替换，数据库不需恢复或回滚DDL。
回退同样属于生产变更，应由明确运维请求触发；此次没有执行回退。
