# 股票数量单位契约生产发布记录

## 范围与授权

2026-10-23，项目所有者在本聊天明确要求“部署线上环境”。本次部署 Accepted ADR-0068、
Issue #92 对应代码、四个有序迁移和既有 Worker/API 服务；历史修复、批量认证、Raw 重放、
派生重算及不可变快照替换没有执行，也不因本记录而获准。

运行提交：`ab1e670a41265a5d8e29136a280747ff26b4b0a0`，版本 `0.2.0`。
构建来自该提交的干净独立检出，未使用 `--allow-dirty`；未包含本地无关 `tinker_classN/`。
已有技术因子调度改动与原线上 Worker 源文件 SHA-256 一致，保留既有调度行为。

## 发布证据

- [发布前生产 check](https://github.com/yuexing89757/data_center/actions/runs/39907168065)：
  原提交 `f496ba8` 的迁移/结构/数据库检查通过。
- [发布提交 CI](https://github.com/yuexing89757/data_center/actions/runs/39907333645)：
  Ruff 格式/静态检查、mypy 通过；普通测试 **1849 passed**，隔离 PostgreSQL 15 集成
  **183 passed**。普通任务中跳过的183项由独立 migrations 任务实际运行。
- [受保护生产 apply 与检查](https://github.com/yuexing89757/data_center/actions/runs/39908935688)：
  目标为同一提交；以下四个迁移均成功，结构与数据库冒烟通过，`orphan_facts=0`：
  - `20261023000100_quantity_unit_certification_and_repair.sql`
  - `20261023000200_quantity_unit_read_gates.sql`
  - `20261023000300_classification_quantity_completeness.sql`
  - `20261023000400_certify_controlled_live_auction_bridge.sql`
- Linux 发布包 SHA-256：
  `5379153ee8dc4a479d5b1d8883923cda8f9e5a330d5106dce71a22251f1d349a`。
- 项目 wheel SHA-256：
  `b4b9dd1246f970c91a17836ef052118c1b5ff59c28bfd1a9e8fc5648d9925a82`。
- 新目录675个发布文件逐字节匹配发布包，生成的完整 FastAPI OpenAPI 与签入契约一致。
  第三方依赖在新虚拟环境中复用既有文件硬链接，原运行环境未覆盖；补齐的8个纯 Python
  wheel 已按锁文件 SHA-256 核验。`uv sync --no-dev --extra api --locked --offline
  --no-install-project` 通过后安装本提交项目 wheel，`uv pip check` 通过。

按项目所有者长期规则，常规部署未创建 PostgreSQL 备份或执行生产恢复演练。
Raw 单独流式保存到本地受限备份存储，不占用线上同等大小的副本空间：
**312596个文件、16107740616字节原始内容**，完整解压逐文件核对字节数和 SHA-256，
压缩归档 **2907464172字节**，SHA-256
`e37ebea5239d6191f0151033d83b53c55f2765183b1bca1516dcd3a4f615af88`。
最终停服务后核对无新增 Raw、文件大小一致；Raw 未修改或删除。文件清单、行级计划、
环境校验摘要和具体回退目录仅保存在受限运维证据，不放入 Git。

## 服务切换与只读验收

维护操作始于上海时间 **13:59:21**。先确认无运行中的 Workflow/Ingestion，
正常停止旧 Worker 和 API；确认旧 PID 退出、调度单实例 advisory lock 释放。
迁移检查通过后于 **14:02:03** 切换两个既有服务链接并启动。
Worker 启动完成后，**14:02:39** 完成实际业务 HTTP/RPC 核验；未把启动过程中
任务页尚未监听的首次检查记作通过，待调度器就绪后重新验证。

两个原发布目录、独立配置、0600权限及 systemd unit/drop-in 均保留，配置哈希不变。
未轮换凭据、改变端口/防火墙、注册操作系统采集任务、发送下游群消息或通知调用方。
Worker 启动按既有目录执行启动检查；没有人工触发历史采集、恢复、重放或重算。

实际验收：

| 项目 | 结果 |
| --- | --- |
| Worker/API | 均 active，重启计数0；使用新版本目录 |
| Worker `--check` | healthy=true，stale_run_count=0，23个既有任务可读 |
| Worker 单实例 | 恰好1个调度锁，任务页正常 |
| API 角色/权限预检、HTTP冒烟 | 通过，未认证请求401，认证有界证券读取200 |
| `/healthz`、`/readyz` | ok / ready |
| 单位版本 RPC | `stock-quantity.v1` |
| 实际历史日K HTTP / API角色RPC | volume、amount均null，质量原因相同；OHLC一致 |
| 旧涨停/跌停快照 | 默认deferred且空成员，显式旧版本409及quantity_units_unverified |
| 技术因子 | 对外v2、来源v1；三种口径的obv/emv/maemv为null |
| 线上完整OpenAPI | 与发布契约完全一致 |
| Worker血缘与TDX池只读检查 | 通过，orphan_facts=0 |
| 历史修复账本/单位证书 | repair_run=0、repair_item=0、certificate=0；未冒称历史已认证 |

依 ADR-0025，当前生产是独立 PostgreSQL + FastAPI 直接调用 `api_v1` RPC，没有运行中的
PostgREST HTTP 网关，也未配置本次工作流的 PostgREST HTTP 探针。
因此上述工作流名称中的“PostgREST”不等于实际网关验收通过；RPC/权限有隔离测试与
实际数据库证据，PostgREST HTTP 的200/406/409验证仍未执行。没有新建网关或MCP服务。

## Worker Raw 只读预览与剩余事项

部署前在 Worker 端对 `SSE:600000 / 2026-10-21 / pytdx` 执行默认只读预览，
Raw 可读取并通过完整性检查，识别1条 `legacy_lots_mapping` 修复候选、2个下游依赖。
冻结计划 SHA-256：
`62ae80a4624d1afe2730d6559005136f11b91e6df77f7a8bc59692721bb950a6`。
只读预览不是写入批准，未来执行前必须重新核对目标、范围、并发与既有恢复规范。
此前“本地 Raw 不可用”的观察不能推断 Worker Raw 丢失。

截至本次验收，没有历史认证，旧数量按契约隔离为null；未完成历史纠错、派生重算、
快照替换、缓存恢复、全部消费方去乘除100签收，也未等到新版本定时采集后的真实写入验收。
来源证据缺口继续阻断对应新批次，见[来源目录](../source-quantity-units.md)。
5000行以上现行ingestion组的历史认证、日K以外通用修复及Raw-only快照重建仍未实现。

发布验收时服务器磁盘剩余约780 MiB（98%已用）；后续磁盘处理见下节。Issue #92 保持 OPEN。
本次结论仅为“整改代码与读取门禁已部署并通过上述线上检查”，不能称为全链路历史统一完成。

## 后续磁盘处理（2026-10-23，上海时间14:15验收）

项目所有者要求先清理磁盘，历史修复、重算及快照替换尚未开始。只读盘点显示根分区约29 GiB，
Raw约17 GiB；当前和回退发布物仍有运行依赖，未按目录年龄删除。另有已持久挂载的数据盘，
处理前可用约16 GB。此操作是明确授权的手工容量维护，不修改 Worker 自动清理白名单或保留期。

清理方案与执行结果：

- 对另一应用备份目录内12个未被进程打开或引用、无额外硬链接的历史 JAR，逐个复制到
  已有数据盘备份归档目录。复制前后核对源身份、字节数和SHA-256，fsync后再原子替换原位置为链接。
  **4269772743字节备份内容完整保留**，12条原路径再次核对哈希并验证普通用户原有读取能力；
  归档根只允许穿越、不开放列表。原路径现在是链接，恢复与备份工具应跟随其目标读取文件。
- 使用 `apt-get clean` 清理可重建的软件包缓存；未卸载软件包或变更系统服务。
- 根分区可用空间从 **817434624字节升至5206454272字节**，实测净增加约4.39 GB；
  `df -h` 为 **4.9G可用、84%已用**。数据盘剩余约12 GB、71%已用。
- 原两个MDC服务PID不变、重启计数0；同机Java监听进程PID不变。API健康、就绪、角色权限、
  单实例锁、任务页、单位RPC/HTTP、旧快照409及完整OpenAPI检查再次通过。
- 数据库、Raw、配置、当前/回退发布版本、日志和历史修复账本未改变；未执行数据库清理、
  VACUUM、历史认证或历史修复。没有发送外部通知。

迁移的逐文件SHA-256、复制及路径切换日志、预览与验收保存在受限运维证据。
已确认数据盘存在持久挂载配置；后续恢复依赖该挂载和保留的归档内容。
本次缓解了根盘空间压力，没有实现ADR-0058尚待交付的Raw压缩登记或停止长期数据增长。
