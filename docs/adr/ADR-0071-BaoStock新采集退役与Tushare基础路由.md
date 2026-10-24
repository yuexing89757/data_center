# ADR-0071：BaoStock 新采集退役与 Tushare 基础路由

- 状态：Accepted
- 日期：2026-10-24
- 关联 Issue：#94
- 决策依据：项目所有者明确要求「BaoStock 数据源可以废弃吗？改为 tushare」。
- 局部替代：ADR-0005 的 Security/Trading Calendar 首选与 BaoStock 显式采集；ADR-0013 的 Tushare 不参与自动路由限制。

## 决策

1. Security 与 Trading Calendar 的自动路由改为 `tushare → akshare`。项目所有者已进一步明确确认保留 AKShare 兜底；不新增候选、不混合部分成功批次。权限、连接或字段错误沿用 ProviderError 处理，数据库、Raw 存储等系统错误仍立即失败。
2. 通过既有 Tushare Adapter 获取 `stock_basic` 的 L/D/P 三种状态及 `trade_cal` 完整自然日区间。继续使用既有标准身份、日期、校验和血缘，不以空响应伪造完整事实。证券新批次升级为 `tushare.security.v2`：项目所有者已确认将 `T600018.SH`、D、上市日 2000-07-19、退市日 2006-10-20 这一精确历史身份完整留在 Raw，标准目录排除它；请求血缘记录四项身份与明确原因。禁止前缀批量过滤、去 T 合并或按数值猜测。其他非标准身份及已知身份条件变化仍阻断。v2 回放重新从 Raw 核对排除明细；v1 回放维持旧规则，不能静默套用新过滤。BSE 独立目录仍保留全来源 Raw 及其明确 scope。
3. BaoStock 从在线 Provider 注册表和 CLI 可选列表移除，默认构造入口明确拒绝新采集，移除运行安装依赖。Worker 原有 `auto` 工作流自然使用新路由，不改变任务时间或创建第二套采集任务。
4. 历史 `provider_code/source_code=baostock`、枚举、事实、Raw、manifest、单位规则及版本化 normalizer 保留。历史回放直接走 normalizer，不需要 BaoStock SDK 或网络登录；未知单位仍按 ADR-0068 阻断，不能借本次退役自动认证。
5. 普通股票 Daily Bar 维持当前 Accepted ADR 的 pytdx 规则；监管指数仍走其独立 Tushare 契约。不执行历史全量覆盖，不删除数据，不变更股票池或竞价规则。
6. 不改变表结构、公开接口字段或单位；现有迁移已经允许 Tushare 来源，因此无需新增空 migration。公开结果中的来源字段可从 BaoStock 变为 Tushare/AKShare，表示实际来源，不伪装成旧来源。

## 验收与发布

测试覆盖首选成功、来源错误回退、系统错误不回退、全部来源失败、禁用 BaoStock 新入口、历史 Raw 回放，以及日线路由保持 pytdx。同步宪法、技术方案、领域设计、README、AGENTS 与依赖锁文件。

本地实现与线上生效分别记录在 Issue #94；上线前需用实际 Worker 凭据只读验证证券目录、日历完整性及接口权限，发布后检查下次基础采集的实际 provider 与任务健康。Tushare 权限/限流/可用性仍是外部依赖，AKShare 兜底也不保证可用。不得以本地 mock 测试宣称线上已经切换。

### 2026-10-24 本地只读实采发现

- `trade_cal` 的 2026-10-19 至 10-25 返回完整 7 个自然日。
- `stock_basic` 返回 L 状态 5577 条、D 状态 342 条、P 状态 0 条；D 中 `T600018.SH`（2006-10-20 退市）不符合六位标准股票身份，因此现有适配器明确报 `unsupported Tushare symbol`，尚不能成功完成全目录适配。
- 不去掉该身份的 T 前缀、不与 `SSE:600018` 合并、不静默忽略、不自动把异常降为 warning。项目所有者已明确确认保留完整 Raw、版本化明确排除该特殊历史身份；按上文 v2 规则实施，旧 Raw 不修改。
- 修复后使用现有本地配置只读实采：Raw 5919 条、标准证券 5918 条，排除 1 条；SZSE 3094、SSE 2466、BSE 358；v2 Raw 回放与实时适配完全一致。日历仍为完整 7 个自然日。
- 本次尚未部署或运行生产采集；不以本地只读实采宣称 Tushare 已接管线上目录。

### 本地交付验证

- `uv run ruff format --check .`：584 文件通过；`uv run ruff check .`：通过。
- `uv run mypy src`：176 个源文件通过；`uv lock --check`：通过。
- 相关测试：172 passed。最终 `uv run pytest`：1973 passed、218 skipped、1 warning，退出码 0。
- 218 项 PostgreSQL 集成检查因未设置隔离 `TEST_DATABASE_URL` 跳过；warning 是现有 pytest cache 目录写入权限问题。本次未使用生产数据库替代隔离测试。
- 独立代码复核未发现阻断问题。未改 SQL、API 字段、历史事实或 Raw 文件，未部署、未运行生产采集；线上 Worker 凭据的实采和发布后任务验收仍待部署阶段执行。

### 2026-10-24 后续授权部署

所有者随后明确要求推送并部署。`a65ecdf` 已发布至Worker/API，包含本ADR对应的 `b76c390`。
用实际Worker配置只读获取5918条标准证券、完整7天日历，精确排除1条既定历史身份且回放一致；
生产运行包路由已核验。服务健康、单例锁和23项任务通过验收，配置保持不变。
没有手工触发目录入库或日频采集，后续定时批次的实际来源/终态仍需正常调度记录验证。
详情见[发布记录](../runbooks/release-a65ecdf-2026-10-24.md)，不能以此前本地阶段状态代替本次线上状态。
