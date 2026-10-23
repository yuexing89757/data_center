# 股票数量单位整改交付与操作边界（2026-10-23）

治理：[ADR-0068（Accepted）](adr/ADR-0068-股票数量单位全链路契约与受控修复.md)、GitHub Issue #92。
范围和原始观察见[设计](superpowers/specs/2026-10-23-stock-quantity-units-design.md)、
[只读证据](volume-unit-audit-evidence-2026-10-23.json)及[来源单位目录](source-quantity-units.md)。
本文是交付状态与操作边界说明，不是生产整改完成证明。
后续明确部署授权已执行：代码、四个迁移与读取门禁上线；详见
[生产发布与验收记录](runbooks/quantity-units-production-2026-10-23.md)。历史修复仍未执行。

## 1. 本地实现与生产状态

| 项目 | 本地实现 | 生产状态 |
|---|---|---|
| 来源边界 | `stock-quantity.v1` / `quantity-normalizer.v1`；手、股、万股及独立可转债张精确转换；未知证据阻断 | 已部署；未验收后续真实定时写入或全部历史来源 |
| 旧 Raw 回放 | AKShare 日 K v1/v2、远程 TDX v1/v2 正确标准化；本地 v1/v2/v3 股数不再转换；原 Raw 不改 | 未运行生产 replay |
| 日 K 修复 | 默认只读预览；计划/目标库 hash、完整 before 指纹、Raw 校验、CAS、新 replay ingestion、审计、恢复及回滚 | Worker 单行只读预览通过；未执行历史写入或全量 Manifest 核验 |
| 准入和读取 | 认证表、受限审计、公共数量 null/质量状态、不可变旧快照拒绝入口 | 四个迁移与服务切换通过；实际 HTTP/RPC null/质量及409通过 |
| 分类量额汇总 | 算法 `1.2.0`；完整成员、volume/amount 独立覆盖数，缺失相应合计为 null | 未重算生产派生结果 |
| 不可变快照 | 精确受影响 snapshot/input_hash/calculation_id 报告；保留既有版本机制 | 未重建、替换或退役任何生产快照 |
| 外部消费 | 文档和公开契约按本地变更审查 | 未取得实际调用方版本/旧补偿逻辑的签收 |

生产 migration 和 Worker/API 服务切换已按后续明确授权执行；没有人工触发修复、
采集、replay、重算、快照替换或凭据操作。旧数量未认证时按契约隔离，不能称为历史已修复。
代码存在及本地单测通过不能替代 Worker Raw 验证、隔离 PostgreSQL 验收和生产操作授权。

## 2. 已观察存量不是可直接执行的计划

以下为 2026-10-23 只读证据中的历史水位，执行时必须重新预览，不自动扩展到后续采集：

| 股票日 K 来源 | 观察数量与日期 | 当前处置 |
|---|---|---|
| `pytdx.remote_daily_bar.v1` | 224,175 行，2026-08-11～2026-10-21 | 已知旧手值映射候选；逐 Raw/自然键证明后才修复 |
| `pytdx.local_daily_bar.v2` | 2,013,506 行，2025-01-02～2026-10-22 | 来源已是股；核对 Raw 后认证，不批量乘100 |
| `pytdx.remote_daily_bar.v2` | 8 行，2026-10-22 | 已按股标准化；核对 Raw 后认证，不再次换算 |
| pytdx 缺可用 manifest | 20 行、4 只证券，2026-08-04～2026-09-18 | 保留独立隔离清单；禁止猜单位或补造 Raw |
| AKShare 股票日 K | 本次库内未观察到 manifest/现行行 | 工具支持已登记 v1/v2；不能宣称存在已修复存量 |

涨停池观察到 2,528 个 member 引用远程 v1 日 K，其中 2,473 个 volume 非空；
日期为 2026-08-11～2026-10-21。这是输入候选，不是获准原地修改的 member 清单。
本机抽查的 34 个 Raw 对象不可读，只证明本机缺少可读证据，不证明 Worker Raw 已丢失。

非股票补充核验发现 `cb_daily.amount` 官方单位为万元，旧适配器误按千元转换。
本地代码已改为独立、精确的万元→元转换，`cb_basic.issue_size` 另按官方元单位认证；
`cb_daily.remain_size` 不在该接口已核实字段表中，非空值阻断、缺失保留null。
2026-10-23 只读补查的 `convertible_bond.daily_bar` 和 `convertible_bond.bond` 均为0行，
本库没有已观察到的债券历史修复候选；这不替其他部署或未来数据作推断。

五档、竞价指示、竞价热表及历史归档、EOD、技术因子 JSON、THS/指数、股本等历史数量，
仍需其独立来源、Raw 和字段组认证。当前日 K 修复 CLI 不会修复或批量认证这些表。

## 3. 日 K 工具的精确范围

实现位于 [`quantity_unit_repair.py`](../src/market_data_center/quantity_unit_repair.py)，
入口为 [`repair_stock_quantity_units.py`](../scripts/repair_stock_quantity_units.py)。

- 事实修改白名单仅为股票 `core.daily_bar.volume` 及新 ingestion 血缘；金额、价格不重写。
  CNY amount 必须与 Raw 的标准输出及 SQL 精度匹配，才与 volume 同事务认证。
- 修复候选仅采用明确登记的 provider/endpoint/schema；旧 remote v1 与 AKShare v1 的目标
  来自校验后的 Raw，绝不采用“当前 volume ×100”。已等于 Raw 目标的行单独验证。
- 原始及 replay lineage 必须可追到唯一匹配的 Raw；验证 SHA256、字节数、行数和自然键。
  缺失、不可读、混合 schema、重复键、多 manifest 歧义及字段冲突明确隔离。
- 预览按最多 500 行分页，`--max-rows` 默认 50,000、上限 1,000,000；超限停止，必须重新分段。
  每页是短只读事务，不承诺全库同一快照。精确自然键、before、来源证据和目标由计划 hash 冻结，
  apply 再比较当前事实/血缘/Raw；水位变化不能用旧审计统计覆盖。
- 修复批次 `--batch-size` 为 1～500。每批事实、新 IngestionRun（`replayed_from_raw_id`）、
  两个字段组证书及 repair item 同事务；批次失败回滚该批，已提交批次保留。
- 已正确历史按完整现行 ingestion 组认证，独立上限 **5,000 行**；不是把修复写批次扩大到5,000。
  完整组锁定复核，SQL statement timeout 25 秒、lock timeout 3 秒；额外一行仅用于检测超界。
  5,001 行及以上或遗漏 sibling 的预览不能取得新合格证。部分预览不会覆盖已有完整 certified 证明。
  超界组需要后续受治理的分页认证能力，缩小日期范围不能伪造完整 ingestion 认证。
- 证书粒度为 `(ingestion_id, stock-quantity.v1, field_group)`，本工具写
  `daily_bar.volume` / `daily_bar.amount`。静态来源规则不能代替持久证书。
- 计划绑定目标数据库身份；同一计划恢复不重复换算，重叠计划只有 exact after/新血缘匹配
  才记 `already_applied`。新事实或 Raw 变化记 conflict，必须另行预览。
- 回滚只在 after 指纹及新 ingestion 仍匹配时恢复 volume/旧血缘；否则 `rollback_conflict`。
  恢复旧问题事实时同时隔离相关证书，不能把旧错误重新发布为股。回滚不修改原 Raw。

## 4. 预览、审核、apply 与回滚

以下是操作模板；除下述单行只读采样，未执行这些范围或任何写操作。
凭据及 Raw 根目录只读取现有 WorkerSettings，命令不传数据库 URL、
API key 或实际 Raw 路径。计划含受控行级 before/after 证据，必须保存于仓库外、按备份级权限管理；
脚本拒绝仓库内输出并拒绝覆盖现有计划文件。

先核对实际批准的日期、provider、symbol 和完整 ingestion 范围。示例只演示单证券有界预览：

```powershell
$quantityPlan = Join-Path $env:TEMP 'quantity-preview-review-20261023.json'
uv run python scripts/repair_stock_quantity_units.py --start 2025-01-02 --end 2026-10-22 --provider pytdx --symbol SSE:600000 --max-rows 5000 --batch-size 100 --output "$quantityPlan"
```

预览只有 SELECT 和本地 Raw 读取。2026-10-23 已对现有业务库做单行只读采样：
`SSE:600000 / 2026-10-21 / pytdx`，`max_rows=10`、`batch_size=10`，结果为1条
`quarantined / raw_integrity_failed`、0个认证候选、0个修复依赖；没有 apply 或写库。
封存计划 hash 为 `0823301478b90d6756b33da9f2cdb00703e1c2face0e749484dd8a834f82d727`，
含行级证据的文件保留在仓库外。该结果表示本机不能读取 Raw，不证明 Worker 对象损坏。
本轮没有执行上述全范围模板。审核计划及 stdout 的
`plan_hash`、counts、reasons、现行 ingestion 认证 eligibility、依赖报告；
核对目标库、精确范围、备份/恢复证明、隔离影响及独立生产操作授权。不要将含行情行的 JSON 提交 Git。

```powershell
$reviewedQuantityHash = '替换为已审核计划的64位SHA256'
uv run python scripts/repair_stock_quantity_units.py --apply --plan "$quantityPlan" --expected-hash "$reviewedQuantityHash" --batch-size 100
```

中断后使用同一文件和同一 hash 恢复。stdout 返回 repair item 及字段组认证计数；
conflict/rollback_conflict 返回退出码2。不能改计划后沿用原 hash，不能把 `completed` 当作所有隔离数据已修复。

```powershell
uv run python scripts/repair_stock_quantity_units.py --rollback --plan "$quantityPlan" --expected-hash "$reviewedQuantityHash" --batch-size 100
```

回滚是独立敏感写操作，也需要精确范围授权。已回滚计划不能直接 apply；需要新预览。
正确历史认证本身不是 volume 修改，不创建逐行 repair item。

可另外验证某一已批准 Raw 批次的标准化路径：

```powershell
$sourceIngestionId = '替换为已审核的ingestion UUID'
uv run market-data-center raw-replay --ingestion-id "$sourceIngestionId" --dry-run
```

普通 `raw-replay` 不提供本工具的计划 hash / before CAS 保护；不能以去掉 `--dry-run`
替代已批准的历史修复步骤。竞价 market snapshot/series 的通用 replay 当前明确禁用。

## 5. 发布与重建的顺序及现有入口

1. 在 Worker 所在环境只读核对 Raw、现行 lineage、精确范围、调用方补偿和恢复依据；
   先通过隔离 PostgreSQL 迁移/权限/恢复测试，记录仍隔离的对象。准备经过审核的预览。
2. 通过[既有生产发布流程](最小生产发布运行手册.md)批准 schema 与软件发布窗口。
   本地新增 migration 依序为 `20261023000100`（证书/账本）、`20261023000200`（读/计算门禁）、
   `20261023000300`（分类覆盖数）、`20261023000400`（现有受控竞价桥接的新批次原子认证；
   不追认复用的旧快照）。受保护 workflow 的 `apply` 一次应用全部待执行 migration，
   当前没有按目标版本暂停或独立读门禁开关；不能把“先只上001、认证后再开002”写成已有命令。
   002 生效会立刻屏蔽未认证存量，必须先批准该影响及旧写入进程切换方案。
   原 FastAPI DTO 禁止额外字段，旧读取进程可能拒绝新增 `quantity_quality` 响应；
   schema、读取进程、Worker 及消费者须在明确维护窗口协调切换，不能假设滚动发布无中断。
   新读取代码通过单位契约 RPC 拒绝旧 schema；不得为临时可用性退回未经门禁的读取。
   分阶段预认证发布还需要明确的发布决策，不能用手工 DDL 绕过。
3. 在获准窗口运行精确 hash 的日 K apply/正确历史认证；复核冲突、未认证组、数量/价格不变项、
   before/after 和新血缘；只对已证明需要的日期安排下游操作。
4. 对分类量额结果使用**既有**版本化派生服务入口。下面的例子仅在日期获准、输入完整性已核对后执行：

   ```powershell
   uv run market-data-center derived-recompute --start-date 2026-10-21 --end-date 2026-10-21 --mode full --algorithm-version 1.2.0
   ```

   这是现有整个日派生 bundle，输出还包含复权行情、日指标、市值；没有仅重算 classification 的 CLI。
   不宣称本工具已经自动选择或重算它们。冻结其他输入，核对价格驱动结果不受数量纠错影响，
   核对 `volume_member_count` / `amount_member_count` 与 null，保存新 calculation_id/input_hash。
5. 精确日期涨跌停快照只能沿既有不可变构建机制发布新版本。其前置 exact basis 股票池如已有效，
   不因量变化强制重建纯价格规则；确需修订时，现有目录入口为：

   ```powershell
   uv run market-data-center stock-pools-build --basis-trade-date 2026-10-21
   uv run market-data-center stock-pool-check --pool-code CN_A_PREVIOUS_DAY_MAINBOARD_LIMIT_UP --effective-trade-date 2026-10-22
   uv run market-data-center stock-pool-check --pool-code CN_A_PREVIOUS_DAY_MAINBOARD_LIMIT_DOWN --effective-trade-date 2026-10-22
   ```

   effective 日期必须从统一交易日历证明，不能固定用自然日+1。check 为只读，build 为写入。
   `stock-pools-build` 使用 `stock_pool/build_stock_pools` 受控 workflow。
6. 以下 exact-date 入口存在，并分别通过 `today_limit_up_snapshot/fill_today_limit_up_snapshot`、
   `today_limit_down_snapshot/fill_today_limit_down_snapshot` 记录 Operations：

   ```powershell
   uv run market-data-center today-limit-up-snapshot --trade-date 2026-10-21
   uv run market-data-center today-limit-down-snapshot --trade-date 2026-10-21
   ```

   **它们当前不是可无条件执行的历史修复步骤。** 两者会重新读取 AKShare 来源并写新 Raw，
   没有 `--from-raw` / 固定原 source ingestion 的重建 CLI；封板/封单资金单位尚未证实，
   当前来源适配器会阻断。必须先解决独立来源契约、原观察重建能力与输入认证，另获操作授权。
   本地修复工具只报告 snapshot ID/input_hash/calculation_id，不自动调用这些命令。
   门禁未满足时保持不完整/409，不能重启未知来源、删旧快照或原地更新 member。
7. 条件满足后的既有持久化会把当前 daily_bar_ingestion_id 等输入纳入 input_hash：
   相同输入返回 unchanged，变化产生新 snapshot/version/calculation_id；旧对象保留。
   发布后必须核对新版本精确日期、成员输入证书与选择结果；显式旧问题版本仍不能绕过409。
   本轮没有生成生产“旧→新 snapshot”清单，也没有完成切换或缓存失效。

以上均为人工受控入口，不注册 OS 采集计划，不改变 Worker 任务目录和既有调度策略。

## 6. 必须保留的审计与验收

修复审计位于受限 `audit.quantity_unit_repair_run/item`，不是 Operations workflow 的冒名任务：
记录 plan hash/版本、固定计划、持久 next_item、原/新 ingestion、原 Raw ID/hash、before/after 指纹、状态。
认证位于 `audit.quantity_unit_certification`，含字段组、Raw hash 集、normalizer、证据 hash 和认证时间。
既有重建的 workflow/job 执行事实保存在 `operations.workflow_run/job_execution`，
派生版本位于 `derived.calculation_run`，快照 metadata 指向对应 calculation/source lineage。
人工命令stdout应保存在受控操作记录，不能声称所有命令自动形成同一种账本。

每次获准操作保存：批准范围/目标库身份、备份恢复证明、计划 hash、状态计数、未修复原因、
新 ingestion 与证书摘要、派生 calculation_id/input_hash、快照旧→新版本映射、API同值检查、
缓存处理及调用方去补偿签收。不可把行级行情、Raw、密钥或真实本地路径放进 Git/公开报告。

最终本地验证（2026-10-23）：

| 检查 | 结果 |
|---|---|
| `uv sync --all-groups` | 75个包解析、73个已安装包检查通过 |
| `uv run ruff format --check .` | 539个文件通过 |
| `uv run ruff check .` | 通过 |
| `uv run mypy src` | 166个源文件通过 |
| `uv run pytest -q --tb=short --show-capture=no -p no:cacheprovider` | **2028 passed, 3 skipped，446.27秒**；通过进程环境注入隔离 PostgreSQL 17.11 的 `TEST_DATABASE_URL` |
| 补充 CLI 默认预览驱动/无写入回归 | `tests/test_quantity_repair_cli.py`：1 passed；随后实际业务库单行只读预览也通过 |

全量运行包含全部183项 PostgreSQL集成用例，不再是因缺测试库而跳过。
仅3项 Windows 测试符号链接因主机不能创建 symlink 而跳过；没有把它们记为通过。
使用 `-p no:cacheprovider` 避免本机已有 pytest cache 的写权限问题，不改变测试选择。
隔离库只监听 IPv4 loopback，不使用业务库作变更测试，不注册系统服务或 OS 定时采集任务。

已执行的数据库测试包括完整迁移和幂等、角色/RLS、备份与独立恢复、501行认证、
5000/5001边界、hash篡改、Raw缺失/歧义、旧版/正确值、CAS/恢复/回滚、重叠计划，
部分预览不撤销完整证书，以及旧复制快照不能因原批次后续认证重新放行。
两条正向链路验证了实际适配器/回放→认证→API角色RPC→FastAPI TestClient；
37股及370.25元保持不变，旧远程v1修复后3700股/37000元重复回放不再次换算，Raw字节不变。
热表/历史归档和缓存相关本地回归通过；这不是已部署 PostgREST 服务、Worker 实际 Raw、
生产缓存恢复或全体外部消费者的端到端验收。

```powershell
uv run ruff format --check .
uv run ruff check .
uv run mypy src
uv run pytest
uv run pytest -m integration
```

仅允许隔离可丢弃的 `TEST_DATABASE_URL`；不得拿业务库替代。完整本地 gate 以主任务最终输出为准。

## 7. 尚未关闭的范围

- Worker 已完成独立文件备份与单行 Raw 核验；全量 Manifest 对应 Raw 核验、完整历史认证/修复、
  修复前后统计和快照替换尚未执行。
- 日 K 以外的历史数据没有通用自动修复/认证脚本；5,000行以上现行 ingestion 组没有分页认证协议。
- AKShare 股本及涨跌停资金、THS/普通指数、数量敏感技术因子、已退役 pysnowball 的独立证据缺口
  仍按[来源目录](source-quantity-units.md)阻断，不能通过改label、全置null或盲乘100冒称成功标准批次。
- 已知消费方向包括九合一应用、研究脚本和 apiServer 技术因子调用，但没有可验证的部署/人员注册表；
  谁仍额外乘除100、使用哪一版接口和缓存尚未签收。本轮没有发送任何外部通知。
- 当前交付是“代码及读取门禁已部署、部分历史工具就绪、历史/来源/调用方仍待验证”，不能宣布数量已全链路统一。
- 来源发布的 `volume_ratio` 为无量纲来源事实，不由本库日线重算；其精确窗口及盘中折算公式
  尚缺上游明确公式证据。现有本地自由流通换手率公式为成交股数/自由流通股数×100%，
  分子分母均为股。不能把来源量比保留值当作已完成公式口径核验。
