# Architecture Decision Records

ADR 记录已经接受的架构决策。其优先级低于项目宪法，高于一般领域说明和历史文档。

## 状态

- `Proposed`：讨论中，不得作为编码依据；
- `Accepted`：已经定稿，代码和 migration 必须遵守；
- `Superseded`：已被后续 ADR 替代；
- `Deprecated`：不再适用，但没有直接替代方案。

## 规则

1. 已接受 ADR 不直接改写核心结论；变化通过新 ADR 替代旧 ADR。
2. PR 必须链接相关 ADR。
3. 实现与 ADR 冲突时，先修改决策，不允许在代码中静默偏离。
4. Wiki 只同步 `Accepted` 决策和已经合并的实现事实。

## 当前 ADR

- `ADR-0007-Capital与公司行为基础事实.md`：定义股本、分红送转和配股输入事实的自然键、单位、修订语义与 AKShare 接入。

- `ADR-0001-第一阶段架构基线.md`：第一阶段开工所需的领域和工程基线。
- `ADR-0002-AKShare第二数据源接入.md`：允许显式选择 AKShare 第二数据源，不引入自动路由或回退。
- `ADR-0003-同花顺动态板块指数.md`：以独立 BoardIndex 领域接入同花顺动态板块指数及每日成分股快照。
- `ADR-0004-pytdx日K补数源.md`：允许 pytdx 读取本地通达信 `.day` 文件作为显式的未复权股票日 K 补数源。
- `ADR-0005-Provider自动路由与故障切换.md`：按数据集能力自动选择 Provider，仅对来源错误执行确定性回退和进程内熔断。
- `ADR-0006-Raw重放与运行恢复.md`：重放创建新 IngestionRun 并引用原 RawManifest，同时定义僵尸运行恢复与只读多源差异报告。
