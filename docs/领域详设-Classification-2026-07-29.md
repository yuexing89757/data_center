# 领域详设：Classification v1

> 状态：有效
> 日期：2026-07-29
> 依据：`adr/ADR-0008-Classification分类与成分历史.md`

## 1. 边界

Classification 保存外部分类体系的版本化目录、完整成分快照，以及未来可信来源提供的成员有效区间。它依赖 Security 的统一 `symbol`，不把分类代码注册为 Security，也不保存板块行情或推导交易信号。

## 2. 标识与类型

分类标识由以下字段组成：

| 字段 | 含义 |
| --- | --- |
| `namespace` | 分类体系命名空间；首期为 `eastmoney` |
| `classification_type` | `industry`、`concept` 或 `index` |
| `classification_code` | 命名空间内的分类代码 |

`source_code` 和 `ingestion_id` 用于追溯，不参与事实去重。同一个代码在不同命名空间或类型中互不冲突。

## 3. 完整目录快照

`ClassificationCatalogSnapshotRecord` 表示某日取得的一份完整目录，包含零到多个 `ClassificationDefinition`：

| 字段 | 约束 |
| --- | --- |
| `snapshot_date` | Asia/Shanghai 本地日期 |
| `code` | 同一目录快照内唯一且非空 |
| `name` | 非空 |
| `level` | 正整数，默认 1 |
| `parent_code` | 可空；非空时必须指向同一快照中的目录项 |

数据库以 `(namespace, classification_type, snapshot_date)` 保存快照头，并记录 `definition_count`。同日修订逐项 UPSERT，再删除新目录中不存在的代码；因此相同快照重复写入不会破坏已抓取的同日成分，真正删除的分类则级联删除其无效成分快照。

## 4. 完整成分快照

`ClassificationMemberSnapshotRecord` 表示某分类在某日的一份完整成员集合。自然键是 `(namespace, classification_type, classification_code, snapshot_date)`，集合项再以 `symbol` 唯一。

- 必须先存在同日目录定义；
- 每个 `symbol` 必须存在于 Security；
- 重复成员、未知分类或未知证券阻断整个快照；
- 允许零成员，快照头的 `member_count = 0` 用于区分空集合和未采集；
- 同日修订原子替换成员集合。

任意日期 `D` 的成员重建应选择该分类 `snapshot_date <= D` 的最新快照。目录和成分分别选择最新完整快照，调用方不得把更晚快照倒填为更早历史。

## 5. 有效区间

`classification.member_interval` 只接受能够提供真实生效日期的未来来源：

| 字段 | 约束 |
| --- | --- |
| `valid_from` | 必填，闭区间起点 |
| `valid_to` | 可空，闭区间终点；不得早于起点 |

同一 `(namespace, type, code, symbol)` 的区间通过 PostgreSQL GiST 排他约束禁止重叠。东方财富当前接口只写完整快照，不从相邻快照推断或写入有效区间。

## 6. Provider、Raw 与路由

AKShare Adapter 使用四个东方财富接口：

- `stock_board_industry_name_em`
- `stock_board_industry_cons_em`
- `stock_board_concept_name_em`
- `stock_board_concept_cons_em`

Raw Schema 分别为 `akshare.classification_catalog.v1` 和 `akshare.classification_members.v1`。请求参数保留类型、代码和上海日期以支持确定性重放。BaoStock 和本地 pytdx 明确声明不提供该能力；自动 Router 对两个 Classification 数据集只选择 AKShare。

## 7. CLI 顺序

```bash
market-data-center classification-catalog --classification-type industry
market-data-center classification-members --classification-type industry --classification-code BK0475
```

概念分类把类型替换为 `concept`。同一天应先采集完整目录，再按目录代码采集成员；单个成分命令不会自动遍历整个目录。

## 8. 存储与 PostgREST

内部表位于 `classification` Schema：

- `catalog_snapshot`
- `definition_snapshot`
- `member_snapshot`
- `member_snapshot_item`
- `member_interval`

`api_v1.classification_catalog_snapshots`、`api_v1.classification_member_snapshots` 和 `api_v1.classification_member_intervals` 只暴露业务字段。Worker 对内部表有最小写权限，匿名和认证客户端只能读取 View。

## 9. 验收

- 行业和概念目录、单分类成员可采集并保留 Raw；
- 相同快照重复写入不增加重复事实；
- 同日修订完整替换，空集合可被保存；
- 未知 Security、重复成员、未知父级和冲突定义被阻断；
- 任意日期可按最新的不晚于该日期的完整快照重建；
- Raw 重放产生新 IngestionRun 并引用原 Raw，不复制 Manifest；
- 数据库拒绝成员有效区间重叠；
- 从空库执行 migration 后三个 `api_v1` View 可读。
