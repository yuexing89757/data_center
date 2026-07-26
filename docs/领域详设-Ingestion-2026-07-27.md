# 领域详设：Ingestion v2

> 状态：第一阶段有效  
> 日期：2026-07-27  
> 依据：`adr/ADR-0001-第一阶段架构基线.md`

## 1. 边界职责

Ingestion 管理一次外部数据采集的运行状态、Raw 对象清单和数据质量结果。它提供追溯和运行审计，不承载 Security、Trading 或 Market 的业务事实。

## 2. IngestionRun

数据库表：`ingestion.ingestion_run`

| 字段 | 类型 | 约束与含义 |
| --- | --- | --- |
| `ingestion_id` | uuid | 主键 |
| `provider_code` | text | 第一阶段固定 `baostock` |
| `dataset_code` | text | `security`、`trading_calendar`、`daily_bar` |
| `status` | text | `pending`、`running`、`succeeded`、`failed`、`partial` |
| `requested_at` | timestamptz | 请求创建时间 |
| `started_at` | timestamptz | 实际开始时间，可空 |
| `finished_at` | timestamptz | 结束时间，可空 |
| `request_params` | jsonb | 已脱敏的请求参数，默认 `{}` |
| `fetched_rows` | bigint | 来源返回行数，默认 0 |
| `accepted_rows` | bigint | 通过校验行数，默认 0 |
| `rejected_rows` | bigint | 被拒绝行数，默认 0 |
| `error_summary` | text | 脱敏错误摘要，可空 |
| `created_at` | timestamptz | 默认 `now()` |
| `updated_at` | timestamptz | 默认 `now()` |

约束：计数字段均不小于 0；结束状态必须有 `finished_at`；`accepted_rows + rejected_rows` 不得大于 `fetched_rows`。

## 3. RawManifest

数据库表：`ingestion.raw_manifest`

| 字段 | 类型 | 约束与含义 |
| --- | --- | --- |
| `raw_id` | uuid | 主键 |
| `ingestion_id` | uuid | 外键关联 IngestionRun |
| `storage_backend` | text | 第一阶段固定 `local` |
| `object_path` | text | 相对 Raw 根目录的路径 |
| `file_format` | text | `parquet` 或 `jsonl` |
| `content_sha256` | text | 64 位小写十六进制 SHA-256 |
| `byte_size` | bigint | 文件字节数 |
| `row_count` | bigint | 文件记录数 |
| `schema_version` | text | Raw 结构版本 |
| `created_at` | timestamptz | 默认 `now()` |

约束：`unique(storage_backend, object_path)`；字节数和行数不小于 0；同一内容可通过 SHA-256 识别，但不强制全局唯一。

## 4. QualityResult

数据库表：`audit.quality_result`

| 字段 | 类型 | 约束与含义 |
| --- | --- | --- |
| `quality_result_id` | uuid | 主键 |
| `ingestion_id` | uuid | 外键关联 IngestionRun |
| `dataset_code` | text | 被检查的数据集 |
| `rule_code` | text | 稳定的规则标识 |
| `severity` | text | `info`、`warning`、`error` |
| `status` | text | `passed`、`failed` |
| `natural_key` | jsonb | 关联记录的自然键，可空 |
| `message` | text | 不含 Secret 的可读说明 |
| `details` | jsonb | 结构化详情，默认 `{}` |
| `created_at` | timestamptz | 默认 `now()` |

严重级别为 `error` 且状态为 `failed` 的记录不得进入 Core。质量结果本身只追加，不做 upsert 覆盖。

## 5. IngestionEnvelope

Provider 不生成 `ingestion_id`。Pipeline 创建 IngestionRun 后包装标准 Record：

```text
IngestionEnvelope[T]
├── ingestion_id: UUID
└── record: T
```

`source_code` 属于 Record 的来源语义；`ingestion_id` 属于 Pipeline 的运行上下文。Persistence 将二者写入 Core 事实。

## 6. 状态流转

```text
pending → running → succeeded
                  ├→ partial
                  └→ failed
```

状态变化必须与计数、结束时间在事务中一致更新。任务异常退出后，后续治理任务可以将长期 `running` 标记为 `failed`，但需记录原因。

## 7. 第一阶段验收

- 每次 CLI 执行创建唯一采集批次；
- Raw 文件落盘成功后才写入 Manifest；
- Manifest 的 SHA-256 与实际文件一致；
- Core 事实的 `ingestion_id` 能关联到运行和 Raw；
- 严重质量失败阻止对应事实入库；
- API 客户端不能直接查询或修改 Ingestion/Audit 表。
