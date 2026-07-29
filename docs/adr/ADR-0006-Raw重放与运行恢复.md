# ADR-0006：Raw 重放与运行恢复

- 状态：Accepted
- 日期：2026-07-29
- 决策者：项目所有者
- 关联：ADR-0001、ADR-0002、ADR-0004、ADR-0005、GitHub Issue #9

## 背景

项目已经做到 Raw-before-normalize，但“保留了 Raw”不等于“能够安全重放”。如果重放重新复制 RawManifest，会破坏一个 Manifest 对应一个不可变对象的语义；如果直接把重放结果挂回原 ingestion run，又会丢失本次执行的时间、结果和失败证据。异常退出还会留下永久 `running` 的批次，多来源数据也缺少只读差异报告。

## 决策

### 1. 重放是新的 IngestionRun

每次非 dry-run 重放创建新的 `ingestion.ingestion_run`，使用新的 `ingestion_id`。新增 nullable 字段：

```text
replayed_from_raw_id -> ingestion.raw_manifest.raw_id
```

实时采集该字段为空；重放运行指向原始不可变 Raw 对象。重放不复制 Raw 文件，也不插入第二条指向相同路径的 RawManifest。重放产生的 Core 事实使用新运行的 `ingestion_id`，通过 `replayed_from_raw_id` 继续追溯原始 Raw。

### 2. 重放必须完整经过边界

重放顺序固定为：

```text
读取 Manifest
  → 校验安全相对路径、格式、字节数、SHA-256、行数
  → 按 provider + dataset + schema_version 选择版本化 normalizer
  → 校验 source_code
  → Validator / QualityResult
  → IngestionEnvelope
  → 幂等 Persistence
```

不支持的 Schema、Raw 缺失或损坏、标准化失败均阻断 Core 写入。非 dry-run 必须写 failed IngestionRun 和阻断级 QualityResult；错误摘要不得包含原始行、数据库连接信息或 Secret。

Dry-run 执行读取、哈希、标准化和领域校验，但不创建运行、不写 QualityResult、不改 Core。

### 3. 僵尸运行恢复

治理命令按显式时长阈值选择 `status = running` 且 `started_at` 早于阈值的批次。实际恢复使用单条原子 UPDATE 将其标记为 `failed`，同时写入 `finished_at` 和稳定错误摘要。Dry-run 只返回候选 ID。

### 4. 多源差异只报告

Daily Bar 多源比较读取历史成功/部分成功批次的 Raw，按各自 Schema normalizer 还原标准 Record，再按 `(symbol, trade_date)` 对比。报告不新增 Core 表、不改变来源优先级、不自动选择“正确值”、不回写 QualityResult。

同一 Provider 对同一自然键存在多次批次时，比较使用 `requested_at` 最新的标准化记录。价格和金额以 Decimal 字符串输出，避免 JSON 浮点转换。

## 后果

### 正面

- Raw 真正具备可验证、可审计的重放路径；
- 重放执行与原始对象的血缘清晰且不重复 Manifest；
- 异常退出不会永久污染运行状态；
- 多来源差异可观察，但不会静默覆盖领域事实。

### 代价

- Raw Schema normalizer 必须随版本长期维护；
- 本地 Raw 丢失时数据库 Manifest 无法单独完成重放；
- 多源比较需要读取 Raw 文件，适合诊断和验收，不替代在线查询。

## 验收

- 任一受支持成功 Raw 批次可 dry-run 并实际重放；
- 重放相同批次不增加领域自然键重复记录；
- Raw 缺失、篡改和不支持 Schema 会阻断并留下脱敏证据；
- replay Core 行能经新 IngestionRun 追溯到原 RawManifest；
- 僵尸运行恢复是原子且可 dry-run；
- 多源差异报告不修改 Core；
- migration、Ruff、mypy、pytest 和 PostgreSQL 集成测试通过。
