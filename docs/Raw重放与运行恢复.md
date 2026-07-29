# Raw 重放与运行恢复

本文是 ADR-0006 的操作手册。所有命令从环境变量读取数据库和 Raw 根目录，不在命令行参数、报告或日志中传递凭据。

## 1. 重放前验证

先执行 dry-run。它会读取 Manifest 和 Raw，校验字节数、SHA-256、行数、Schema 版本，运行标准化与领域校验，但不创建 IngestionRun，也不写 Core：

```bash
market-data-center raw-replay \
  --ingestion-id 74b11082-4ec0-4ae4-826f-a80a96cb9985 \
  --dry-run
```

输出是单行 JSON。`status=valid` 表示重放输入有效；命令失败时只输出稳定的 `operation` 与 `error_type`，退出码非零。

## 2. 实际重放

```bash
market-data-center raw-replay \
  --ingestion-id 74b11082-4ec0-4ae4-826f-a80a96cb9985
```

实际重放创建新的 IngestionRun。新 Core 行使用新 `ingestion_id`，运行的 `replayed_from_raw_id` 指向原 RawManifest。重跑仍按领域自然键 upsert，不以来源区分重复。

Raw 缺失、字节数或 SHA-256 不一致、行数不一致、不支持的 Schema、标准化或领域校验失败都会阻断对应写入。失败批次记录脱敏 QualityResult，不复制 Raw 文件。

## 3. 恢复僵尸运行

先查看超过 60 分钟仍为 `running` 的候选：

```bash
market-data-center recover-stale-runs --older-than-minutes 60 --dry-run
```

确认阈值后执行：

```bash
market-data-center recover-stale-runs --older-than-minutes 60
```

实际命令使用单条原子 UPDATE，将仍满足条件的运行标记为 `failed`，同时设置 `finished_at` 和稳定错误摘要。它不会处理 `pending` 或已经处于终态的运行。

## 4. Daily Bar 多源比较

```bash
market-data-center compare-daily-bars \
  --symbol SSE:600000 \
  --start-date 2026-07-01 \
  --end-date 2026-07-28
```

命令从历史成功/部分成功批次的 Raw 还原标准 DailyBarRecord，报告各 Provider 覆盖行数、可比较日期数和逐字段差异。金额和价格以十进制字符串输出。报告不修改 Core、不自动仲裁来源，也不把差异写成交易信号。

## 5. 常见阻断

- `RawIntegrityError`：Raw 文件缺失、路径越界、字节数/SHA-256/行数不符或 JSONL 无效；
- `ProviderError`：Schema 版本不支持、请求参数不足或来源字段无法标准化；
- `LookupError`：指定 IngestionRun 不存在；
- Daily Bar 引用未知证券或非交易日：批次按 Validator 结果 partial/failed，不绕过质量规则。

本地 Raw 必须与数据库备份成对保留。只有数据库 Manifest 而没有对应文件，无法完成重放。
