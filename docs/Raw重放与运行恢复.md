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

ADR-0058 的第一阶段已在本地实现 gzip 读取兼容，尚未部署：原 Manifest 的逻辑路径、
SHA-256、字节数及行数保持不变。仅在原 JSONL 文件不存在时尝试同路径加 `.gz` 的压缩表示，
解压读取受原字节数上限约束，再校验原 Manifest；明文损坏或无权读取时不会退回 gzip。
逻辑路径已有 gzip 文件时禁止重新写入 JSONL；路径越界、软链接和非普通文件会被拒绝。
龙虎榜孤儿恢复扫描同样识别 `.jsonl.gz`，双表示只产生一个逻辑候选，不自动删除文件。

此阶段仅提供兼容读取，自动压缩、登记、校验后移除明文及其他维护步骤仍待实施；不要据此手工
删除生产明文。部署与首次生产压缩仍需单独授权，旧的明文专用版本不能直接作为压缩后的回退版本。

## 2. 实际重放

```bash
market-data-center raw-replay \
  --ingestion-id 74b11082-4ec0-4ae4-826f-a80a96cb9985
```

实际重放创建新的 IngestionRun。新 Core 行使用新 `ingestion_id`，运行的 `replayed_from_raw_id` 指向原 RawManifest。重跑仍按领域自然键 upsert，不以来源区分重复。

Raw 缺失、字节数或 SHA-256 不一致、行数不一致、不支持的 Schema、标准化或领域校验失败都会阻断对应写入。失败批次记录脱敏 QualityResult，不复制 Raw 文件。

## 3. 恢复僵尸运行

### 官方监管事件历史补采（2026-09-22）

Issue #83 的受控入口 `regulation-events` 每次只查询一个精确交易日、一个官方来源。
执行前必须明确确认交易所访问、保存和再分发条款，且目标日不早于 2026-07-06、不晚于今天，
并存在于交易日历。不要并行调用官方源；适配器每个真实 HTTP 请求前间隔一秒，保留页数、
文档数和网络超时上限。

```bash
market-data-center regulation-events --trade-date 2026-09-21 --source sse_official --confirm-official-source-terms-reviewed
market-data-center regulation-events --trade-date 2026-09-21 --source szse_official --confirm-official-source-terms-reviewed
market-data-center regulation-calculate --trade-date 2026-09-21
```

近30交易日查询需先按数据库日历补齐该窗口内两家的事件，不能只补目标日就声称窗口完整。
逐日串行执行；失败保留 IngestionRun/质量记录和已取得的 Raw，不把失败日期当作无事件。
完成后检查每个日期/来源的成功记录、Raw dry-run，再计算目标日并核验两个 API。

历史查询区间采用上海时间半开区间，次日零点不混入下一日。Raw 的 `trade_date_start/end`
是历史查询日期；`observed_at` 是本次真实抓取时间，`observed_from/to` 是本次采集审计区间，
不是历史交易日。禁止回拨时钟、倒填观察时间或直接更新旧计算批次的公告清单。
新事件改变输入哈希后生成新计算批次；无输入变化时仍复用旧批次，不强制覆盖。

本入口不注册或启用任何定时任务、不改数据库结构和权限，也不访问竞价数据。
生产执行及发布修复代码需要明确授权；2026-09-22 项目所有者已批准本次
2026-08-11～2026-09-21 官方事件补采及仅 2026-09-21 重算，并确认官方源条款门禁。

### 僵尸运行

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

2026-09-22 修复后的注意事项：

- 股东人数重放与采集均隔离非法标准行，登记 `shareholder_count.invalid_record` ERROR；
  合法行可发布，混合结果为 partial，全拒绝为 failed，不改写日期或人数。可能截断的
  满额 Raw 不能单独证明完整覆盖；补齐整天应重新运行受控范围同步及切片逻辑。
- 远程 pytdx 的特定零字解码产物 `5.877471754111438e-39` 在成交量/金额标准化为零，
  原始 Raw 不变；其他非整数量仍失败，不能据此推断停牌或补造行情。
- Regulation 每次采集前查询截至目标日的30个交易日和三个白名单基准指数；缺失或无效
  前收盘价触发有界区间补采，完成后重新查询实际覆盖。普通股票不改源、不补造。
- BaoStock logout 错误只记录脱敏清理警告，不覆盖实际请求错误或已采集事实。

生产补跑必须先获得授权、核对精确日期/ingestion ID，并避开竞价采集时段；本地测试不等于
已经部署或补齐生产数据。股本关系非法、上市历史不足及北交所来源覆盖缺口继续明确保留。

- `RawIntegrityError`：Raw 文件缺失、路径越界、字节数/SHA-256/行数不符或 JSONL 无效；
- `ProviderError`：Schema 版本不支持、请求参数不足或来源字段无法标准化；
- `LookupError`：指定 IngestionRun 不存在；
- Daily Bar 引用未知证券或非交易日：批次按 Validator 结果 partial/failed，不绕过质量规则。

本地 Raw 必须与数据库备份成对保留。只有数据库 Manifest 而没有对应文件，无法完成重放。
