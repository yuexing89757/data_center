# 本地最大一分钟量实验

此入口只用于所有者授权的本地样例验证，不是正式 Provider、API 或每日任务。
不加载 `.env`，不创建 Settings，不连接数据库，不启动 Worker/API，不写分钟 Raw。
只依赖显式传入的本地 `pytdx.endpoint_pool.v1` 快照；不发现、刷新节点或切换节点拼接数据。
现有日线 capability 仅选择候选，不能认证分钟能力或单位。当前实验支持 SSE/SZSE
代码格式，不代替证券主数据校验；不能据此将基金等品种认证为普通股票。

在项目根目录运行：

```powershell
uv run python scripts/experiments/max_minute_volume_probe.py --symbol SSE:600327 --trade-date 2026-11-03 --pool-file C:/local/explicit-pool.json --output C:/local/peak-summary.json
```

也可以通过 `importlib.util.spec_from_file_location` 加载脚本并调用
`run_probe(symbol, datetime.date(...), pathlib.Path(...))`，返回同样的摘要字典。
不用启动 HTTP 服务。输出只有摘要和分页请求哈希，不包含全部分钟数据。

`status=complete_experimental` 才有 `experimental_max_source_volume`；缺失、重复、
非法标签或未读到历史边界时不返回部分最大值。完整真实零返回0，并列取最早时间。
首尾原生量柱均包含。标准字段 `max_minute_volume_shares` 始终为 null：
本实验不登记正式单位规则或签发 `stock-quantity.v1` 认证。

单位研究状态仅在已核验的 peer/category/市场/证券/日期样例中返回
`research_verified_not_registered`，其来源原值按股解释、倍数1；
其余身份返回 `unverified`。实验原值不得直接冒充标准认证股数。
完整单位说明见 [核验文档](../../docs/minute-volume-unit-evidence-2026-11-08.md)。
正式每日接入与落库仍是 Proposed ADR-0077，不由本脚本启用。

2026-11-08 本机实测：大东方 SSE:600327，2026-11-03，240根完整，
峰值原值7,260,200，时间09:58。该样例用于消费项目11月4日竞价图本地实验。
本机参考池快照由此前读取的生产池摘要重建，不宣称当前生产池仍未变化，
脚本记录的是传入快照本身的哈希；生产池原始哈希及交叉来源证据保留在核验材料中。
网络历史窗口有限，超出可获取范围时必须保留缺口，不能回退其他日期。

## 消费端绘图口径（所有者于2026-11-08确认）

本段记录消费端展示契约，不在数据中心实现绘图计算或策略。
前一交易日最大一分钟量为M，单位股，保留来源原值；展示上限不能写回M。
实验值仍遵循上文研究状态及正式认证边界。

- 对实际已有的合法竞价快照，逐时点计算C(t)=撮合量(t)+同点未匹配量(t)，
  Cmax取这些同点合计的最大值；不能把两个不同时点的各自最大值相加。
- 初始上限S=max(M, ceil(Cmax×11/10))，向上取整到股。盘中只使用已出现的快照，
  每次更新S取上一上限与新计算上限的最大值；后续数据减少时不缩小，不读取未来数据。
- 整张图用同一个S作为1倍，十格等分，每格0.1倍；撮合量从底边向上，
  未匹配量从顶边向下，共用该尺度。S扩大后统一重画，不截断、不画超出1倍标记。
- 缺失时点不补零；09:25:20开盘后的普通盘口不当作竞价未匹配量。

核对样例：大东方2026-11-04，M取2026-11-03的7,260,200股。
同点合计最大在09:15:20：撮合量2,684,300股+买方未匹配量12,977,600股
=15,661,900股，S=17,228,090股，每格1,722,809股。
这是本地展示口径同步，不修改915/923分歧量公式、策略门槛、生产数据或当前指标入口。

## 验证记录

验证：`uv run pytest tests/test_max_minute_volume_probe.py -q -p no:cacheprovider`。
退出码0表示完整实验摘要，2表示不完整，1表示读取/输入失败；
失败仅打印错误类型，避免泄露连接配置或本地路径。

本次本地验证记录：

- 上述聚焦测试20项通过。
- `uv run ruff format --check .`：639个文件通过。
- `uv run ruff check .`：通过。
- `uv run mypy src`：189个源文件通过。
- `uv run pytest -p no:cacheprovider`：2264 passed，239 skipped，228.54秒。
  禁用pytest缓存是因为本机`.pytest_cache`写入权限受限，不改变测试选择。
  PostgreSQL测试因未配置隔离`TEST_DATABASE_URL`跳过，另有3项本机符号链接能力测试跳过。
  `uv run pytest -m integration`未另行执行：没有隔离数据库，本实验也不新增数据库路径。
