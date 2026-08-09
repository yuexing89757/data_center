# ADR-0024：远程 TDX 日 K 数据源

- 状态：Accepted
- 日期：2026-08-09
- 关联 Issue：#37
- 取代：ADR-0004 中普通股票 Daily Bar 使用本地 `vipdoc` Reader 的决定
- 澄清：ADR-0005 的 Daily Bar 路由仍固定为 `pytdx`，但实现改为远程 TDX 节点

## 背景

Linux Worker 无法挂载原 Windows 通达信目录。pytdx 同时支持本地文件和 TDX 行情协议，
因此需要在不改变未复权 Daily Bar 领域语义、Raw 可重放性和缺口可见性的前提下，改为访问
显式配置的互联网 TDX 节点。公共节点没有 SLA，可能限流、拒绝连接、缺少北交所数据或
返回不完整历史，不能把网络成功等同于数据完整。

## 决策

1. `pytdx` 普通股票 Daily Bar 只使用 `PYTDX_DAILY_BAR_ENDPOINTS` 中按顺序配置的
   `host:port` 节点；禁止运行时扫描、测速发现或使用库内置的动态节点目录。
2. 建立 Provider 会话时按顺序尝试节点，总尝试次数由
   `PYTDX_DAILY_BAR_MAX_ATTEMPTS` 限制，连接/读取超时由
   `PYTDX_DAILY_BAR_TIMEOUT_SECONDS` 限制。Router 现有连续三次 ProviderError 熔断继续生效。
3. 一旦会话连接成功，该 Provider 实例只使用一个节点。读取失败时整个请求失败并关闭
   会话，不在成功批次中途切换节点；下一个独立尝试才可重新选择节点。
4. SSE 使用 TDX market `1`，SZSE 与 BSE 使用 market `0`。若配置节点不提供 BSE 或
   请求区间无记录，返回显式 `ProviderRequestUnavailable`，不得用其他网络 Provider 补齐。
5. 仅请求未复权日线 category `9`，分页大小与页数均有上限。价格、成交量和成交额在
   Provider 边界标准化；Domain 继续使用 Decimal、股和 CNY。
6. Raw 保存字符串化 TDX 行记录，request metadata 保存实际 endpoint、market、分页参数
   和 `adjust=none`。Domain Record 只保留 `source_code=pytdx`，不泄漏 endpoint 字段。
7. 新 Raw schema 为 `pytdx.remote_daily_bar.v1`；旧本地 Raw schema v1/v2 继续可重放。
8. `PYTDX_VIPDOC_PATH` 从 Worker 和 Linux 发布配置删除。原来依赖本地 TDX 分类文件的
   Classification 路由不再使用 pytdx，改为 AKShare。

## 后果与限制

- Linux 不再需要通达信安装或只读挂载，但必须允许 Worker 向配置节点的 TCP 端口出站。
- 节点列表由运维维护；失效、限流和数据覆盖差异会形成失败或可见缺口，不静默仲裁。
- endpoint 是来源审计元数据，不改变 `provider_code=pytdx` 和领域自然键。
- 无生产节点可用性保证；上线前必须对每个配置节点分别验证 SSE、SZSE，并在需要 BSE 时
  验证北交所样本。探测只用于运维验收，不进入运行时发现逻辑。

## 验收

- Mock 测试覆盖配置解析、连接失败与有限 failover、读取失败、分页、BSE、Raw replay、
  单 endpoint lineage 和关闭连接。
- Linux 环境模板、systemd、smoke、README 与发布手册不再要求 `vipdoc`。
- Ruff、mypy 与完整单元测试通过；不得在测试或部署准备中触发生产采集。
