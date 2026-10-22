# ADR-0024：远程 TDX 日 K 数据源

- 状态：Accepted（显式 endpoint 配置与禁止运行时发现的决定已由 ADR-0026 取代）
- 日期：2026-08-09
- 关联 Issue：#37
- 取代：ADR-0004 中普通股票 Daily Bar 使用本地 `vipdoc` Reader 的决定
- 澄清：ADR-0005 的 Daily Bar 路由仍固定为 `pytdx`，但实现改为远程 TDX 节点
- 后续决定：ADR-0026 统一节点池取代本文的 endpoint 配置与节点发现限制；未复权日 K、
  Raw lineage、有限连接 failover、单批次固定 endpoint 和缺口可见语义继续有效

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

## 2026-08-11 Accepted clarification：本地优先模式

- 当 `PYTDX_VIPDOC_PATH` 指向有效的通达信 `vipdoc` 目录时，Daily Bar 采集进入**本地优先模式**：
  先读取本地 `.day` 二进制文件（`pytdx.local_daily_bar.v2` Raw schema），仅当本地文件缺失时
  才 fallback 到远程 TDX 节点。
- 本地优先模式下，`PYTDX_DAILY_BAR_ENDPOINTS` 和 IP 池文件均为可选。若未配置任何远程
  endpoint，Provider 在本地文件缺失时返回显式 `ProviderRequestUnavailable`（可见缺口），
  不伪造数据。
- 本地优先模式不改变 Daily Bar 领域语义、Decimal 精度、自然键、Raw 可重放性和缺口可见性。
  Raw schema `pytdx.local_daily_bar.v2` 行记录已在 ADR-0024 主体中声明可重放。
- 决策第 8 条中"`PYTDX_VIPDOC_PATH` 从 Worker 和 Linux 发布配置删除"仅适用于无本地通达信
  安装的 Linux 部署；Windows 或挂载了通达信目录的环境可设置 `PYTDX_VIPDOC_PATH` 启用本地优先。
- 分类路由不受影响：Classification 仍由 AKShare 提供，不使用 pytdx 本地分类文件。

## 2026-10-23 实现澄清：标准协议兼容与精确单位（Issue #90）

- category `9`、未复权语义、有界分页和单市场固定 endpoint 保持不变。部分旧节点虽然可连接，
  却只返回 2 字节、声称含 800 条记录的空壳响应；这属于协议错误，不能解释为合法空页、
  截取为部分成功记录或据此填写停牌。受控补充节点与 pytdx 内置候选共同按 ADR-0026 探测。
- 日线协议解码器直接把整数价格及编码量额解为 Decimal，拒绝截断、非法日期和多余字节。
  节点探测与实际采集共用解码器；探测读取真实 100 条请求页，不能仅以 TCP 连通判定能力。
- 新远程 Raw 为 `pytdx.remote_daily_bar.v2`：保留来源成交量的“手”值，request metadata 显式
  记录 `volume_unit=lot`、`volume_lot_size=100`；标准化时精确乘 100 转成股，非整数股阻断。
  成交额单位仍为 CNY。来源若仅提供整手精度，不推测缺少的零股；因此可能与本地 `.day`
  的逐股成交量相差不足一手。
- 旧远程 v1 及本地 v1/v2 Raw 保持原有重放行为，不静默修正存量 Core 或覆盖历史 Raw。
  存量远程成交量的核查和纠正必须作为独立、显式授权的恢复操作。

## 验收

### 2026-10-23 本地昨收上下文实现澄清

本地 `.day` 读取必须在裁剪目标日期前计算最近较早来源记录的收盘价，作为既有
`previous_close` 语义的输入。上下文记录不写入目标批次；缺少上下文保持 `None`。
本地 v2 Raw 字段和旧批次重放行为不变。已落库的空字段必须通过明确授权的重新采集恢复，
并核对其他行情字段未变化；普通只检查自然键缺口的批量任务不会自动修复它。

- Mock 测试覆盖配置解析、连接失败与有限 failover、读取失败、分页、BSE、Raw replay、
  单 endpoint lineage 和关闭连接。
- Mock 测试覆盖本地优先模式：vipdoc_path 设置时本地 .day 文件优先、本地文件缺失时
  返回 ProviderRequestUnavailable、无远程 endpoint 时本地-only 模式正常工作。
- Linux 环境模板、systemd、smoke、README 与发布手册不再要求 `vipdoc`。
- Ruff、mypy 与完整单元测试通过；不得在测试或部署准备中触发生产采集。
