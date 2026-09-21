# 领域详设：DragonTiger 游资席位画像 v1

> 状态：设计已批准，待实施
> 日期：2026-09-13
> 关联 Issue：#79
> 上级决策：`adr/ADR-0055-Tushare龙虎榜历史与游资席位画像.md`（Accepted）

## 1. 目标和边界

本设计补充最近两年完全缺失的龙虎榜交易日，建立人工审核的游资—席位关系，并把既有时点安全
席位画像算法物化为可查询数据。它不修改 DragonTiger 事件和席位行为的来源事实语义，也不产生
主观资金质量评分。

```text
EastMoney 日常采集 ───────────────┐
                                   ├─> DragonTigerEvent + SeatTrade
Tushare 缺失日期独立修复 -> Raw ──┘                 │
                                                     ├─> SeatOutcome
版本化人工游资清单 -> 审核映射 -> TradingSeat ──────┤
                                                     └─> TradingSeatProfileDaily
                                                                  │
                                              有界客观组成查询 <───┘
```

## 2. 历史修复

修复范围是运行日向前两个自然年的交易日，到最近一个已收盘交易日为止。候选日期必须来自
`CN_A_SHARE` 交易日历。只有该日期不存在任何成功 DragonTiger 标准事实时才调用 Tushare；部分覆盖
日期保持显式质量状态，不跨来源补行。

每个日期使用 `top_list` 和 `top_inst` 构成一个原子 Provider 批次。任一接口失败、日期不一致、未知
证券、未知原因或无法连接明细时，该日期失败且继续下一日期。Raw、manifest 和稳定错误码必须在
标准事实失败时仍可追溯。重复执行对成功日期零新增。

## 3. 游资目录

### 3.1 HotMoneyActor

- `actor_id: UUID`
- `actor_code: text`，仓库稳定编码且唯一
- `canonical_name: text`
- `aliases: text[]`
- `is_active: bool`
- `created_at / updated_at`

### 3.2 HotMoneySeatMapping

- `mapping_id: UUID`
- `actor_id: UUID`
- `seat_id: UUID`
- `valid_from / valid_to: date | None`
- `source_alias_name: text`
- `evidence_note: text`
- `review_status: APPROVED | REJECTED | PENDING`
- `reviewed_at: timestamptz | None`
- `catalog_version: text`

同一游资可以在不重叠的有效期内对应多个席位，同一席位也可保存历史映射，但产生歧义的重叠
`APPROVED` 映射必须失败。只有 `APPROVED` 且交易日在有效期内的记录参与公共查询。清单以仓库内
版本化数据文件维护，由显式同步命令校验后写入；不开放远程修改 API。

Tushare 只有席位名称时不自动创建 `seat_id`。`hm_list` 提供游资名录及其来源机构关系；`hm_detail`
提供逐日股票、买入额和卖出额证据。金额按已验证的来源披露精度四舍五入到 100 元；只有
`(trade_date, symbol, buy_amount, sell_amount)` 与现有 `SeatTrade` 中带稳定 `seat_id` 的事实在该
精度上精确且唯一匹配时，才可为该关系生成审核通过的候选映射。
简称或相似名称不参与匹配；无匹配、多候选、同一来源机构落到多个席位或同一席位落到多个游资时
跳过并记录计数。候选清单写入仓库接受版本审查后，才由显式同步命令原子发布；泛化“机构专用”等
名称仍禁止映射为游资。

## 4. 席位收益和画像

有效买入观察满足：`buy_amount > 0`，并且 `sell_amount` 为空、为零或 `buy_amount > sell_amount`。
纯卖出、净卖出和买卖金额均缺失的观察不进入样本。事件收盘价是基准价；T+1/T+3/T+5 使用
`CN_A_SHARE` 后续交易日对应的未复权 `core.daily_bar.close`。

```text
return_h = close(T+h) / event_close - 1
win_h = return_h > 0
```

缺少事件收盘价、目标交易日或目标日 K 时，不生成该 horizon 的 Outcome。画像按
`seat_id + as_of_date + algorithm_version` 唯一，保存：

- 累计有效买入次数、买入额和卖出额；
- T+1/T+3/T+5 的样本数、胜率和平均收益；
- 使用的收益定义、胜负定义和输入水位线；
- 计算批次及创建时间。

只有 `label_available_date <= as_of_date` 的 Outcome 可以进入画像。事件日的资金组成查询只能关联
严格早于事件日的画像，避免未来数据泄漏。算法口径变化必须发布新版本，不能改写旧版本。

## 5. Worker 和应用服务

- `dragon_tiger_history_repair`：显式 CLI，接受有界日期范围、dry-run 和执行确认；逐交易日提交。
- `hot_money_catalog_sync`：显式 CLI，读取版本化清单，先校验再原子发布，不进入每日调度。
- `hot_money_catalog_build`：显式 CLI，按月读取不超过 730 个自然日的 `hm_list` / `hm_detail`，仅以
  精确唯一的交易事实匹配生成候选 JSON；不直接写数据库、不进入每日调度。
- `dragon_tiger_seat_profile_daily`：Worker 代码任务，交易日收盘且所需日线可用后运行；幂等重算当日
  画像，失败可重试且不影响 DragonTiger 原始采集。

任务记录候选数、成功数、跳过数、失败数和质量码。日志不得包含 token、数据库地址、来源完整负载
或人工证据中的敏感信息。

## 6. 公共读取

新增单一 RPC/FastAPI 查询，以必传 `trade_date` 和六位股票代码为输入。返回：

- 事件原因、窗口、披露完整性和质量码；
- 龙虎榜买入、卖出、净额、净买强度及 top1/top3/top5 集中度；
- 机构和北向买入、卖出与可计算净额；
- 事件中审核通过的游资及其席位、买卖金额；
- 每个稳定席位严格早于事件日的最新画像，包括三个 horizon 的样本数、胜率和平均收益；
- 算法版本、画像日期和输入水位线。

查询不产生总评分、不回退其他交易日，结果有数量上限和 5 秒数据库超时。无当日事件返回未找到。
FastAPI 只能调用 `api_v1` RPC。

2026-09-18 补充一个精确日期只读查询，固定返回温州帮、欢乐海岸、鑫多多、歌神、小棉袄、
炒股养家和方新侠在当日有效 `APPROVED` 映射下的席位行为。结果按游资、股票和稳定席位保留，
完全相同的金额披露去重；买卖任一金额缺失时净买额为空。查询不扩展名录、不推断身份、不回退日期。

## 7. 验收

- Tushare 两接口、单位、空值、未知原因、未知证券、Raw replay 和单日原子失败测试；
- 完全缺失日期才修复、部分日期不拼接、成功日期幂等跳过测试；
- 游资映射有效期、审核状态、歧义阻断、同名不合并和泛化席位禁止映射测试；
- 有效买入筛选、三个 horizon、停牌/缺 K 线、严格时点和算法版本测试；
- migration、RLS、Worker 权限、API 角色隔离、RPC 上限/超时及三份契约测试；
- 生产单日冒烟、两年覆盖报告和无凭据泄漏检查。
