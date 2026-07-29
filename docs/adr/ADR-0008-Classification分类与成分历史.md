# ADR-0008：Classification 分类与成分历史

- 状态：Accepted
- 日期：2026-07-29
- 关联 Issue：#11
- 实现澄清：2026-07-29 增加本地通达信分类 Adapter

## 背景

行业、概念和指数分类用于按板块聚合行情与后续市场统计。分类本身不是可交易证券，第三方接口当前只提供“此刻的完整目录”和“此刻的完整成分”，不能据此伪造历史生效区间。系统还需要区分不同分类体系，允许同一个分类代码在来源修订后幂等更新，并保证成员只引用已知 Security。

## 决策

1. 新增 Classification 领域，支持 `industry`、`concept`、`index` 三类；AKShare 实现东方财富行业和概念，本地 pytdx Adapter 实现通达信行业和概念。
2. 分类身份由 `(namespace, classification_type, classification_code)` 表达。`namespace` 是分类体系命名空间，东方财富固定为 `eastmoney`，本地通达信固定为 `tdx`；`source_code` 与 `ingestion_id` 只用于追溯，不参与自然键。
3. 当前型接口保存两种相互独立的完整快照：
   - 目录快照自然键为 `(namespace, classification_type, snapshot_date)`；
   - 成分快照自然键为 `(namespace, classification_type, classification_code, snapshot_date)`。
4. 目录项保存 `name`、`level` 和可空 `parent_code`。父级必须存在于同一完整目录快照，分类不能成为自己的父级。
5. 完整快照必须有头记录和计数，因此空目录或空成分仍是可区分的成功事实，不与“尚未采集”混淆。
6. 成分快照必须引用同一日期、同一命名空间和类型的目录项；成员必须引用已存在的统一 Security `symbol`。未知分类、未知证券、重复成员或冲突目录项阻断整个快照写入并生成质量结果。
7. 同一自然键再次采集视为来源修订：在一个事务中更新快照头、UPSERT 当前目录项并删除真正消失的目录项，或替换完整成员集合。重复写入相同内容不产生重复事实，也不删除仍有效的成员快照。
8. 任意日期 `D` 的当前型分类重建规则是选择 `snapshot_date <= D` 的最新完整快照。没有快照的日期返回未知，不向前或向后猜测。
9. 为未来能够提供真实历史生效日的来源预留独立 `member_interval` 事实，区间自然键为 `(namespace, classification_type, classification_code, symbol, valid_from)`；同一成员的有效区间不得重叠。当前东方财富 Adapter 不写该表。
10. 全流程执行 Raw → Normalizer → Validator → `IngestionEnvelope` → Persistence，并支持从原 RawManifest 重放。Provider 专用中文列名和通达信文件格式止于 Adapter 边界。
11. 分类目录和成分通过 `api_v1` 只读 View 暴露；内部表启用 RLS。第一阶段仍不引入 FastAPI。
12. 同花顺 `883423`“昨日涨停”动态板块指数继续由 ADR-0003 的 BoardIndex 边界负责，不属于本 ADR 的 Classification 实现。

## 结果

- 可按采集日期保存并重建东方财富行业、概念目录和成分。
- 分类体系与证券身份分离，多来源扩展不会把来源字段扩散到领域层。
- 当前快照和真实有效区间不会混用，无法证明的历史不会被伪造。
- 同日修订、Raw 重放和完整快照替换具有确定的幂等语义。
- 外网不可用时可从 `T0002/hq_cache/tdxhy.cfg`、`tdxzs*.cfg` 和 `infoharbor_block.dat` 读取本地行业与概念快照；未知 Security 仍阻断完整快照，不被静默过滤。

## 参考

- AKShare `stock_board_industry_name_em` / `stock_board_industry_cons_em`
- AKShare `stock_board_concept_name_em` / `stock_board_concept_cons_em`
- 通达信本地 `tdxhy.cfg`、`tdxzs.cfg`、`tdxzs3.cfg`、`infoharbor_block.dat`
