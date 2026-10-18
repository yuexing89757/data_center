-- 中文数据字典：仅补充业务表及字段注释，不修改事实、结构、约束或权限。
-- 保留已有注释；NULL 继续表示缺失，不等同于零。
-- 公共 api_v1 视图/RPC 及扩展、迁移历史、调度器内部表不属于本迁移范围。

-- audit.quality_archive
comment on table audit.quality_archive is '质量结果无损归档清单；保存原文与压缩对象校验信息及原始质量记录身份。';
comment on column audit.quality_archive.archive_id is '质量归档批次标识。';
comment on column audit.quality_archive.ingestion_id is '采集批次标识，关联 ingestion.ingestion_run，用于追溯来源和 Raw 对象。';
comment on column audit.quality_archive.rule_code is '质量或计算规则代码。';
comment on column audit.quality_archive.schema_version is '原始数据或归档记录格式版本。';
comment on column audit.quality_archive.object_path is '受控存储根下的相对对象定位；不是客户端可读取的数据库路径。';
comment on column audit.quality_archive.content_sha256 is '未压缩原始内容的SHA-256摘要。';
comment on column audit.quality_archive.content_bytes is '未压缩原始内容字节数。';
comment on column audit.quality_archive.compressed_sha256 is '压缩对象的SHA-256摘要。';
comment on column audit.quality_archive.compressed_bytes is '压缩对象字节数。';
comment on column audit.quality_archive.row_count is '对象中的逻辑记录行数。';
comment on column audit.quality_archive.quality_ids is '归档包含的原始质量记录标识集合。';
comment on column audit.quality_archive.first_created_at is '归档内最早质量记录的创建时间（带时区）。';
comment on column audit.quality_archive.last_created_at is '归档内最晚质量记录的创建时间（带时区）。';
comment on column audit.quality_archive.archived_at is '归档完成时间（带时区）。';

-- audit.quality_result
comment on table audit.quality_result is '数据质量规则执行结果；记录自然键、严重级别、结论及诊断详情。';
comment on column audit.quality_result.quality_result_id is '质量规则执行结果标识。';
comment on column audit.quality_result.ingestion_id is '采集批次标识，关联 ingestion.ingestion_run，用于追溯来源和 Raw 对象。';
comment on column audit.quality_result.dataset_code is '受控采集数据集代码。';
comment on column audit.quality_result.rule_code is '质量或计算规则代码。';
comment on column audit.quality_result.severity is '质量问题严重级别。';
comment on column audit.quality_result.status is '记录或运行状态；允许值及终态约束见本表 CHECK 约束。';
comment on column audit.quality_result.natural_key is '被校验领域记录的自然键。';
comment on column audit.quality_result.message is '可读结果或诊断说明。';
comment on column audit.quality_result.details is '结构化质量诊断详情，不含凭据。';
comment on column audit.quality_result.created_at is '记录首次写入时间（带时区）。';

-- billboard.dragon_tiger_event
comment on table billboard.dragon_tiger_event is '龙虎榜上榜事件来源事实；区分触发窗口与披露金额统计期间。';
comment on column billboard.dragon_tiger_event.event_id is '来源事件唯一标识。';
comment on column billboard.dragon_tiger_event.symbol is '标准证券代码，格式如 SSE:600000、SZSE:000001、BSE:920000；保留代码前导零。';
comment on column billboard.dragon_tiger_event.market is '统一交易日历市场代码 CN_A_SHARE。';
comment on column billboard.dragon_tiger_event.trade_date is '交易所本地交易日期；以统一交易日历为准。';
comment on column billboard.dragon_tiger_event.reason_id is '龙虎榜标准上榜原因标识。';
comment on column billboard.dragon_tiger_event.reason_name_raw is '来源返回的上榜原因原文。';
comment on column billboard.dragon_tiger_event.close_price is '来源收盘价，人民币元。';
comment on column billboard.dragon_tiger_event.change_pct is '涨跌幅，百分点。';
comment on column billboard.dragon_tiger_event.turnover_amount is '成交金额，人民币元。';
comment on column billboard.dragon_tiger_event.turnover_rate is '换手率，百分点。';
comment on column billboard.dragon_tiger_event.amplitude is '振幅，百分点。';
comment on column billboard.dragon_tiger_event.lhb_buy_amount is '龙虎榜披露买入金额，人民币元。';
comment on column billboard.dragon_tiger_event.lhb_sell_amount is '龙虎榜披露卖出金额，人民币元。';
comment on column billboard.dragon_tiger_event.source_code is '实际数据来源代码；单个成功采集批次不合并多个来源。';
comment on column billboard.dragon_tiger_event.source_record_id is '来源系统记录标识。';
comment on column billboard.dragon_tiger_event.ingestion_id is '采集批次标识，关联 ingestion.ingestion_run，用于追溯来源和 Raw 对象。';
comment on column billboard.dragon_tiger_event.content_hash is '规范化记录或快照内容的校验摘要。';
comment on column billboard.dragon_tiger_event.created_at is '记录首次写入时间（带时区）。';
comment on column billboard.dragon_tiger_event.updated_at is '记录最后更新时间（带时区）。';
comment on column billboard.dragon_tiger_event.trigger_window_basis is '上榜触发窗口依据；未知时为空，不从金额期间推断。';
comment on column billboard.dragon_tiger_event.trigger_window_sessions is '触发窗口交易日数。';
comment on column billboard.dragon_tiger_event.trigger_occurrence_count is '触发窗口内事件发生次数。';
comment on column billboard.dragon_tiger_event.trigger_start_date is '上榜触发窗口起始日期。';
comment on column billboard.dragon_tiger_event.trigger_end_date is '上榜触发窗口结束日期。';
comment on column billboard.dragon_tiger_event.amount_period_basis is '披露成交金额统计期间依据。';
comment on column billboard.dragon_tiger_event.amount_period_sessions is '披露成交金额统计交易日数。';
comment on column billboard.dragon_tiger_event.amount_period_start_date is '披露成交金额统计起始日期。';
comment on column billboard.dragon_tiger_event.amount_period_end_date is '披露成交金额统计结束日期。';
comment on column billboard.dragon_tiger_event.buy_disclosure_present is '是否存在买方成交披露；缺失不以零金额替代。';
comment on column billboard.dragon_tiger_event.sell_disclosure_present is '是否存在卖方成交披露；缺失不以零金额替代。';

-- billboard.dragon_tiger_reason
comment on table billboard.dragon_tiger_reason is '龙虎榜上榜原因标准目录；区分原因类型和统计期间。';
comment on column billboard.dragon_tiger_reason.reason_id is '龙虎榜标准上榜原因标识。';
comment on column billboard.dragon_tiger_reason.reason_code is '龙虎榜标准上榜原因代码。';
comment on column billboard.dragon_tiger_reason.reason_name is '标准上榜原因名称。';
comment on column billboard.dragon_tiger_reason.reason_type is '上榜原因类型。';
comment on column billboard.dragon_tiger_reason.description is '标准目录条目的解释说明。';
comment on column billboard.dragon_tiger_reason.is_active is '目录条目是否有效。';
comment on column billboard.dragon_tiger_reason.created_at is '记录首次写入时间（带时区）。';
comment on column billboard.dragon_tiger_reason.updated_at is '记录最后更新时间（带时区）。';

-- billboard.hot_money_actor
comment on column billboard.hot_money_actor.actor_id is '人工审核游资主体标识。';
comment on column billboard.hot_money_actor.actor_code is '游资主体稳定目录代码。';
comment on column billboard.hot_money_actor.canonical_name is '规范化主体或席位名称。';
comment on column billboard.hot_money_actor.aliases is '人工维护的主体别名集合。';
comment on column billboard.hot_money_actor.is_active is '目录条目是否有效。';
comment on column billboard.hot_money_actor.created_at is '记录首次写入时间（带时区）。';
comment on column billboard.hot_money_actor.updated_at is '记录最后更新时间（带时区）。';

-- billboard.hot_money_seat_mapping
comment on column billboard.hot_money_seat_mapping.mapping_id is '主体与席位映射标识。';
comment on column billboard.hot_money_seat_mapping.actor_id is '人工审核游资主体标识。';
comment on column billboard.hot_money_seat_mapping.seat_id is '稳定交易席位标识。';
comment on column billboard.hot_money_seat_mapping.valid_from is '有效起始日期；为空时的无界语义由所属领域定义。';
comment on column billboard.hot_money_seat_mapping.valid_to is '有效结束日期；为空时的无界语义由所属领域定义。';
comment on column billboard.hot_money_seat_mapping.source_alias_name is '审核依据中的来源别名。';
comment on column billboard.hot_money_seat_mapping.evidence_note is '人工映射依据说明。';
comment on column billboard.hot_money_seat_mapping.review_status is '人工审核状态：PENDING、APPROVED或REJECTED。';
comment on column billboard.hot_money_seat_mapping.reviewed_at is '人工审核完成时间（带时区）。';
comment on column billboard.hot_money_seat_mapping.catalog_version is '人工审核目录版本。';
comment on column billboard.hot_money_seat_mapping.created_at is '记录首次写入时间（带时区）。';

-- billboard.reason_source_alias
comment on table billboard.reason_source_alias is '龙虎榜上榜原因来源映射；关联来源原因与标准原因身份。';
comment on column billboard.reason_source_alias.source_code is '实际数据来源代码；单个成功采集批次不合并多个来源。';
comment on column billboard.reason_source_alias.source_reason_code is '来源系统上榜原因代码。';
comment on column billboard.reason_source_alias.source_reason_name is '来源系统上榜原因名称。';
comment on column billboard.reason_source_alias.reason_id is '龙虎榜标准上榜原因标识。';
comment on column billboard.reason_source_alias.first_seen_at is '首次观测到该来源映射的时间（带时区）。';
comment on column billboard.reason_source_alias.last_seen_at is '最近观测到该来源映射的时间（带时区）。';

-- billboard.seat_trade
comment on table billboard.seat_trade is '龙虎榜席位成交披露事实；买卖方向缺失保留为空，不以零代替。';
comment on column billboard.seat_trade.seat_trade_id is '席位成交披露记录标识。';
comment on column billboard.seat_trade.event_id is '来源事件唯一标识。';
comment on column billboard.seat_trade.source_code is '实际数据来源代码；单个成功采集批次不合并多个来源。';
comment on column billboard.seat_trade.source_event_id is '来源系统事件标识。';
comment on column billboard.seat_trade.source_record_id is '来源系统记录标识。';
comment on column billboard.seat_trade.symbol is '标准证券代码，格式如 SSE:600000、SZSE:000001、BSE:920000；保留代码前导零。';
comment on column billboard.seat_trade.trade_date is '交易所本地交易日期；以统一交易日历为准。';
comment on column billboard.seat_trade.seat_id is '稳定交易席位标识。';
comment on column billboard.seat_trade.seat_source_key is '本条披露中的来源席位身份键。';
comment on column billboard.seat_trade.seat_name_raw is '来源返回的交易席位名称原文。';
comment on column billboard.seat_trade.buy_amount is '披露买入金额，人民币元；未披露时为空。';
comment on column billboard.seat_trade.sell_amount is '披露卖出金额，人民币元；未披露时为空。';
comment on column billboard.seat_trade.buy_rank is '买入金额披露排名，1至5；未披露时为空。';
comment on column billboard.seat_trade.sell_rank is '卖出金额披露排名，1至5；未披露时为空。';
comment on column billboard.seat_trade.is_institution is '来源是否标记为机构专用席位。';
comment on column billboard.seat_trade.is_northbound is '来源是否标记为沪深股通席位。';
comment on column billboard.seat_trade.ingestion_id is '采集批次标识，关联 ingestion.ingestion_run，用于追溯来源和 Raw 对象。';
comment on column billboard.seat_trade.content_hash is '规范化记录或快照内容的校验摘要。';
comment on column billboard.seat_trade.created_at is '记录首次写入时间（带时区）。';

-- billboard.trading_seat
comment on table billboard.trading_seat is '龙虎榜稳定交易席位主数据；保存规范名称、券商和营业部信息。';
comment on column billboard.trading_seat.seat_id is '稳定交易席位标识。';
comment on column billboard.trading_seat.canonical_name is '规范化主体或席位名称。';
comment on column billboard.trading_seat.broker_name is '证券公司名称。';
comment on column billboard.trading_seat.branch_name is '营业部名称。';
comment on column billboard.trading_seat.seat_type is '交易席位类型。';
comment on column billboard.trading_seat.province is '席位所属省级行政区域；未知时为空。';
comment on column billboard.trading_seat.city is '席位所属城市；未知时为空。';
comment on column billboard.trading_seat.first_seen_date is '首次观测到该席位身份的交易日期。';
comment on column billboard.trading_seat.last_seen_date is '最近观测到该席位身份的交易日期。';
comment on column billboard.trading_seat.is_active is '目录条目是否有效。';
comment on column billboard.trading_seat.created_at is '记录首次写入时间（带时区）。';
comment on column billboard.trading_seat.updated_at is '记录最后更新时间（带时区）。';

-- billboard.trading_seat_alias
comment on table billboard.trading_seat_alias is '龙虎榜交易席位来源别名；关联稳定席位与来源身份。';
comment on column billboard.trading_seat_alias.alias_id is '交易席位来源别名标识。';
comment on column billboard.trading_seat_alias.alias_name is '来源席位别名。';
comment on column billboard.trading_seat_alias.created_at is '记录首次写入时间（带时区）。';
comment on column billboard.trading_seat_alias.identity_id is '稳定来源身份映射标识。';
comment on column billboard.trading_seat_alias.first_seen_date is '首次观测到该席位身份的交易日期。';
comment on column billboard.trading_seat_alias.last_seen_date is '最近观测到该席位身份的交易日期。';

-- billboard.trading_seat_profile_daily
comment on column billboard.trading_seat_profile_daily.seat_id is '稳定交易席位标识。';
comment on column billboard.trading_seat_profile_daily.as_of_date is '画像统计截止日期；不使用此日期之后的输入。';
comment on column billboard.trading_seat_profile_daily.algorithm_version is '计算算法版本；用于重现结果。';
comment on column billboard.trading_seat_profile_daily.metric_definition is '画像指标口径定义版本。';
comment on column billboard.trading_seat_profile_daily.return_definition is '画像收益率计算口径版本。';
comment on column billboard.trading_seat_profile_daily.participation_definition is '连续参与统计口径版本。';
comment on column billboard.trading_seat_profile_daily.total_lhb_count is '截至统计日累计龙虎榜参与次数。';
comment on column billboard.trading_seat_profile_daily.total_buy_amount is '截至统计日披露买入金额合计，人民币元。';
comment on column billboard.trading_seat_profile_daily.total_sell_amount is '截至统计日披露卖出金额合计，人民币元。';
comment on column billboard.trading_seat_profile_daily.t1_sample_count is 'T+1交易日表现的有效样本数。';
comment on column billboard.trading_seat_profile_daily.t1_win_rate is 'T+1交易日胜率，小数比例，范围0至1；无样本时为空。';
comment on column billboard.trading_seat_profile_daily.t1_avg_return is 'T+1交易日平均收益率，小数比例；无样本时为空。';
comment on column billboard.trading_seat_profile_daily.t3_sample_count is 'T+3交易日表现的有效样本数。';
comment on column billboard.trading_seat_profile_daily.t3_win_rate is 'T+3交易日胜率，小数比例，范围0至1；无样本时为空。';
comment on column billboard.trading_seat_profile_daily.t3_avg_return is 'T+3交易日平均收益率，小数比例；无样本时为空。';
comment on column billboard.trading_seat_profile_daily.t5_sample_count is 'T+5交易日表现的有效样本数。';
comment on column billboard.trading_seat_profile_daily.t5_win_rate is 'T+5交易日胜率，小数比例，范围0至1；无样本时为空。';
comment on column billboard.trading_seat_profile_daily.t5_avg_return is 'T+5交易日平均收益率，小数比例；无样本时为空。';
comment on column billboard.trading_seat_profile_daily.consecutive_participation_sample_count is '连续参与统计有效样本数。';
comment on column billboard.trading_seat_profile_daily.consecutive_participation_rate is '连续参与率，小数比例，范围0至1。';
comment on column billboard.trading_seat_profile_daily.input_watermark_date is '画像输入数据水位日期，不晚于统计截止日期。';
comment on column billboard.trading_seat_profile_daily.calculation_id is '计算批次标识；用于追溯算法版本及冻结输入，同一逻辑结果不混用计算批次。';
comment on column billboard.trading_seat_profile_daily.created_at is '记录首次写入时间（带时区）。';

-- billboard.trading_seat_source_identity
comment on column billboard.trading_seat_source_identity.identity_id is '稳定来源身份映射标识。';
comment on column billboard.trading_seat_source_identity.seat_id is '稳定交易席位标识。';
comment on column billboard.trading_seat_source_identity.source_code is '实际数据来源代码；单个成功采集批次不合并多个来源。';
comment on column billboard.trading_seat_source_identity.source_seat_key is '来源系统席位身份键。';
comment on column billboard.trading_seat_source_identity.first_seen_date is '首次观测到该席位身份的交易日期。';
comment on column billboard.trading_seat_source_identity.last_seen_date is '最近观测到该席位身份的交易日期。';
comment on column billboard.trading_seat_source_identity.created_at is '记录首次写入时间（带时区）。';
comment on column billboard.trading_seat_source_identity.updated_at is '记录最后更新时间（带时区）。';

-- capital.distribution
comment on table capital.distribution is '利润分配与送转事实；保存公告、登记、除权除息及每股分配方案。';
comment on column capital.distribution.symbol is '标准证券代码，格式如 SSE:600000、SZSE:000001、BSE:920000；保留代码前导零。';
comment on column capital.distribution.report_period is '财务报告期截止日期。';
comment on column capital.distribution.announcement_date is '来源公告日期。';
comment on column capital.distribution.record_date is '股权或债权登记日期。';
comment on column capital.distribution.ex_date is '除权除息日期。';
comment on column capital.distribution.cash_dividend_per_share is '每股现金分红，人民币元/股。';
comment on column capital.distribution.bonus_share_ratio is '每股送股比例，以每1股为基数。';
comment on column capital.distribution.transfer_share_ratio is '每股转增比例，以每1股为基数。';
comment on column capital.distribution.status is '记录或运行状态；允许值及终态约束见本表 CHECK 约束。';
comment on column capital.distribution.source_code is '实际数据来源代码；单个成功采集批次不合并多个来源。';
comment on column capital.distribution.ingestion_id is '采集批次标识，关联 ingestion.ingestion_run，用于追溯来源和 Raw 对象。';
comment on column capital.distribution.created_at is '记录首次写入时间（带时区）。';
comment on column capital.distribution.updated_at is '记录最后更新时间（带时区）。';

-- capital.rights_issue
comment on table capital.rights_issue is '配股事实；保存配股比例、价格、股本基数和关键日期。';
comment on column capital.rights_issue.symbol is '标准证券代码，格式如 SSE:600000、SZSE:000001、BSE:920000；保留代码前导零。';
comment on column capital.rights_issue.record_date is '股权或债权登记日期。';
comment on column capital.rights_issue.announcement_date is '来源公告日期。';
comment on column capital.rights_issue.ex_date is '除权除息日期。';
comment on column capital.rights_issue.payment_start_date is '配股缴款开始日期。';
comment on column capital.rights_issue.payment_end_date is '配股缴款结束日期。';
comment on column capital.rights_issue.listing_date is '新增股份上市日期。';
comment on column capital.rights_issue.rights_ratio is '每股配股比例，以每1股为基数。';
comment on column capital.rights_issue.rights_price is '配股价格，人民币元/股。';
comment on column capital.rights_issue.base_shares is '配股股本基数，股。';
comment on column capital.rights_issue.proceeds is '配股募集资金，人民币元。';
comment on column capital.rights_issue.source_code is '实际数据来源代码；单个成功采集批次不合并多个来源。';
comment on column capital.rights_issue.ingestion_id is '采集批次标识，关联 ingestion.ingestion_run，用于追溯来源和 Raw 对象。';
comment on column capital.rights_issue.created_at is '记录首次写入时间（带时区）。';
comment on column capital.rights_issue.updated_at is '记录最后更新时间（带时区）。';

-- capital.share_capital
comment on table capital.share_capital is '证券股本变动事实；按生效日保存总股本、流通及限售股数。';
comment on column capital.share_capital.symbol is '标准证券代码，格式如 SSE:600000、SZSE:000001、BSE:920000；保留代码前导零。';
comment on column capital.share_capital.effective_date is '生效日期。';
comment on column capital.share_capital.total_shares is '总股本，股。';
comment on column capital.share_capital.restricted_shares is '限售股数，股。';
comment on column capital.share_capital.circulating_shares is '流通股本，股。';
comment on column capital.share_capital.listed_a_shares is '已上市流通A股股数，股。';
comment on column capital.share_capital.change_reason is '股本变动原因。';
comment on column capital.share_capital.source_code is '实际数据来源代码；单个成功采集批次不合并多个来源。';
comment on column capital.share_capital.ingestion_id is '采集批次标识，关联 ingestion.ingestion_run，用于追溯来源和 Raw 对象。';
comment on column capital.share_capital.created_at is '记录首次写入时间（带时区）。';
comment on column capital.share_capital.updated_at is '记录最后更新时间（带时区）。';

-- classification.catalog_snapshot
comment on table classification.catalog_snapshot is '分类目录快照；记录指定命名空间、分类类型和快照日期的定义数量。';
comment on column classification.catalog_snapshot.namespace is '分类或板块来源命名空间。';
comment on column classification.catalog_snapshot.classification_type is '分类类型代码。';
comment on column classification.catalog_snapshot.snapshot_date is '来源快照日期。';
comment on column classification.catalog_snapshot.definition_count is '快照内分类定义数量。';
comment on column classification.catalog_snapshot.source_code is '实际数据来源代码；单个成功采集批次不合并多个来源。';
comment on column classification.catalog_snapshot.ingestion_id is '采集批次标识，关联 ingestion.ingestion_run，用于追溯来源和 Raw 对象。';
comment on column classification.catalog_snapshot.created_at is '记录首次写入时间（带时区）。';
comment on column classification.catalog_snapshot.updated_at is '记录最后更新时间（带时区）。';

-- classification.definition_snapshot
comment on table classification.definition_snapshot is '分类定义快照；保存分类代码、名称及父子层级。';
comment on column classification.definition_snapshot.namespace is '分类或板块来源命名空间。';
comment on column classification.definition_snapshot.classification_type is '分类类型代码。';
comment on column classification.definition_snapshot.snapshot_date is '来源快照日期。';
comment on column classification.definition_snapshot.classification_code is '来源命名空间内的分类代码。';
comment on column classification.definition_snapshot.name is '该目录快照中的分类名称。';
comment on column classification.definition_snapshot.level is '分类层级序号。';
comment on column classification.definition_snapshot.parent_code is '上级分类代码；顶层分类可为空。';
comment on column classification.definition_snapshot.source_code is '实际数据来源代码；单个成功采集批次不合并多个来源。';
comment on column classification.definition_snapshot.ingestion_id is '采集批次标识，关联 ingestion.ingestion_run，用于追溯来源和 Raw 对象。';
comment on column classification.definition_snapshot.created_at is '记录首次写入时间（带时区）。';

-- classification.member_interval
comment on table classification.member_interval is '分类成员有效区间；用于按历史日期解析证券归属。';
comment on column classification.member_interval.namespace is '分类或板块来源命名空间。';
comment on column classification.member_interval.classification_type is '分类类型代码。';
comment on column classification.member_interval.classification_code is '来源命名空间内的分类代码。';
comment on column classification.member_interval.symbol is '标准证券代码，格式如 SSE:600000、SZSE:000001、BSE:920000；保留代码前导零。';
comment on column classification.member_interval.valid_from is '有效起始日期；为空时的无界语义由所属领域定义。';
comment on column classification.member_interval.valid_to is '有效结束日期；为空时的无界语义由所属领域定义。';
comment on column classification.member_interval.source_code is '实际数据来源代码；单个成功采集批次不合并多个来源。';
comment on column classification.member_interval.ingestion_id is '采集批次标识，关联 ingestion.ingestion_run，用于追溯来源和 Raw 对象。';
comment on column classification.member_interval.created_at is '记录首次写入时间（带时区）。';
comment on column classification.member_interval.updated_at is '记录最后更新时间（带时区）。';

-- classification.member_snapshot
comment on table classification.member_snapshot is '分类成员快照头；记录某分类在指定快照日的成员数量和采集血缘。';
comment on column classification.member_snapshot.namespace is '分类或板块来源命名空间。';
comment on column classification.member_snapshot.classification_type is '分类类型代码。';
comment on column classification.member_snapshot.classification_code is '来源命名空间内的分类代码。';
comment on column classification.member_snapshot.snapshot_date is '来源快照日期。';
comment on column classification.member_snapshot.member_count is '快照或聚合范围内的成员数量。';
comment on column classification.member_snapshot.source_code is '实际数据来源代码；单个成功采集批次不合并多个来源。';
comment on column classification.member_snapshot.ingestion_id is '采集批次标识，关联 ingestion.ingestion_run，用于追溯来源和 Raw 对象。';
comment on column classification.member_snapshot.created_at is '记录首次写入时间（带时区）。';
comment on column classification.member_snapshot.updated_at is '记录最后更新时间（带时区）。';

-- classification.member_snapshot_item
comment on table classification.member_snapshot_item is '分类成员快照明细；记录指定分类快照包含的证券。';
comment on column classification.member_snapshot_item.namespace is '分类或板块来源命名空间。';
comment on column classification.member_snapshot_item.classification_type is '分类类型代码。';
comment on column classification.member_snapshot_item.classification_code is '来源命名空间内的分类代码。';
comment on column classification.member_snapshot_item.snapshot_date is '来源快照日期。';
comment on column classification.member_snapshot_item.symbol is '标准证券代码，格式如 SSE:600000、SZSE:000001、BSE:920000；保留代码前导零。';
comment on column classification.member_snapshot_item.source_code is '实际数据来源代码；单个成功采集批次不合并多个来源。';
comment on column classification.member_snapshot_item.ingestion_id is '采集批次标识，关联 ingestion.ingestion_run，用于追溯来源和 Raw 对象。';
comment on column classification.member_snapshot_item.created_at is '记录首次写入时间（带时区）。';

-- convertible_bond.bond
comment on table convertible_bond.bond is '可转债主数据；保存正股关联、发行条款、转股条件及生命周期。';
comment on column convertible_bond.bond.symbol is '标准证券代码，格式如 SSE:600000、SZSE:000001、BSE:920000；保留代码前导零。';
comment on column convertible_bond.bond.bond_code is '可转债六位交易代码字符串；保留前导零。';
comment on column convertible_bond.bond.bond_short_name is '可转债简称。';
comment on column convertible_bond.bond.bond_full_name is '可转债全称。';
comment on column convertible_bond.bond.underlying_symbol is '正股标准证券代码，关联 core.security。';
comment on column convertible_bond.bond.exchange is '交易所代码：SSE、SZSE 或 BSE，具体范围由表约束确定。';
comment on column convertible_bond.bond.par_value is '债券面值，人民币元/张。';
comment on column convertible_bond.bond.issue_size is '发行规模，沿用来源的亿元口径。';
comment on column convertible_bond.bond.issue_date is '发行日期。';
comment on column convertible_bond.bond.value_date is '起息日期。';
comment on column convertible_bond.bond.maturity_years is '债券期限，年。';
comment on column convertible_bond.bond.maturity_date is '到期日期。';
comment on column convertible_bond.bond.convert_price_initial is '初始转股价格，人民币元/股。';
comment on column convertible_bond.bond.convert_price is '转股价格，人民币元/股。';
comment on column convertible_bond.bond.convert_start_date is '转股开始日期。';
comment on column convertible_bond.bond.convert_end_date is '转股结束日期。';
comment on column convertible_bond.bond.coupon_rate is '代表性票面利率；当前适配器缺失时保留为空。';
comment on column convertible_bond.bond.redeem_clause is '赎回条款原文。';
comment on column convertible_bond.bond.sell_back_clause is '回售条款原文。';
comment on column convertible_bond.bond.lifecycle_status is '可转债生命周期状态。';
comment on column convertible_bond.bond.source_code is '实际数据来源代码；单个成功采集批次不合并多个来源。';
comment on column convertible_bond.bond.ingestion_id is '采集批次标识，关联 ingestion.ingestion_run，用于追溯来源和 Raw 对象。';
comment on column convertible_bond.bond.created_at is '记录首次写入时间（带时区）。';
comment on column convertible_bond.bond.updated_at is '记录最后更新时间（带时区）。';

-- convertible_bond.call_event
comment on table convertible_bond.call_event is '可转债赎回或回售事件公告事实。';
comment on column convertible_bond.call_event.symbol is '标准证券代码，格式如 SSE:600000、SZSE:000001、BSE:920000；保留代码前导零。';
comment on column convertible_bond.call_event.event_type is '领域事件类型代码。';
comment on column convertible_bond.call_event.announcement_date is '来源公告日期。';
comment on column convertible_bond.call_event.trigger_date is '事件触发日期。';
comment on column convertible_bond.call_event.record_date is '股权或债权登记日期。';
comment on column convertible_bond.call_event.call_price is '赎回或回售价格，人民币元/张。';
comment on column convertible_bond.call_event.status is '记录或运行状态；允许值及终态约束见本表 CHECK 约束。';
comment on column convertible_bond.call_event.source_code is '实际数据来源代码；单个成功采集批次不合并多个来源。';
comment on column convertible_bond.call_event.ingestion_id is '采集批次标识，关联 ingestion.ingestion_run，用于追溯来源和 Raw 对象。';
comment on column convertible_bond.call_event.created_at is '记录首次写入时间（带时区）。';

-- convertible_bond.convert_price_revision
comment on table convertible_bond.convert_price_revision is '可转债转股价调整事实；保存生效日、调整前后价格与原因。';
comment on column convertible_bond.convert_price_revision.symbol is '标准证券代码，格式如 SSE:600000、SZSE:000001、BSE:920000；保留代码前导零。';
comment on column convertible_bond.convert_price_revision.effective_date is '生效日期。';
comment on column convertible_bond.convert_price_revision.convert_price_before is '调整前转股价，人民币元/股。';
comment on column convertible_bond.convert_price_revision.convert_price_after is '调整后转股价，人民币元/股。';
comment on column convertible_bond.convert_price_revision.revision_reason is '转股价格调整原因。';
comment on column convertible_bond.convert_price_revision.announcement_date is '来源公告日期。';
comment on column convertible_bond.convert_price_revision.source_code is '实际数据来源代码；单个成功采集批次不合并多个来源。';
comment on column convertible_bond.convert_price_revision.ingestion_id is '采集批次标识，关联 ingestion.ingestion_run，用于追溯来源和 Raw 对象。';
comment on column convertible_bond.convert_price_revision.created_at is '记录首次写入时间（带时区）。';

-- convertible_bond.daily_bar
comment on table convertible_bond.daily_bar is '可转债未复权日行情；成交量单位为张，成交额单位为人民币元。';
comment on column convertible_bond.daily_bar.symbol is '标准证券代码，格式如 SSE:600000、SZSE:000001、BSE:920000；保留代码前导零。';
comment on column convertible_bond.daily_bar.trade_date is '交易所本地交易日期；以统一交易日历为准。';
comment on column convertible_bond.daily_bar.market is '统一交易日历市场代码 CN_A_SHARE。';
comment on column convertible_bond.daily_bar.open is '开盘价，人民币元。';
comment on column convertible_bond.daily_bar.high is '最高价，人民币元。';
comment on column convertible_bond.daily_bar.low is '最低价，人民币元。';
comment on column convertible_bond.daily_bar.close is '收盘价，人民币元。';
comment on column convertible_bond.daily_bar.previous_close is '上一交易日收盘价，人民币元；来源事实不补造缺失值。';
comment on column convertible_bond.daily_bar.volume is '可转债成交量，张；不是股数。';
comment on column convertible_bond.daily_bar.amount is '成交额，人民币元。';
comment on column convertible_bond.daily_bar.pct_chg is '涨跌幅，百分点；1表示1%。';
comment on column convertible_bond.daily_bar.convert_value is '每张可转债的转股价值，人民币元。';
comment on column convertible_bond.daily_bar.convert_premium_pct is '转股溢价率，百分点；1表示1%。';
comment on column convertible_bond.daily_bar.convert_price is '转股价格，人民币元/股。';
comment on column convertible_bond.daily_bar.remain_size is '剩余规模，沿用来源的亿元口径。';
comment on column convertible_bond.daily_bar.trade_status is '当日交易状态，停牌和缺失不等同于零成交。';
comment on column convertible_bond.daily_bar.source_code is '实际数据来源代码；单个成功采集批次不合并多个来源。';
comment on column convertible_bond.daily_bar.ingestion_id is '采集批次标识，关联 ingestion.ingestion_run，用于追溯来源和 Raw 对象。';
comment on column convertible_bond.daily_bar.created_at is '记录首次写入时间（带时区）。';
comment on column convertible_bond.daily_bar.updated_at is '记录最后更新时间（带时区）。';

-- core.board_index
comment on table core.board_index is '板块指数主数据；板块身份与普通证券身份分离。';
comment on column core.board_index.board_id is '命名空间内板块指数的标准身份标识。';
comment on column core.board_index.board_code is '来源板块代码；不是普通证券代码。';
comment on column core.board_index.namespace is '分类或板块来源命名空间。';
comment on column core.board_index.name is '板块指数名称。';
comment on column core.board_index.board_type is '板块类型代码。';
comment on column core.board_index.market is '统一交易日历市场代码 CN_A_SHARE。';
comment on column core.board_index.status is '记录或运行状态；允许值及终态约束见本表 CHECK 约束。';
comment on column core.board_index.source_code is '实际数据来源代码；单个成功采集批次不合并多个来源。';
comment on column core.board_index.ingestion_id is '采集批次标识，关联 ingestion.ingestion_run，用于追溯来源和 Raw 对象。';
comment on column core.board_index.created_at is '记录首次写入时间（带时区）。';
comment on column core.board_index.updated_at is '记录最后更新时间（带时区）。';

-- core.board_index_constituent_snapshot
comment on table core.board_index_constituent_snapshot is '板块指数成分证券日快照。';
comment on column core.board_index_constituent_snapshot.board_id is '命名空间内板块指数的标准身份标识。';
comment on column core.board_index_constituent_snapshot.trade_date is '交易所本地交易日期；以统一交易日历为准。';
comment on column core.board_index_constituent_snapshot.symbol is '标准证券代码，格式如 SSE:600000、SZSE:000001、BSE:920000；保留代码前导零。';
comment on column core.board_index_constituent_snapshot.source_code is '实际数据来源代码；单个成功采集批次不合并多个来源。';
comment on column core.board_index_constituent_snapshot.ingestion_id is '采集批次标识，关联 ingestion.ingestion_run，用于追溯来源和 Raw 对象。';
comment on column core.board_index_constituent_snapshot.created_at is '记录首次写入时间（带时区）。';

-- core.board_index_daily_bar
comment on table core.board_index_daily_bar is '板块指数日行情事实；保存来源指数的日频价格及量额。';
comment on column core.board_index_daily_bar.board_id is '命名空间内板块指数的标准身份标识。';
comment on column core.board_index_daily_bar.trade_date is '交易所本地交易日期；以统一交易日历为准。';
comment on column core.board_index_daily_bar.market is '统一交易日历市场代码 CN_A_SHARE。';
comment on column core.board_index_daily_bar.open is '板块指数开盘点位。';
comment on column core.board_index_daily_bar.high is '板块指数最高点位。';
comment on column core.board_index_daily_bar.low is '板块指数最低点位。';
comment on column core.board_index_daily_bar.close is '板块指数收盘点位。';
comment on column core.board_index_daily_bar.volume is '成交量，股。';
comment on column core.board_index_daily_bar.amount is '成交额，人民币元。';
comment on column core.board_index_daily_bar.source_code is '实际数据来源代码；单个成功采集批次不合并多个来源。';
comment on column core.board_index_daily_bar.ingestion_id is '采集批次标识，关联 ingestion.ingestion_run，用于追溯来源和 Raw 对象。';
comment on column core.board_index_daily_bar.created_at is '记录首次写入时间（带时区）。';
comment on column core.board_index_daily_bar.updated_at is '记录最后更新时间（带时区）。';

-- core.board_moneyflow
comment on column core.board_moneyflow.trade_date is '交易所本地交易日期；以统一交易日历为准。';
comment on column core.board_moneyflow.board_code is '来源板块代码；不是普通证券代码。';
comment on column core.board_moneyflow.board_type is '板块类型代码。';
comment on column core.board_moneyflow.name is '该交易日来源返回的板块名称快照。';
comment on column core.board_moneyflow.pct_change is '板块当日涨跌幅，百分点。';
comment on column core.board_moneyflow.close is '板块指数收盘点位；来源未提供时为空。';
comment on column core.board_moneyflow.net_amount is '主力净流入金额，人民币元。';
comment on column core.board_moneyflow.net_amount_rate is '主力净流入占比，百分点。';
comment on column core.board_moneyflow.extra_large_amount is '超大单净流入金额，人民币元。';
comment on column core.board_moneyflow.extra_large_rate is '超大单净流入占比，百分点。';
comment on column core.board_moneyflow.large_amount is '大单净流入金额，人民币元。';
comment on column core.board_moneyflow.large_rate is '大单净流入占比，百分点。';
comment on column core.board_moneyflow.medium_amount is '中单净流入金额，人民币元。';
comment on column core.board_moneyflow.medium_rate is '中单净流入占比，百分点。';
comment on column core.board_moneyflow.small_amount is '小单净流入金额，人民币元。';
comment on column core.board_moneyflow.small_rate is '小单净流入占比，百分点。';
comment on column core.board_moneyflow.top_small_buy_stock is '来源提供的小单净买入代表股票名称。';
comment on column core.board_moneyflow.rank is '来源资金流向排名。';
comment on column core.board_moneyflow.source_code is '实际数据来源代码；单个成功采集批次不合并多个来源。';
comment on column core.board_moneyflow.ingestion_id is '采集批次标识，关联 ingestion.ingestion_run，用于追溯来源和 Raw 对象。';
comment on column core.board_moneyflow.first_observed_at is '数据中心首次观测到该来源事实的时间（带时区）。';

-- core.daily_bar
comment on table core.daily_bar is '股票未复权日行情事实；按证券和交易日唯一，成交量为股、成交额为人民币元，缺口不填充。';
comment on column core.daily_bar.symbol is '标准证券代码，格式如 SSE:600000、SZSE:000001、BSE:920000；保留代码前导零。';
comment on column core.daily_bar.trade_date is '交易所本地交易日期；以统一交易日历为准。';
comment on column core.daily_bar.market is '统一交易日历市场代码 CN_A_SHARE。';
comment on column core.daily_bar.open is '开盘价，人民币元。';
comment on column core.daily_bar.high is '最高价，人民币元。';
comment on column core.daily_bar.low is '最低价，人民币元。';
comment on column core.daily_bar.close is '收盘价，人民币元。';
comment on column core.daily_bar.previous_close is '上一交易日收盘价，人民币元；来源事实不补造缺失值。';
comment on column core.daily_bar.volume is '成交量，股。';
comment on column core.daily_bar.amount is '成交额，人民币元。';
comment on column core.daily_bar.trade_status is '当日交易状态，停牌和缺失不等同于零成交。';
comment on column core.daily_bar.is_st is '是否为特别处理证券。';
comment on column core.daily_bar.source_code is '实际数据来源代码；单个成功采集批次不合并多个来源。';
comment on column core.daily_bar.ingestion_id is '采集批次标识，关联 ingestion.ingestion_run，用于追溯来源和 Raw 对象。';
comment on column core.daily_bar.created_at is '记录首次写入时间（带时区）。';
comment on column core.daily_bar.updated_at is '记录最后更新时间（带时区）。';

-- core.deducted_profit
comment on table core.deducted_profit is '扣除非经常性损益净利润公告事实；保留公告可见时点和修订身份。';
comment on column core.deducted_profit.symbol is '标准证券代码，格式如 SSE:600000、SZSE:000001、BSE:920000；保留代码前导零。';
comment on column core.deducted_profit.report_period is '财务报告期截止日期。';
comment on column core.deducted_profit.announcement_date is '来源公告日期。';
comment on column core.deducted_profit.actual_announcement_date is '来源实际公告日期；未知时为空。';
comment on column core.deducted_profit.effective_announcement_date is '有效公告日期，按领域定义在实际公告日期与公告日期间确定。';
comment on column core.deducted_profit.cumulative_deducted_profit is '报告期累计扣非净利润，人民币元。';
comment on column core.deducted_profit.quarterly_deducted_profit is '单季度扣非净利润，人民币元；未知时为空。';
comment on column core.deducted_profit.update_flag is '来源修订标志。';
comment on column core.deducted_profit.revision_key is '公告修订身份键，用于区分同一统计期的不同披露版本。';
comment on column core.deducted_profit.source_code is '实际数据来源代码；单个成功采集批次不合并多个来源。';
comment on column core.deducted_profit.ingestion_id is '采集批次标识，关联 ingestion.ingestion_run，用于追溯来源和 Raw 对象。';
comment on column core.deducted_profit.first_observed_at is '数据中心首次观测到该来源事实的时间（带时区）。';

-- core.security
comment on table core.security is '证券主数据；以标准证券代码标识证券及其交易所、类型和上市生命周期。';
comment on column core.security.symbol is '标准证券代码，格式如 SSE:600000、SZSE:000001、BSE:920000；保留代码前导零。';
comment on column core.security.code is '交易所六位证券代码字符串；保留前导零。';
comment on column core.security.exchange is '交易所代码：SSE、SZSE 或 BSE，具体范围由表约束确定。';
comment on column core.security.current_name is '证券当前名称。';
comment on column core.security.security_type is '证券类型代码。';
comment on column core.security.status is '记录或运行状态；允许值及终态约束见本表 CHECK 约束。';
comment on column core.security.ipo_date is '上市日期。';
comment on column core.security.delisting_date is '退市日期；未知或尚未退市时为空。';
comment on column core.security.source_code is '实际数据来源代码；单个成功采集批次不合并多个来源。';
comment on column core.security.ingestion_id is '采集批次标识，关联 ingestion.ingestion_run，用于追溯来源和 Raw 对象。';
comment on column core.security.created_at is '记录首次写入时间（带时区）。';
comment on column core.security.updated_at is '记录最后更新时间（带时区）。';

-- core.security_name_history
comment on table core.security_name_history is '证券历史名称及生效区间；用于按历史时点解析证券名称。';
comment on column core.security_name_history.symbol is '标准证券代码，格式如 SSE:600000、SZSE:000001、BSE:920000；保留代码前导零。';
comment on column core.security_name_history.name is '该生效区间内使用的证券历史名称。';
comment on column core.security_name_history.effective_from is '名称生效起始日期。';
comment on column core.security_name_history.effective_to is '名称生效结束日期；为空表示尚未结束。';
comment on column core.security_name_history.source_code is '实际数据来源代码；单个成功采集批次不合并多个来源。';
comment on column core.security_name_history.ingestion_id is '采集批次标识，关联 ingestion.ingestion_run，用于追溯来源和 Raw 对象。';
comment on column core.security_name_history.created_at is '记录首次写入时间（带时区）。';

-- core.shareholder_count
comment on table core.shareholder_count is '股东户数公告事实；按统计日期和修订身份保存公告可见时点。';
comment on column core.shareholder_count.symbol is '标准证券代码，格式如 SSE:600000、SZSE:000001、BSE:920000；保留代码前导零。';
comment on column core.shareholder_count.statistics_date is '股东户数统计截止日期。';
comment on column core.shareholder_count.announcement_date is '来源公告日期。';
comment on column core.shareholder_count.shareholder_count is '股东户数，户。';
comment on column core.shareholder_count.revision_key is '公告修订身份键，用于区分同一统计期的不同披露版本。';
comment on column core.shareholder_count.source_code is '实际数据来源代码；单个成功采集批次不合并多个来源。';
comment on column core.shareholder_count.ingestion_id is '采集批次标识，关联 ingestion.ingestion_run，用于追溯来源和 Raw 对象。';
comment on column core.shareholder_count.first_observed_at is '数据中心首次观测到该来源事实的时间（带时区）。';

-- core.stock_daily_indicator
comment on table core.stock_daily_indicator is '股票每日指标来源事实；保存估值、换手率、量比、股本及市值。';
comment on column core.stock_daily_indicator.symbol is '标准证券代码，格式如 SSE:600000、SZSE:000001、BSE:920000；保留代码前导零。';
comment on column core.stock_daily_indicator.trade_date is '交易所本地交易日期；以统一交易日历为准。';
comment on column core.stock_daily_indicator.market is '统一交易日历市场代码 CN_A_SHARE。';
comment on column core.stock_daily_indicator.close is '收盘价，人民币元。';
comment on column core.stock_daily_indicator.turnover_rate_pct is '换手率，百分点；1表示1%。';
comment on column core.stock_daily_indicator.free_float_turnover_rate_pct is '自由流通股本换手率，百分点；1表示1%。';
comment on column core.stock_daily_indicator.volume_ratio is '量比，无量纲倍数。';
comment on column core.stock_daily_indicator.pe is '市盈率，倍。';
comment on column core.stock_daily_indicator.pe_ttm is '滚动市盈率，倍。';
comment on column core.stock_daily_indicator.pb is '市净率，倍。';
comment on column core.stock_daily_indicator.ps is '市销率，倍。';
comment on column core.stock_daily_indicator.ps_ttm is '滚动市销率，倍。';
comment on column core.stock_daily_indicator.dividend_yield_pct is '股息率，百分点；1表示1%。';
comment on column core.stock_daily_indicator.dividend_yield_ttm_pct is '滚动股息率，百分点；1表示1%。';
comment on column core.stock_daily_indicator.total_shares is '总股本，股。';
comment on column core.stock_daily_indicator.circulating_shares is '流通股本，股。';
comment on column core.stock_daily_indicator.free_float_shares is '自由流通股本，股；与流通股本口径不同。';
comment on column core.stock_daily_indicator.total_market_value is '总市值，人民币元。';
comment on column core.stock_daily_indicator.circulating_market_value is '流通市值，人民币元。';
comment on column core.stock_daily_indicator.price_limit_status is '每日指标来源报告的涨跌停状态。';
comment on column core.stock_daily_indicator.source_code is '实际数据来源代码；单个成功采集批次不合并多个来源。';
comment on column core.stock_daily_indicator.ingestion_id is '采集批次标识，关联 ingestion.ingestion_run，用于追溯来源和 Raw 对象。';
comment on column core.stock_daily_indicator.created_at is '记录首次写入时间（带时区）。';
comment on column core.stock_daily_indicator.updated_at is '记录最后更新时间（带时区）。';

-- core.stock_technical_factor_snapshot
comment on table core.stock_technical_factor_snapshot is '股票专业版技术面因子日快照；来源 Tushare stk_factor_pro，按因子目录版本分组保存，独立于每日指标。';
comment on column core.stock_technical_factor_snapshot.symbol is '标准证券代码，格式如 SSE:600000、SZSE:000001、BSE:920000；保留代码前导零。';
comment on column core.stock_technical_factor_snapshot.trade_date is '交易所本地交易日期；以统一交易日历为准。';
comment on column core.stock_technical_factor_snapshot.market is '统一交易日历市场代码 CN_A_SHARE。';
comment on column core.stock_technical_factor_snapshot.source_code is '实际数据来源代码；单个成功采集批次不合并多个来源。';
comment on column core.stock_technical_factor_snapshot.factor_catalog_version is '技术因子目录版本，定义字段集合、类型及分组。';
comment on column core.stock_technical_factor_snapshot.unadjusted is '未复权因子对象；按因子目录定义，Decimal以字符串保存，缺失为JSON null。';
comment on column core.stock_technical_factor_snapshot.forward_adjusted is '前复权因子对象；按因子目录定义，Decimal以字符串保存，缺失为JSON null。';
comment on column core.stock_technical_factor_snapshot.backward_adjusted is '后复权因子对象；按因子目录定义，Decimal以字符串保存，缺失为JSON null。';
comment on column core.stock_technical_factor_snapshot.common is '与复权无关的公共因子对象；字段类型及缺失语义由因子目录定义。';
comment on column core.stock_technical_factor_snapshot.ingestion_id is '采集批次标识，关联 ingestion.ingestion_run，用于追溯来源和 Raw 对象。';
comment on column core.stock_technical_factor_snapshot.created_at is '记录首次写入时间（带时区）。';
comment on column core.stock_technical_factor_snapshot.updated_at is '记录最后更新时间（带时区）。';

-- core.top10_float_holder
comment on column core.top10_float_holder.symbol is '标准证券代码，格式如 SSE:600000、SZSE:000001、BSE:920000；保留代码前导零。';
comment on column core.top10_float_holder.end_date is '股东持股报告期截止日期；不晚于公告日期。';
comment on column core.top10_float_holder.announcement_date is '来源公告日期。';
comment on column core.top10_float_holder.holder_rank is '前十大流通股东在报告期内的排名。';
comment on column core.top10_float_holder.holder_name is '股东名称原文。';
comment on column core.top10_float_holder.hold_amount is '持有流通股份数量，股。';
comment on column core.top10_float_holder.hold_ratio is '占总股本比例，百分点。';
comment on column core.top10_float_holder.hold_float_ratio is '占流通股本比例，百分点。';
comment on column core.top10_float_holder.hold_change is '持股数量变动，股；可为负数。';
comment on column core.top10_float_holder.holder_type is '来源股东类型。';
comment on column core.top10_float_holder.source_code is '实际数据来源代码；单个成功采集批次不合并多个来源。';
comment on column core.top10_float_holder.ingestion_id is '采集批次标识，关联 ingestion.ingestion_run，用于追溯来源和 Raw 对象。';
comment on column core.top10_float_holder.first_observed_at is '数据中心首次观测到该来源事实的时间（带时区）。';

-- core.trading_calendar
comment on table core.trading_calendar is '统一市场交易日历；保存是否交易及相邻交易日。';
comment on column core.trading_calendar.market is '统一交易日历市场代码 CN_A_SHARE。';
comment on column core.trading_calendar.trade_date is '交易所本地交易日期；以统一交易日历为准。';
comment on column core.trading_calendar.is_trading_day is '是否为交易日。';
comment on column core.trading_calendar.previous_trading_day is '上一交易日期。';
comment on column core.trading_calendar.next_trading_day is '下一交易日期。';
comment on column core.trading_calendar.source_code is '实际数据来源代码；单个成功采集批次不合并多个来源。';
comment on column core.trading_calendar.ingestion_id is '采集批次标识，关联 ingestion.ingestion_run，用于追溯来源和 Raw 对象。';
comment on column core.trading_calendar.created_at is '记录首次写入时间（带时区）。';
comment on column core.trading_calendar.updated_at is '记录最后更新时间（带时区）。';

-- derived.adjusted_daily_bar
comment on table derived.adjusted_daily_bar is '版本化复权日行情；与计算批次和复权类型绑定，不覆盖未复权来源事实。';
comment on column derived.adjusted_daily_bar.calculation_id is '计算批次标识；用于追溯算法版本及冻结输入，同一逻辑结果不混用计算批次。';
comment on column derived.adjusted_daily_bar.symbol is '标准证券代码，格式如 SSE:600000、SZSE:000001、BSE:920000；保留代码前导零。';
comment on column derived.adjusted_daily_bar.trade_date is '交易所本地交易日期；以统一交易日历为准。';
comment on column derived.adjusted_daily_bar.adjustment_type is '复权类型代码。';
comment on column derived.adjusted_daily_bar.adjustment_factor is '对应复权类型和计算批次的价格调整因子。';
comment on column derived.adjusted_daily_bar.open is '开盘价，人民币元。';
comment on column derived.adjusted_daily_bar.high is '最高价，人民币元。';
comment on column derived.adjusted_daily_bar.low is '最低价，人民币元。';
comment on column derived.adjusted_daily_bar.close is '收盘价，人民币元。';
comment on column derived.adjusted_daily_bar.previous_close is '上一交易日收盘价，人民币元；来源事实不补造缺失值。';

-- derived.auction_quote_metric
comment on table derived.auction_quote_metric is '竞价五档报价客观派生指标；保存价差、盘口深度和封单额及算法版本。';
comment on column derived.auction_quote_metric.session_id is '竞价采集会话标识。';
comment on column derived.auction_quote_metric.symbol is '标准证券代码，格式如 SSE:600000、SZSE:000001、BSE:920000；保留代码前导零。';
comment on column derived.auction_quote_metric.sample_seq is '会话内计划采样序号；不是采集批次标识。';
comment on column derived.auction_quote_metric.spread is '买卖最优报价价差，人民币元。';
comment on column derived.auction_quote_metric.mid_price is '最优买卖报价中间价，人民币元。';
comment on column derived.auction_quote_metric.bid_depth_5 is '买方前五档数量合计，股。';
comment on column derived.auction_quote_metric.ask_depth_5 is '卖方前五档数量合计，股。';
comment on column derived.auction_quote_metric.imbalance_5 is '五档买卖深度不平衡比例，按算法版本定义，无量纲。';
comment on column derived.auction_quote_metric.seal_amount is '封单金额，人民币元；具体方向及计算时点由所属领域算法定义。';
comment on column derived.auction_quote_metric.calculated_at is '客观指标计算时间（带时区）。';
comment on column derived.auction_quote_metric.algorithm_version is '计算算法版本；用于重现结果。';
comment on column derived.auction_quote_metric.price_limit_rule_version is '计算封单额所采用的涨跌停规则版本。';

-- derived.calculation_run
comment on table derived.calculation_run is '客观派生计算批次；保存算法版本、输入水位、输入摘要及执行状态。';
comment on column derived.calculation_run.calculation_id is '计算批次标识；用于追溯算法版本及冻结输入，同一逻辑结果不混用计算批次。';
comment on column derived.calculation_run.calculation_code is '客观计算任务代码。';
comment on column derived.calculation_run.algorithm_version is '计算算法版本；用于重现结果。';
comment on column derived.calculation_run.mode is '计算运行模式。';
comment on column derived.calculation_run.start_date is '计算范围起始日期。';
comment on column derived.calculation_run.end_date is '计算范围结束日期。';
comment on column derived.calculation_run.status is '记录或运行状态；允许值及终态约束见本表 CHECK 约束。';
comment on column derived.calculation_run.input_watermark is '计算输入水位，记录本次计算实际采用的输入边界。';
comment on column derived.calculation_run.input_hash is '规范化计算输入或采集内容的校验摘要。';
comment on column derived.calculation_run.requested_at is '请求创建时间（带时区）。';
comment on column derived.calculation_run.calculated_at is '客观指标计算时间（带时区）。';
comment on column derived.calculation_run.finished_at is '实际结束时间（带时区）；运行中可为空。';
comment on column derived.calculation_run.output_rows is '计算输出记录数。';
comment on column derived.calculation_run.error_summary is '错误摘要；不包含凭据或连接串。';

-- derived.close_price_new_high_120d_member
comment on table derived.close_price_new_high_120d_member is '120交易日收盘价创新高成员；保存收盘价、前119交易日最高收盘价及突破幅度。';
comment on column derived.close_price_new_high_120d_member.snapshot_id is '不可变快照标识。';
comment on column derived.close_price_new_high_120d_member.symbol is '标准证券代码，格式如 SSE:600000、SZSE:000001、BSE:920000；保留代码前导零。';
comment on column derived.close_price_new_high_120d_member.display_name is '快照使用的历史展示名称。';
comment on column derived.close_price_new_high_120d_member.close is '收盘价，人民币元。';
comment on column derived.close_price_new_high_120d_member.previous_119d_high is '此前119交易日最高收盘价，人民币元。';
comment on column derived.close_price_new_high_120d_member.breakout_pct is '收盘价相对前119交易日最高收盘价的突破幅度，百分点。';

-- derived.close_price_new_high_120d_snapshot
comment on table derived.close_price_new_high_120d_snapshot is '120交易日收盘价创新高不可变快照头；记录候选、历史覆盖、遗漏及计算版本。';
comment on column derived.close_price_new_high_120d_snapshot.snapshot_id is '不可变快照标识。';
comment on column derived.close_price_new_high_120d_snapshot.calculation_id is '计算批次标识；用于追溯算法版本及冻结输入，同一逻辑结果不混用计算批次。';
comment on column derived.close_price_new_high_120d_snapshot.trade_date is '交易所本地交易日期；以统一交易日历为准。';
comment on column derived.close_price_new_high_120d_snapshot.version is '同一自然范围内的快照版本号。';
comment on column derived.close_price_new_high_120d_snapshot.status is '记录或运行状态；允许值及终态约束见本表 CHECK 约束。';
comment on column derived.close_price_new_high_120d_snapshot.candidate_count is '计算候选证券数量。';
comment on column derived.close_price_new_high_120d_snapshot.eligible_history_count is '具有满足计算条件历史行情的证券数量。';
comment on column derived.close_price_new_high_120d_snapshot.omitted_count is '从结果中遗漏的证券数量。';
comment on column derived.close_price_new_high_120d_snapshot.member_count is '快照或聚合范围内的成员数量。';
comment on column derived.close_price_new_high_120d_snapshot.incomplete_history_count is '历史行情不完整的证券数量。';
comment on column derived.close_price_new_high_120d_snapshot.non_trading_bar_count is '历史窗口含非交易行情状态的证券数量。';
comment on column derived.close_price_new_high_120d_snapshot.nonpositive_price_count is '历史窗口存在非正价格的证券数量。';
comment on column derived.close_price_new_high_120d_snapshot.missing_name_count is '缺少历史名称的证券数量。';
comment on column derived.close_price_new_high_120d_snapshot.input_hash is '规范化计算输入或采集内容的校验摘要。';
comment on column derived.close_price_new_high_120d_snapshot.content_hash is '规范化记录或快照内容的校验摘要。';
comment on column derived.close_price_new_high_120d_snapshot.algorithm_version is '计算算法版本；用于重现结果。';
comment on column derived.close_price_new_high_120d_snapshot.generated_at is '快照生成时间（带时区）。';

-- derived.daily_metric
comment on table derived.daily_metric is '版本化证券日频客观指标；保存收益率与移动均价。';
comment on column derived.daily_metric.calculation_id is '计算批次标识；用于追溯算法版本及冻结输入，同一逻辑结果不混用计算批次。';
comment on column derived.daily_metric.symbol is '标准证券代码，格式如 SSE:600000、SZSE:000001、BSE:920000；保留代码前导零。';
comment on column derived.daily_metric.trade_date is '交易所本地交易日期；以统一交易日历为准。';
comment on column derived.daily_metric.total_return_1d is '一日总收益率，小数比例，非百分点。';
comment on column derived.daily_metric.moving_average_5 is '5交易日移动均价，人民币元。';
comment on column derived.daily_metric.moving_average_10 is '10交易日移动均价，人民币元。';
comment on column derived.daily_metric.moving_average_20 is '20交易日移动均价，人民币元。';

-- derived.daily_price_limit
comment on table derived.daily_price_limit is '版本化主板每日涨跌停价格派生事实；保存参考价、价格步长与规则版本。';
comment on column derived.daily_price_limit.calculation_id is '计算批次标识；用于追溯算法版本及冻结输入，同一逻辑结果不混用计算批次。';
comment on column derived.daily_price_limit.symbol is '标准证券代码，格式如 SSE:600000、SZSE:000001、BSE:920000；保留代码前导零。';
comment on column derived.daily_price_limit.trade_date is '交易所本地交易日期；以统一交易日历为准。';
comment on column derived.daily_price_limit.previous_close is '上一交易日收盘价，人民币元；来源事实不补造缺失值。';
comment on column derived.daily_price_limit.upper_limit is '当日涨停价格，人民币元。';
comment on column derived.daily_price_limit.lower_limit is '当日跌停价格，人民币元。';
comment on column derived.daily_price_limit.limit_ratio is '涨跌停幅度，小数比例，非百分点。';
comment on column derived.daily_price_limit.price_tick is '最小价格变动单位，人民币元。';
comment on column derived.daily_price_limit.is_st is '是否为特别处理证券。';
comment on column derived.daily_price_limit.rule_version is '客观计算规则版本。';
comment on column derived.daily_price_limit.algorithm_version is '计算算法版本；用于重现结果。';

-- derived.market_capitalization
comment on table derived.market_capitalization is '版本化证券市值派生结果；由行情及股本输入计算。';
comment on column derived.market_capitalization.calculation_id is '计算批次标识；用于追溯算法版本及冻结输入，同一逻辑结果不混用计算批次。';
comment on column derived.market_capitalization.symbol is '标准证券代码，格式如 SSE:600000、SZSE:000001、BSE:920000；保留代码前导零。';
comment on column derived.market_capitalization.trade_date is '交易所本地交易日期；以统一交易日历为准。';
comment on column derived.market_capitalization.total_market_cap is '总市值，人民币元。';
comment on column derived.market_capitalization.circulating_market_cap is '流通市值，人民币元。';

-- derived.price_limit_event
comment on table derived.price_limit_event is '版本化每日收盘涨跌停命中事实。';
comment on column derived.price_limit_event.calculation_id is '计算批次标识；用于追溯算法版本及冻结输入，同一逻辑结果不混用计算批次。';
comment on column derived.price_limit_event.symbol is '标准证券代码，格式如 SSE:600000、SZSE:000001、BSE:920000；保留代码前导零。';
comment on column derived.price_limit_event.trade_date is '交易所本地交易日期；以统一交易日历为准。';
comment on column derived.price_limit_event.direction is '方向代码；具体允许值见所属领域与表约束。';
comment on column derived.price_limit_event.close is '收盘价，人民币元。';
comment on column derived.price_limit_event.limit_price is '对应方向的涨停或跌停价格，人民币元。';
comment on column derived.price_limit_event.rule_version is '客观计算规则版本。';
comment on column derived.price_limit_event.algorithm_version is '计算算法版本；用于重现结果。';

-- ingestion.ingestion_run
comment on table ingestion.ingestion_run is '数据采集运行记录；记录单一实际来源、请求参数、状态及接收/拒绝计数。';
comment on column ingestion.ingestion_run.ingestion_id is '采集批次标识，关联 ingestion.ingestion_run，用于追溯来源和 Raw 对象。';
comment on column ingestion.ingestion_run.provider_code is '实际采集适配器代码。';
comment on column ingestion.ingestion_run.dataset_code is '受控采集数据集代码。';
comment on column ingestion.ingestion_run.status is '记录或运行状态；允许值及终态约束见本表 CHECK 约束。';
comment on column ingestion.ingestion_run.requested_at is '请求创建时间（带时区）。';
comment on column ingestion.ingestion_run.started_at is '实际开始时间（带时区）。';
comment on column ingestion.ingestion_run.finished_at is '实际结束时间（带时区）；运行中可为空。';
comment on column ingestion.ingestion_run.request_params is '本次采集实际使用的请求参数，不含凭据。';
comment on column ingestion.ingestion_run.fetched_rows is '来源实际返回的记录数。';
comment on column ingestion.ingestion_run.accepted_rows is '通过校验并接收的记录数。';
comment on column ingestion.ingestion_run.rejected_rows is '校验拒绝的记录数。';
comment on column ingestion.ingestion_run.error_summary is '错误摘要；不包含凭据或连接串。';
comment on column ingestion.ingestion_run.created_at is '记录首次写入时间（带时区）。';
comment on column ingestion.ingestion_run.updated_at is '记录最后更新时间（带时区）。';

-- ingestion.raw_manifest
comment on table ingestion.raw_manifest is '不可变原始数据对象清单；保存对象定位、格式、校验摘要和采集血缘。';
comment on column ingestion.raw_manifest.raw_id is '不可变原始数据对象标识，关联 ingestion.raw_manifest。';
comment on column ingestion.raw_manifest.ingestion_id is '采集批次标识，关联 ingestion.ingestion_run，用于追溯来源和 Raw 对象。';
comment on column ingestion.raw_manifest.storage_backend is '不可变原始对象存储后端代码。';
comment on column ingestion.raw_manifest.object_path is '受控存储根下的相对对象定位；不是客户端可读取的数据库路径。';
comment on column ingestion.raw_manifest.file_format is '不可变原始对象文件格式。';
comment on column ingestion.raw_manifest.content_sha256 is '未压缩原始内容的SHA-256摘要。';
comment on column ingestion.raw_manifest.byte_size is '原始对象字节数。';
comment on column ingestion.raw_manifest.row_count is '对象中的逻辑记录行数。';
comment on column ingestion.raw_manifest.schema_version is '原始数据或归档记录格式版本。';
comment on column ingestion.raw_manifest.created_at is '记录首次写入时间（带时区）。';

-- metrics.classification_daily_metric
comment on table metrics.classification_daily_metric is '版本化分类日频聚合指标；保存成员覆盖、涨跌计数、量额和市值。';
comment on column metrics.classification_daily_metric.calculation_id is '计算批次标识；用于追溯算法版本及冻结输入，同一逻辑结果不混用计算批次。';
comment on column metrics.classification_daily_metric.namespace is '分类或板块来源命名空间。';
comment on column metrics.classification_daily_metric.classification_type is '分类类型代码。';
comment on column metrics.classification_daily_metric.classification_code is '来源命名空间内的分类代码。';
comment on column metrics.classification_daily_metric.membership_snapshot_date is '本次聚合实际使用的分类成员快照日期。';
comment on column metrics.classification_daily_metric.trade_date is '交易所本地交易日期；以统一交易日历为准。';
comment on column metrics.classification_daily_metric.member_count is '快照或聚合范围内的成员数量。';
comment on column metrics.classification_daily_metric.priced_member_count is '具有可用行情的成员数量。';
comment on column metrics.classification_daily_metric.advancing_count is '上涨成员数量。';
comment on column metrics.classification_daily_metric.declining_count is '下跌成员数量。';
comment on column metrics.classification_daily_metric.unchanged_count is '平盘成员数量。';
comment on column metrics.classification_daily_metric.total_volume is '成员成交量合计，股。';
comment on column metrics.classification_daily_metric.total_amount is '成员成交额合计，人民币元。';
comment on column metrics.classification_daily_metric.equal_weight_return is '成员等权收益率，小数比例，非百分点。';
comment on column metrics.classification_daily_metric.total_market_cap is '总市值，人民币元。';
comment on column metrics.classification_daily_metric.market_cap_member_count is '具有可用市值数据的成员数量。';

-- operations.data_cleanup_report
comment on table operations.data_cleanup_report is '受控数据清理执行报告；记录保留边界、执行步骤和磁盘观测结果。';
comment on column operations.data_cleanup_report.workflow_run_id is '工作流执行标识，关联 operations.workflow_run。';
comment on column operations.data_cleanup_report.reference_date is '本次保留与清理计算采用的参考日期。';
comment on column operations.data_cleanup_report.started_at is '实际开始时间（带时区）。';
comment on column operations.data_cleanup_report.finished_at is '实际结束时间（带时区）；运行中可为空。';
comment on column operations.data_cleanup_report.cutoffs is '按数据类别记录的保留截止日期或时间。';
comment on column operations.data_cleanup_report.steps is '受控清理步骤及各步骤执行结果。';
comment on column operations.data_cleanup_report.disks is '清理前后磁盘容量观测结果。';
comment on column operations.data_cleanup_report.owned_temps is '本程序拥有的临时对象检查与清理结果。';
comment on column operations.data_cleanup_report.status is '记录或运行状态；允许值及终态约束见本表 CHECK 约束。';

-- operations.job_execution
comment on table operations.job_execution is '工作流内任务执行事实；记录任务顺序、重试和结果，不存储 APScheduler 序列化状态。';
comment on column operations.job_execution.job_execution_id is '工作流内任务执行标识。';
comment on column operations.job_execution.workflow_run_id is '工作流执行标识，关联 operations.workflow_run。';
comment on column operations.job_execution.job_code is '受控任务目录中的任务代码。';
comment on column operations.job_execution.sequence_no is '工作流内任务执行顺序号。';
comment on column operations.job_execution.attempt is '当前执行尝试次数。';
comment on column operations.job_execution.status is '记录或运行状态；允许值及终态约束见本表 CHECK 约束。';
comment on column operations.job_execution.started_at is '实际开始时间（带时区）。';
comment on column operations.job_execution.finished_at is '实际结束时间（带时区）；运行中可为空。';
comment on column operations.job_execution.fetched_rows is '来源实际返回的记录数。';
comment on column operations.job_execution.accepted_rows is '通过校验并接收的记录数。';
comment on column operations.job_execution.rejected_rows is '校验拒绝的记录数。';
comment on column operations.job_execution.error_summary is '错误摘要；不包含凭据或连接串。';

-- operations.workflow_run
comment on table operations.workflow_run is 'Worker 工作流执行事实；记录计划触发、实际执行、状态和结果计数。';
comment on column operations.workflow_run.workflow_run_id is '工作流执行标识，关联 operations.workflow_run。';
comment on column operations.workflow_run.workflow_code is '受控工作流目录中的工作流代码。';
comment on column operations.workflow_run.scheduled_for is '计划触发时间（带时区）；不等于实际开始时间。';
comment on column operations.workflow_run.trigger_source is '触发来源代码，如调度、手动或恢复。';
comment on column operations.workflow_run.attempt is '当前执行尝试次数。';
comment on column operations.workflow_run.status is '记录或运行状态；允许值及终态约束见本表 CHECK 约束。';
comment on column operations.workflow_run.started_at is '实际开始时间（带时区）。';
comment on column operations.workflow_run.finished_at is '实际结束时间（带时区）；运行中可为空。';
comment on column operations.workflow_run.accepted_rows is '通过校验并接收的记录数。';
comment on column operations.workflow_run.rejected_rows is '校验拒绝的记录数。';
comment on column operations.workflow_run.error_summary is '错误摘要；不包含凭据或连接串。';

-- realtime.auction_collection_round
comment on table realtime.auction_collection_round is '股票池竞价采集轮次；记录计划时点、采集完成时点及报价计数。';
comment on column realtime.auction_collection_round.session_id is '竞价采集会话标识。';
comment on column realtime.auction_collection_round.sample_seq is '会话内计划采样序号；不是采集批次标识。';
comment on column realtime.auction_collection_round.ingestion_id is '采集批次标识，关联 ingestion.ingestion_run，用于追溯来源和 Raw 对象。';
comment on column realtime.auction_collection_round.scheduled_at is '计划采样时间（带时区），按Asia/Shanghai交易时槽定义。';
comment on column realtime.auction_collection_round.collected_at is '来源采集完成时间（带时区），独立于异步持久化提交时间。';
comment on column realtime.auction_collection_round.phase is '竞价采集阶段代码。';
comment on column realtime.auction_collection_round.status is '记录或运行状态；允许值及终态约束见本表 CHECK 约束。';
comment on column realtime.auction_collection_round.expected_quotes is '本会话或轮次期望的报价数量。';
comment on column realtime.auction_collection_round.successful_quotes is '成功采集的报价数量。';
comment on column realtime.auction_collection_round.failed_quotes is '失败或缺失的报价数量。';
comment on column realtime.auction_collection_round.latency_ms is '采集耗时，毫秒。';
comment on column realtime.auction_collection_round.error_summary is '错误摘要；不包含凭据或连接串。';

-- realtime.auction_collection_session
comment on table realtime.auction_collection_session is '股票池竞价采集会话；冻结输入池版本并记录采样窗口与轮次汇总。';
comment on column realtime.auction_collection_session.session_id is '竞价采集会话标识。';
comment on column realtime.auction_collection_session.pool_snapshot_id is '采集会话所使用的股票池快照标识。';
comment on column realtime.auction_collection_session.pool_snapshot_version is '采集会话冻结的股票池快照版本。';
comment on column realtime.auction_collection_session.basis_trade_date is '生成股票池所依据的基准交易日期。';
comment on column realtime.auction_collection_session.effective_trade_date is '股票池精确生效交易日期；不得回退到较早日期。';
comment on column realtime.auction_collection_session.window_start is '采集窗口开始时间（带时区）。';
comment on column realtime.auction_collection_session.window_end is '采集窗口结束时间（带时区）。';
comment on column realtime.auction_collection_session.cadence_seconds is '基础采样间隔，秒；实际计划时槽以所属领域规则为准。';
comment on column realtime.auction_collection_session.expected_rounds is '期望采样轮次数。';
comment on column realtime.auction_collection_session.expected_quotes is '本会话或轮次期望的报价数量。';
comment on column realtime.auction_collection_session.provider_code is '实际采集适配器代码。';
comment on column realtime.auction_collection_session.status is '记录或运行状态；允许值及终态约束见本表 CHECK 约束。';
comment on column realtime.auction_collection_session.started_at is '实际开始时间（带时区）。';
comment on column realtime.auction_collection_session.finished_at is '实际结束时间（带时区）；运行中可为空。';
comment on column realtime.auction_collection_session.successful_rounds is '成功采样轮次数。';
comment on column realtime.auction_collection_session.partial_rounds is '部分成功的采样轮次数。';
comment on column realtime.auction_collection_session.failed_rounds is '失败采样轮次数。';
comment on column realtime.auction_collection_session.successful_quotes is '成功采集的报价数量。';
comment on column realtime.auction_collection_session.failed_quotes is '失败或缺失的报价数量。';
comment on column realtime.auction_collection_session.error_summary is '错误摘要；不包含凭据或连接串。';
comment on column realtime.auction_collection_session.created_at is '记录首次写入时间（带时区）。';

-- realtime.call_auction_indicative_detail
comment on table realtime.call_auction_indicative_detail is '历史竞价指示明细来源事实；保留来源序号、指示价、展示量与来源展示分类。';
comment on column realtime.call_auction_indicative_detail.ingestion_id is '采集批次标识，关联 ingestion.ingestion_run，用于追溯来源和 Raw 对象。';
comment on column realtime.call_auction_indicative_detail.symbol is '标准证券代码，格式如 SSE:600000、SZSE:000001、BSE:920000；保留代码前导零。';
comment on column realtime.call_auction_indicative_detail.trade_date is '交易所本地交易日期；以统一交易日历为准。';
comment on column realtime.call_auction_indicative_detail.source_sequence is '来源返回的记录顺序号；保留来源排序。';
comment on column realtime.call_auction_indicative_detail.observed_at is '本次来源事实观测时间（带时区）；不等于计划时间或入库时间。';
comment on column realtime.call_auction_indicative_detail.indicative_price is '集合竞价指示价格，人民币元；不是逐笔成交价。';
comment on column realtime.call_auction_indicative_detail.displayed_volume_shares is '来源展示数量，股；不擅自解释为实际成交量。';
comment on column realtime.call_auction_indicative_detail.source_display_classification is '来源展示分类；不推断委托或撮合方向。';

-- realtime.call_auction_indicative_snapshot
comment on table realtime.call_auction_indicative_snapshot is '竞价指示明细快照头；保存单证券单日来源版本、记录数及 Raw 血缘。';
comment on column realtime.call_auction_indicative_snapshot.ingestion_id is '采集批次标识，关联 ingestion.ingestion_run，用于追溯来源和 Raw 对象。';
comment on column realtime.call_auction_indicative_snapshot.raw_id is '不可变原始数据对象标识，关联 ingestion.raw_manifest。';
comment on column realtime.call_auction_indicative_snapshot.symbol is '标准证券代码，格式如 SSE:600000、SZSE:000001、BSE:920000；保留代码前导零。';
comment on column realtime.call_auction_indicative_snapshot.trade_date is '交易所本地交易日期；以统一交易日历为准。';
comment on column realtime.call_auction_indicative_snapshot.version is '同一自然范围内的快照版本号。';
comment on column realtime.call_auction_indicative_snapshot.status is '记录或运行状态；允许值及终态约束见本表 CHECK 约束。';
comment on column realtime.call_auction_indicative_snapshot.source_code is '实际数据来源代码；单个成功采集批次不合并多个来源。';
comment on column realtime.call_auction_indicative_snapshot.record_count is '快照中的来源明细记录数。';
comment on column realtime.call_auction_indicative_snapshot.created_at is '记录首次写入时间（带时区）。';
comment on column realtime.call_auction_indicative_snapshot.fetched_at is '来源数据获取时间（带时区）。';
comment on column realtime.call_auction_indicative_snapshot.input_hash is '规范化计算输入或采集内容的校验摘要。';

-- realtime.call_auction_market_series_round
comment on table realtime.call_auction_market_series_round is '全市场竞价序列采样轮次；记录请求尝试及被选择的采集批次。';
comment on column realtime.call_auction_market_series_round.session_id is '竞价采集会话标识。';
comment on column realtime.call_auction_market_series_round.sample_seq is '会话内计划采样序号；不是采集批次标识。';
comment on column realtime.call_auction_market_series_round.scheduled_at is '计划采样时间（带时区），按Asia/Shanghai交易时槽定义。';
comment on column realtime.call_auction_market_series_round.status is '记录或运行状态；允许值及终态约束见本表 CHECK 约束。';
comment on column realtime.call_auction_market_series_round.attempt_count is '当前轮次实际请求尝试次数。';
comment on column realtime.call_auction_market_series_round.expected_quotes is '本会话或轮次期望的报价数量。';
comment on column realtime.call_auction_market_series_round.successful_quotes is '成功采集的报价数量。';
comment on column realtime.call_auction_market_series_round.failed_quotes is '失败或缺失的报价数量。';
comment on column realtime.call_auction_market_series_round.selected_ingestion_id is '该轮规范读取选择的采集批次标识；不合并不同来源尝试。';
comment on column realtime.call_auction_market_series_round.error_summary is '错误摘要；不包含凭据或连接串。';

-- realtime.call_auction_market_series_session
comment on table realtime.call_auction_market_series_session is '全市场开盘竞价序列采集会话；冻结证券全集、计划窗口及32个采样轮次。';
comment on column realtime.call_auction_market_series_session.session_id is '竞价采集会话标识。';
comment on column realtime.call_auction_market_series_session.workflow_run_id is '工作流执行标识，关联 operations.workflow_run。';
comment on column realtime.call_auction_market_series_session.trade_date is '交易所本地交易日期；以统一交易日历为准。';
comment on column realtime.call_auction_market_series_session.window_start is '采集窗口开始时间（带时区）。';
comment on column realtime.call_auction_market_series_session.window_end is '采集窗口结束时间（带时区）。';
comment on column realtime.call_auction_market_series_session.cadence_seconds is '基础采样间隔，秒；实际计划时槽以所属领域规则为准。';
comment on column realtime.call_auction_market_series_session.expected_rounds is '期望采样轮次数。';
comment on column realtime.call_auction_market_series_session.universe_symbols is '会话开始时冻结的有序标准证券代码全集。';
comment on column realtime.call_auction_market_series_session.universe_count is '冻结证券全集数量。';
comment on column realtime.call_auction_market_series_session.universe_hash is '冻结证券全集规范编码的SHA-256摘要。';
comment on column realtime.call_auction_market_series_session.status is '记录或运行状态；允许值及终态约束见本表 CHECK 约束。';
comment on column realtime.call_auction_market_series_session.started_at is '实际开始时间（带时区）。';
comment on column realtime.call_auction_market_series_session.finished_at is '实际结束时间（带时区）；运行中可为空。';
comment on column realtime.call_auction_market_series_session.successful_rounds is '成功采样轮次数。';
comment on column realtime.call_auction_market_series_session.partial_rounds is '部分成功的采样轮次数。';
comment on column realtime.call_auction_market_series_session.failed_rounds is '失败采样轮次数。';
comment on column realtime.call_auction_market_series_session.successful_quotes is '成功采集的报价数量。';
comment on column realtime.call_auction_market_series_session.failed_quotes is '失败或缺失的报价数量。';
comment on column realtime.call_auction_market_series_session.error_summary is '错误摘要；不包含凭据或连接串。';

-- realtime.call_auction_market_series_snapshot
comment on table realtime.call_auction_market_series_snapshot is '全市场竞价序列来源快照；按交易日分区，指示值与成交值由 value_semantics 区分。';
comment on column realtime.call_auction_market_series_snapshot.trade_date is '交易所本地交易日期；以统一交易日历为准。';
comment on column realtime.call_auction_market_series_snapshot.ingestion_id is '采集批次标识，关联 ingestion.ingestion_run，用于追溯来源和 Raw 对象。';
comment on column realtime.call_auction_market_series_snapshot.session_id is '竞价采集会话标识。';
comment on column realtime.call_auction_market_series_snapshot.sample_seq is '会话内计划采样序号；不是采集批次标识。';
comment on column realtime.call_auction_market_series_snapshot.scheduled_at is '计划采样时间（带时区），按Asia/Shanghai交易时槽定义。';
comment on column realtime.call_auction_market_series_snapshot.symbol is '标准证券代码，格式如 SSE:600000、SZSE:000001、BSE:920000；保留代码前导零。';
comment on column realtime.call_auction_market_series_snapshot.observed_at is '本次来源事实观测时间（带时区）；不等于计划时间或入库时间。';
comment on column realtime.call_auction_market_series_snapshot.last_price is 'auction_indicative语义下为买一指示价；opening_trade等成交语义下为来源最新成交价，人民币元。';
comment on column realtime.call_auction_market_series_snapshot.previous_close is '上一交易日收盘价，人民币元；来源事实不补造缺失值。';
comment on column realtime.call_auction_market_series_snapshot.high_price is '来源最高成交价，人民币元；竞价指示价不一定处于已成交价格区间内。';
comment on column realtime.call_auction_market_series_snapshot.low_price is '来源最低成交价，人民币元；竞价指示价不一定处于已成交价格区间内。';
comment on column realtime.call_auction_market_series_snapshot.cumulative_volume is 'auction_indicative语义下为买一指示数量；成交语义下为来源累计成交量，股。';
comment on column realtime.call_auction_market_series_snapshot.cumulative_amount is 'auction_indicative语义下为指示价乘指示数量；成交语义下为累计成交额，人民币元。';
comment on column realtime.call_auction_market_series_snapshot.source_code is '实际数据来源代码；单个成功采集批次不合并多个来源。';
comment on column realtime.call_auction_market_series_snapshot.created_at is '记录首次写入时间（带时区）。';
comment on column realtime.call_auction_market_series_snapshot.value_semantics is '数值语义：auction_indicative为竞价指示值，opening_trade为开盘成交值；历史未知语义按原标签保留。';
comment on column realtime.call_auction_market_series_snapshot.batch_code is '按上海时区计划采样时刻生成的六位批次代码HHMMSS，如091820；不替代轮次自然键。';
comment on column realtime.call_auction_market_series_snapshot.bid1_price is '买1档报价，人民币元；缺失价格不以零替代。';
comment on column realtime.call_auction_market_series_snapshot.bid1_volume is '买1档数量，股；缺失数量不以零替代。';
comment on column realtime.call_auction_market_series_snapshot.bid2_price is '买2档报价，人民币元；缺失价格不以零替代。';
comment on column realtime.call_auction_market_series_snapshot.bid2_volume is '买2档数量，股；缺失数量不以零替代。';
comment on column realtime.call_auction_market_series_snapshot.bid3_price is '买3档报价，人民币元；缺失价格不以零替代。';
comment on column realtime.call_auction_market_series_snapshot.bid3_volume is '买3档数量，股；缺失数量不以零替代。';
comment on column realtime.call_auction_market_series_snapshot.bid4_price is '买4档报价，人民币元；缺失价格不以零替代。';
comment on column realtime.call_auction_market_series_snapshot.bid4_volume is '买4档数量，股；缺失数量不以零替代。';
comment on column realtime.call_auction_market_series_snapshot.bid5_price is '买5档报价，人民币元；缺失价格不以零替代。';
comment on column realtime.call_auction_market_series_snapshot.bid5_volume is '买5档数量，股；缺失数量不以零替代。';
comment on column realtime.call_auction_market_series_snapshot.ask1_price is '卖1档报价，人民币元；缺失价格不以零替代。';
comment on column realtime.call_auction_market_series_snapshot.ask1_volume is '卖1档数量，股；缺失数量不以零替代。';
comment on column realtime.call_auction_market_series_snapshot.ask2_price is '卖2档报价，人民币元；缺失价格不以零替代。';
comment on column realtime.call_auction_market_series_snapshot.ask2_volume is '卖2档数量，股；缺失数量不以零替代。';
comment on column realtime.call_auction_market_series_snapshot.ask3_price is '卖3档报价，人民币元；缺失价格不以零替代。';
comment on column realtime.call_auction_market_series_snapshot.ask3_volume is '卖3档数量，股；缺失数量不以零替代。';
comment on column realtime.call_auction_market_series_snapshot.ask4_price is '卖4档报价，人民币元；缺失价格不以零替代。';
comment on column realtime.call_auction_market_series_snapshot.ask4_volume is '卖4档数量，股；缺失数量不以零替代。';
comment on column realtime.call_auction_market_series_snapshot.ask5_price is '卖5档报价，人民币元；缺失价格不以零替代。';
comment on column realtime.call_auction_market_series_snapshot.ask5_volume is '卖5档数量，股；缺失数量不以零替代。';

-- realtime.call_auction_market_series_snapshot_history
comment on table realtime.call_auction_market_series_snapshot_history is '全市场竞价序列历史归档；保留原快照身份、数值语义及采集血缘。';
comment on column realtime.call_auction_market_series_snapshot_history.trade_date is '交易所本地交易日期；以统一交易日历为准。';
comment on column realtime.call_auction_market_series_snapshot_history.ingestion_id is '采集批次标识，关联 ingestion.ingestion_run，用于追溯来源和 Raw 对象。';
comment on column realtime.call_auction_market_series_snapshot_history.session_id is '竞价采集会话标识。';
comment on column realtime.call_auction_market_series_snapshot_history.sample_seq is '会话内计划采样序号；不是采集批次标识。';
comment on column realtime.call_auction_market_series_snapshot_history.scheduled_at is '计划采样时间（带时区），按Asia/Shanghai交易时槽定义。';
comment on column realtime.call_auction_market_series_snapshot_history.symbol is '标准证券代码，格式如 SSE:600000、SZSE:000001、BSE:920000；保留代码前导零。';
comment on column realtime.call_auction_market_series_snapshot_history.observed_at is '本次来源事实观测时间（带时区）；不等于计划时间或入库时间。';
comment on column realtime.call_auction_market_series_snapshot_history.last_price is 'auction_indicative语义下为买一指示价；opening_trade等成交语义下为来源最新成交价，人民币元。';
comment on column realtime.call_auction_market_series_snapshot_history.previous_close is '上一交易日收盘价，人民币元；来源事实不补造缺失值。';
comment on column realtime.call_auction_market_series_snapshot_history.high_price is '来源最高成交价，人民币元；竞价指示价不一定处于已成交价格区间内。';
comment on column realtime.call_auction_market_series_snapshot_history.low_price is '来源最低成交价，人民币元；竞价指示价不一定处于已成交价格区间内。';
comment on column realtime.call_auction_market_series_snapshot_history.cumulative_volume is 'auction_indicative语义下为买一指示数量；成交语义下为来源累计成交量，股。';
comment on column realtime.call_auction_market_series_snapshot_history.cumulative_amount is 'auction_indicative语义下为指示价乘指示数量；成交语义下为累计成交额，人民币元。';
comment on column realtime.call_auction_market_series_snapshot_history.source_code is '实际数据来源代码；单个成功采集批次不合并多个来源。';
comment on column realtime.call_auction_market_series_snapshot_history.created_at is '记录首次写入时间（带时区）。';
comment on column realtime.call_auction_market_series_snapshot_history.value_semantics is '数值语义：auction_indicative为竞价指示值，opening_trade为开盘成交值；历史未知语义按原标签保留。';
comment on column realtime.call_auction_market_series_snapshot_history.batch_code is '按上海时区计划采样时刻生成的六位批次代码HHMMSS，如091820；不替代轮次自然键。';
comment on column realtime.call_auction_market_series_snapshot_history.bid1_price is '买1档报价，人民币元；缺失价格不以零替代。';
comment on column realtime.call_auction_market_series_snapshot_history.bid1_volume is '买1档数量，股；缺失数量不以零替代。';
comment on column realtime.call_auction_market_series_snapshot_history.bid2_price is '买2档报价，人民币元；缺失价格不以零替代。';
comment on column realtime.call_auction_market_series_snapshot_history.bid2_volume is '买2档数量，股；缺失数量不以零替代。';
comment on column realtime.call_auction_market_series_snapshot_history.bid3_price is '买3档报价，人民币元；缺失价格不以零替代。';
comment on column realtime.call_auction_market_series_snapshot_history.bid3_volume is '买3档数量，股；缺失数量不以零替代。';
comment on column realtime.call_auction_market_series_snapshot_history.bid4_price is '买4档报价，人民币元；缺失价格不以零替代。';
comment on column realtime.call_auction_market_series_snapshot_history.bid4_volume is '买4档数量，股；缺失数量不以零替代。';
comment on column realtime.call_auction_market_series_snapshot_history.bid5_price is '买5档报价，人民币元；缺失价格不以零替代。';
comment on column realtime.call_auction_market_series_snapshot_history.bid5_volume is '买5档数量，股；缺失数量不以零替代。';
comment on column realtime.call_auction_market_series_snapshot_history.ask1_price is '卖1档报价，人民币元；缺失价格不以零替代。';
comment on column realtime.call_auction_market_series_snapshot_history.ask1_volume is '卖1档数量，股；缺失数量不以零替代。';
comment on column realtime.call_auction_market_series_snapshot_history.ask2_price is '卖2档报价，人民币元；缺失价格不以零替代。';
comment on column realtime.call_auction_market_series_snapshot_history.ask2_volume is '卖2档数量，股；缺失数量不以零替代。';
comment on column realtime.call_auction_market_series_snapshot_history.ask3_price is '卖3档报价，人民币元；缺失价格不以零替代。';
comment on column realtime.call_auction_market_series_snapshot_history.ask3_volume is '卖3档数量，股；缺失数量不以零替代。';
comment on column realtime.call_auction_market_series_snapshot_history.ask4_price is '卖4档报价，人民币元；缺失价格不以零替代。';
comment on column realtime.call_auction_market_series_snapshot_history.ask4_volume is '卖4档数量，股；缺失数量不以零替代。';
comment on column realtime.call_auction_market_series_snapshot_history.ask5_price is '卖5档报价，人民币元；缺失价格不以零替代。';
comment on column realtime.call_auction_market_series_snapshot_history.ask5_volume is '卖5档数量，股；缺失数量不以零替代。';

-- realtime.call_auction_market_snapshot
comment on table realtime.call_auction_market_snapshot is '每日09:26全市场竞价快照来源事实；保存价格、量额、五档及封单额。';
comment on column realtime.call_auction_market_snapshot.ingestion_id is '采集批次标识，关联 ingestion.ingestion_run，用于追溯来源和 Raw 对象。';
comment on column realtime.call_auction_market_snapshot.symbol is '标准证券代码，格式如 SSE:600000、SZSE:000001、BSE:920000；保留代码前导零。';
comment on column realtime.call_auction_market_snapshot.trade_date is '交易所本地交易日期；以统一交易日历为准。';
comment on column realtime.call_auction_market_snapshot.observed_at is '本次来源事实观测时间（带时区）；不等于计划时间或入库时间。';
comment on column realtime.call_auction_market_snapshot.last_price is '来源最新价，人民币元；是否为竞价指示价由数值语义字段确定。';
comment on column realtime.call_auction_market_snapshot.previous_close is '上一交易日收盘价，人民币元；来源事实不补造缺失值。';
comment on column realtime.call_auction_market_snapshot.high_price is '来源最高成交价，人民币元；竞价指示价不一定处于已成交价格区间内。';
comment on column realtime.call_auction_market_snapshot.low_price is '来源最低成交价，人民币元；竞价指示价不一定处于已成交价格区间内。';
comment on column realtime.call_auction_market_snapshot.cumulative_volume is '来源累计数量，股；竞价阶段的具体含义由数值语义确定。';
comment on column realtime.call_auction_market_snapshot.cumulative_amount is '来源累计金额，人民币元；竞价阶段的具体含义由数值语义确定。';
comment on column realtime.call_auction_market_snapshot.source_code is '实际数据来源代码；单个成功采集批次不合并多个来源。';
comment on column realtime.call_auction_market_snapshot.created_at is '记录首次写入时间（带时区）。';
comment on column realtime.call_auction_market_snapshot.bid1_price is '买1档报价，人民币元；缺失价格不以零替代。';
comment on column realtime.call_auction_market_snapshot.bid1_volume is '买1档数量，股；缺失数量不以零替代。';
comment on column realtime.call_auction_market_snapshot.bid2_price is '买2档报价，人民币元；缺失价格不以零替代。';
comment on column realtime.call_auction_market_snapshot.bid2_volume is '买2档数量，股；缺失数量不以零替代。';
comment on column realtime.call_auction_market_snapshot.bid3_price is '买3档报价，人民币元；缺失价格不以零替代。';
comment on column realtime.call_auction_market_snapshot.bid3_volume is '买3档数量，股；缺失数量不以零替代。';
comment on column realtime.call_auction_market_snapshot.bid4_price is '买4档报价，人民币元；缺失价格不以零替代。';
comment on column realtime.call_auction_market_snapshot.bid4_volume is '买4档数量，股；缺失数量不以零替代。';
comment on column realtime.call_auction_market_snapshot.bid5_price is '买5档报价，人民币元；缺失价格不以零替代。';
comment on column realtime.call_auction_market_snapshot.bid5_volume is '买5档数量，股；缺失数量不以零替代。';
comment on column realtime.call_auction_market_snapshot.ask1_price is '卖1档报价，人民币元；缺失价格不以零替代。';
comment on column realtime.call_auction_market_snapshot.ask1_volume is '卖1档数量，股；缺失数量不以零替代。';
comment on column realtime.call_auction_market_snapshot.ask2_price is '卖2档报价，人民币元；缺失价格不以零替代。';
comment on column realtime.call_auction_market_snapshot.ask2_volume is '卖2档数量，股；缺失数量不以零替代。';
comment on column realtime.call_auction_market_snapshot.ask3_price is '卖3档报价，人民币元；缺失价格不以零替代。';
comment on column realtime.call_auction_market_snapshot.ask3_volume is '卖3档数量，股；缺失数量不以零替代。';
comment on column realtime.call_auction_market_snapshot.ask4_price is '卖4档报价，人民币元；缺失价格不以零替代。';
comment on column realtime.call_auction_market_snapshot.ask4_volume is '卖4档数量，股；缺失数量不以零替代。';
comment on column realtime.call_auction_market_snapshot.ask5_price is '卖5档报价，人民币元；缺失价格不以零替代。';
comment on column realtime.call_auction_market_snapshot.ask5_volume is '卖5档数量，股；缺失数量不以零替代。';
comment on column realtime.call_auction_market_snapshot.seal_amount is '封单金额，人民币元；具体方向及计算时点由所属领域算法定义。';

-- realtime.call_auction_snapshot
comment on table realtime.call_auction_snapshot is '每日固定时点竞价快照；保存价格、量额及竞价溢价率。';
comment on column realtime.call_auction_snapshot.symbol is '标准证券代码，格式如 SSE:600000、SZSE:000001、BSE:920000；保留代码前导零。';
comment on column realtime.call_auction_snapshot.trade_date is '交易所本地交易日期；以统一交易日历为准。';
comment on column realtime.call_auction_snapshot.last_price is '来源最新价，人民币元；是否为竞价指示价由数值语义字段确定。';
comment on column realtime.call_auction_snapshot.previous_close is '上一交易日收盘价，人民币元；来源事实不补造缺失值。';
comment on column realtime.call_auction_snapshot.cumulative_volume is '来源累计数量，股；竞价阶段的具体含义由数值语义确定。';
comment on column realtime.call_auction_snapshot.cumulative_amount is '来源累计金额，人民币元；竞价阶段的具体含义由数值语义确定。';
comment on column realtime.call_auction_snapshot.auction_premium_pct is '竞价溢价率，百分点；1表示1%。';
comment on column realtime.call_auction_snapshot.source_code is '实际数据来源代码；单个成功采集批次不合并多个来源。';
comment on column realtime.call_auction_snapshot.ingestion_id is '采集批次标识，关联 ingestion.ingestion_run，用于追溯来源和 Raw 对象。';
comment on column realtime.call_auction_snapshot.created_at is '记录首次写入时间（带时区）。';
comment on column realtime.call_auction_snapshot.observed_at is '本次来源事实观测时间（带时区）；不等于计划时间或入库时间。';

-- realtime.eod_quote_snapshot
comment on table realtime.eod_quote_snapshot is '收盘五档报价快照；保存同日收盘盘口及封单额来源事实。';
comment on column realtime.eod_quote_snapshot.symbol is '标准证券代码，格式如 SSE:600000、SZSE:000001、BSE:920000；保留代码前导零。';
comment on column realtime.eod_quote_snapshot.trade_date is '交易所本地交易日期；以统一交易日历为准。';
comment on column realtime.eod_quote_snapshot.last_price is '来源最新价，人民币元；是否为竞价指示价由数值语义字段确定。';
comment on column realtime.eod_quote_snapshot.previous_close is '上一交易日收盘价，人民币元；来源事实不补造缺失值。';
comment on column realtime.eod_quote_snapshot.bid1_price is '买1档报价，人民币元；缺失价格不以零替代。';
comment on column realtime.eod_quote_snapshot.bid1_volume is '买1档数量，股；缺失数量不以零替代。';
comment on column realtime.eod_quote_snapshot.bid2_price is '买2档报价，人民币元；缺失价格不以零替代。';
comment on column realtime.eod_quote_snapshot.bid2_volume is '买2档数量，股；缺失数量不以零替代。';
comment on column realtime.eod_quote_snapshot.bid3_price is '买3档报价，人民币元；缺失价格不以零替代。';
comment on column realtime.eod_quote_snapshot.bid3_volume is '买3档数量，股；缺失数量不以零替代。';
comment on column realtime.eod_quote_snapshot.bid4_price is '买4档报价，人民币元；缺失价格不以零替代。';
comment on column realtime.eod_quote_snapshot.bid4_volume is '买4档数量，股；缺失数量不以零替代。';
comment on column realtime.eod_quote_snapshot.bid5_price is '买5档报价，人民币元；缺失价格不以零替代。';
comment on column realtime.eod_quote_snapshot.bid5_volume is '买5档数量，股；缺失数量不以零替代。';
comment on column realtime.eod_quote_snapshot.ask1_price is '卖1档报价，人民币元；缺失价格不以零替代。';
comment on column realtime.eod_quote_snapshot.ask1_volume is '卖1档数量，股；缺失数量不以零替代。';
comment on column realtime.eod_quote_snapshot.ask2_price is '卖2档报价，人民币元；缺失价格不以零替代。';
comment on column realtime.eod_quote_snapshot.ask2_volume is '卖2档数量，股；缺失数量不以零替代。';
comment on column realtime.eod_quote_snapshot.ask3_price is '卖3档报价，人民币元；缺失价格不以零替代。';
comment on column realtime.eod_quote_snapshot.ask3_volume is '卖3档数量，股；缺失数量不以零替代。';
comment on column realtime.eod_quote_snapshot.ask4_price is '卖4档报价，人民币元；缺失价格不以零替代。';
comment on column realtime.eod_quote_snapshot.ask4_volume is '卖4档数量，股；缺失数量不以零替代。';
comment on column realtime.eod_quote_snapshot.ask5_price is '卖5档报价，人民币元；缺失价格不以零替代。';
comment on column realtime.eod_quote_snapshot.ask5_volume is '卖5档数量，股；缺失数量不以零替代。';
comment on column realtime.eod_quote_snapshot.seal_amount is '封单金额，人民币元；具体方向及计算时点由所属领域算法定义。';
comment on column realtime.eod_quote_snapshot.source_code is '实际数据来源代码；单个成功采集批次不合并多个来源。';
comment on column realtime.eod_quote_snapshot.ingestion_id is '采集批次标识，关联 ingestion.ingestion_run，用于追溯来源和 Raw 对象。';
comment on column realtime.eod_quote_snapshot.created_at is '记录首次写入时间（带时区）。';

-- realtime.five_level_quote_snapshot
comment on table realtime.five_level_quote_snapshot is '竞价五档报价来源快照；保存采样身份、报价语义及原始采集血缘。';
comment on column realtime.five_level_quote_snapshot.session_id is '竞价采集会话标识。';
comment on column realtime.five_level_quote_snapshot.pool_snapshot_id is '采集会话所使用的股票池快照标识。';
comment on column realtime.five_level_quote_snapshot.ingestion_id is '采集批次标识，关联 ingestion.ingestion_run，用于追溯来源和 Raw 对象。';
comment on column realtime.five_level_quote_snapshot.raw_id is '不可变原始数据对象标识，关联 ingestion.raw_manifest。';
comment on column realtime.five_level_quote_snapshot.symbol is '标准证券代码，格式如 SSE:600000、SZSE:000001、BSE:920000；保留代码前导零。';
comment on column realtime.five_level_quote_snapshot.sample_seq is '会话内计划采样序号；不是采集批次标识。';
comment on column realtime.five_level_quote_snapshot.scheduled_at is '计划采样时间（带时区），按Asia/Shanghai交易时槽定义。';
comment on column realtime.five_level_quote_snapshot.collected_at is '来源采集完成时间（带时区），独立于异步持久化提交时间。';
comment on column realtime.five_level_quote_snapshot.source_timestamp is '来源返回的行情时间（带时区）；未知时为空。';
comment on column realtime.five_level_quote_snapshot.phase is '竞价采集阶段代码。';
comment on column realtime.five_level_quote_snapshot.quote_semantics is '报价数值语义；区分竞价指示与其他来源报价含义。';
comment on column realtime.five_level_quote_snapshot.quote_status is '来源报价状态；缺失或停牌不等同于零报价。';
comment on column realtime.five_level_quote_snapshot.last_price is '来源最新价，人民币元；是否为竞价指示价由数值语义字段确定。';
comment on column realtime.five_level_quote_snapshot.previous_close is '上一交易日收盘价，人民币元；来源事实不补造缺失值。';
comment on column realtime.five_level_quote_snapshot.open is '开盘价，人民币元。';
comment on column realtime.five_level_quote_snapshot.high is '最高价，人民币元。';
comment on column realtime.five_level_quote_snapshot.low is '最低价，人民币元。';
comment on column realtime.five_level_quote_snapshot.cumulative_volume is '来源累计数量，股；竞价阶段的具体含义由数值语义确定。';
comment on column realtime.five_level_quote_snapshot.cumulative_amount is '来源累计金额，人民币元；竞价阶段的具体含义由数值语义确定。';
comment on column realtime.five_level_quote_snapshot.bid1_price is '买1档报价，人民币元；缺失价格不以零替代。';
comment on column realtime.five_level_quote_snapshot.bid1_volume is '买1档数量，股；缺失数量不以零替代。';
comment on column realtime.five_level_quote_snapshot.ask1_price is '卖1档报价，人民币元；缺失价格不以零替代。';
comment on column realtime.five_level_quote_snapshot.ask1_volume is '卖1档数量，股；缺失数量不以零替代。';
comment on column realtime.five_level_quote_snapshot.bid2_price is '买2档报价，人民币元；缺失价格不以零替代。';
comment on column realtime.five_level_quote_snapshot.bid2_volume is '买2档数量，股；缺失数量不以零替代。';
comment on column realtime.five_level_quote_snapshot.ask2_price is '卖2档报价，人民币元；缺失价格不以零替代。';
comment on column realtime.five_level_quote_snapshot.ask2_volume is '卖2档数量，股；缺失数量不以零替代。';
comment on column realtime.five_level_quote_snapshot.bid3_price is '买3档报价，人民币元；缺失价格不以零替代。';
comment on column realtime.five_level_quote_snapshot.bid3_volume is '买3档数量，股；缺失数量不以零替代。';
comment on column realtime.five_level_quote_snapshot.ask3_price is '卖3档报价，人民币元；缺失价格不以零替代。';
comment on column realtime.five_level_quote_snapshot.ask3_volume is '卖3档数量，股；缺失数量不以零替代。';
comment on column realtime.five_level_quote_snapshot.bid4_price is '买4档报价，人民币元；缺失价格不以零替代。';
comment on column realtime.five_level_quote_snapshot.bid4_volume is '买4档数量，股；缺失数量不以零替代。';
comment on column realtime.five_level_quote_snapshot.ask4_price is '卖4档报价，人民币元；缺失价格不以零替代。';
comment on column realtime.five_level_quote_snapshot.ask4_volume is '卖4档数量，股；缺失数量不以零替代。';
comment on column realtime.five_level_quote_snapshot.bid5_price is '买5档报价，人民币元；缺失价格不以零替代。';
comment on column realtime.five_level_quote_snapshot.bid5_volume is '买5档数量，股；缺失数量不以零替代。';
comment on column realtime.five_level_quote_snapshot.ask5_price is '卖5档报价，人民币元；缺失价格不以零替代。';
comment on column realtime.five_level_quote_snapshot.ask5_volume is '卖5档数量，股；缺失数量不以零替代。';
comment on column realtime.five_level_quote_snapshot.source_code is '实际数据来源代码；单个成功采集批次不合并多个来源。';
comment on column realtime.five_level_quote_snapshot.created_at is '记录首次写入时间（带时区）。';

-- realtime.stock_quote_snapshot
comment on table realtime.stock_quote_snapshot is '股票实时行情来源快照；保存来源时间、报价状态和五档盘口，不作为历史日行情补缺来源。';
comment on column realtime.stock_quote_snapshot.ingestion_id is '采集批次标识，关联 ingestion.ingestion_run，用于追溯来源和 Raw 对象。';
comment on column realtime.stock_quote_snapshot.raw_id is '不可变原始数据对象标识，关联 ingestion.raw_manifest。';
comment on column realtime.stock_quote_snapshot.symbol is '标准证券代码，格式如 SSE:600000、SZSE:000001、BSE:920000；保留代码前导零。';
comment on column realtime.stock_quote_snapshot.observed_at is '本次来源事实观测时间（带时区）；不等于计划时间或入库时间。';
comment on column realtime.stock_quote_snapshot.source_timestamp is '来源返回的行情时间（带时区）；未知时为空。';
comment on column realtime.stock_quote_snapshot.quote_status is '来源报价状态；缺失或停牌不等同于零报价。';
comment on column realtime.stock_quote_snapshot.last_price is '来源最新价，人民币元；是否为竞价指示价由数值语义字段确定。';
comment on column realtime.stock_quote_snapshot.previous_close is '上一交易日收盘价，人民币元；来源事实不补造缺失值。';
comment on column realtime.stock_quote_snapshot.open is '开盘价，人民币元。';
comment on column realtime.stock_quote_snapshot.high is '最高价，人民币元。';
comment on column realtime.stock_quote_snapshot.low is '最低价，人民币元。';
comment on column realtime.stock_quote_snapshot.cumulative_volume is '来源累计数量，股；竞价阶段的具体含义由数值语义确定。';
comment on column realtime.stock_quote_snapshot.cumulative_amount is '来源累计金额，人民币元；竞价阶段的具体含义由数值语义确定。';
comment on column realtime.stock_quote_snapshot.bid1_price is '买1档报价，人民币元；缺失价格不以零替代。';
comment on column realtime.stock_quote_snapshot.bid1_volume is '买1档数量，股；缺失数量不以零替代。';
comment on column realtime.stock_quote_snapshot.bid2_price is '买2档报价，人民币元；缺失价格不以零替代。';
comment on column realtime.stock_quote_snapshot.bid2_volume is '买2档数量，股；缺失数量不以零替代。';
comment on column realtime.stock_quote_snapshot.bid3_price is '买3档报价，人民币元；缺失价格不以零替代。';
comment on column realtime.stock_quote_snapshot.bid3_volume is '买3档数量，股；缺失数量不以零替代。';
comment on column realtime.stock_quote_snapshot.bid4_price is '买4档报价，人民币元；缺失价格不以零替代。';
comment on column realtime.stock_quote_snapshot.bid4_volume is '买4档数量，股；缺失数量不以零替代。';
comment on column realtime.stock_quote_snapshot.bid5_price is '买5档报价，人民币元；缺失价格不以零替代。';
comment on column realtime.stock_quote_snapshot.bid5_volume is '买5档数量，股；缺失数量不以零替代。';
comment on column realtime.stock_quote_snapshot.ask1_price is '卖1档报价，人民币元；缺失价格不以零替代。';
comment on column realtime.stock_quote_snapshot.ask1_volume is '卖1档数量，股；缺失数量不以零替代。';
comment on column realtime.stock_quote_snapshot.ask2_price is '卖2档报价，人民币元；缺失价格不以零替代。';
comment on column realtime.stock_quote_snapshot.ask2_volume is '卖2档数量，股；缺失数量不以零替代。';
comment on column realtime.stock_quote_snapshot.ask3_price is '卖3档报价，人民币元；缺失价格不以零替代。';
comment on column realtime.stock_quote_snapshot.ask3_volume is '卖3档数量，股；缺失数量不以零替代。';
comment on column realtime.stock_quote_snapshot.ask4_price is '卖4档报价，人民币元；缺失价格不以零替代。';
comment on column realtime.stock_quote_snapshot.ask4_volume is '卖4档数量，股；缺失数量不以零替代。';
comment on column realtime.stock_quote_snapshot.ask5_price is '卖5档报价，人民币元；缺失价格不以零替代。';
comment on column realtime.stock_quote_snapshot.ask5_volume is '卖5档数量，股；缺失数量不以零替代。';
comment on column realtime.stock_quote_snapshot.source_code is '实际数据来源代码；单个成功采集批次不合并多个来源。';
comment on column realtime.stock_quote_snapshot.created_at is '记录首次写入时间（带时区）。';

-- regulation.calculated_event
comment on table regulation.calculated_event is '监管监测客观计算事件；保存命中规则及对应计算窗口。';
comment on column regulation.calculated_event.calculation_id is '计算批次标识；用于追溯算法版本及冻结输入，同一逻辑结果不混用计算批次。';
comment on column regulation.calculated_event.symbol is '标准证券代码，格式如 SSE:600000、SZSE:000001、BSE:920000；保留代码前导零。';
comment on column regulation.calculated_event.trade_date is '交易所本地交易日期；以统一交易日历为准。';
comment on column regulation.calculated_event.level is '本次计算事件的监管级别。';
comment on column regulation.calculated_event.direction is '方向代码；具体允许值见所属领域与表约束。';
comment on column regulation.calculated_event.kind is '监管规则或计算事件类别。';
comment on column regulation.calculated_event.rule_codes is '本计算事件命中的规则代码集合。';
comment on column regulation.calculated_event.windows is '本计算事件对应的规则窗口集合。';

-- regulation.calculation_run
comment on table regulation.calculation_run is '监管规则计算批次；冻结交易日、规则集、输入水位及覆盖计数。';
comment on column regulation.calculation_run.calculation_id is '计算批次标识；用于追溯算法版本及冻结输入，同一逻辑结果不混用计算批次。';
comment on column regulation.calculation_run.trade_date is '交易所本地交易日期；以统一交易日历为准。';
comment on column regulation.calculation_run.next_trade_date is '下一交易日期，用于下一交易日客观边界计算。';
comment on column regulation.calculation_run.status is '记录或运行状态；允许值及终态约束见本表 CHECK 约束。';
comment on column regulation.calculation_run.algorithm_version is '计算算法版本；用于重现结果。';
comment on column regulation.calculation_run.rule_set_version is '监管规则集版本。';
comment on column regulation.calculation_run.rule_set_hash is '计算实际采用规则集的内容摘要。';
comment on column regulation.calculation_run.scenario_config_version is '下一交易日场景配置版本。';
comment on column regulation.calculation_run.input_hash is '规范化计算输入或采集内容的校验摘要。';
comment on column regulation.calculation_run.market_watermark is '行情输入水位。';
comment on column regulation.calculation_run.capital_watermark is '股本输入水位。';
comment on column regulation.calculation_run.event_watermark is '官方监管事件输入水位。';
comment on column regulation.calculation_run.expected_count is '预期评估证券数量。';
comment on column regulation.calculation_run.complete_count is '输入完整的证券数量。';
comment on column regulation.calculation_run.incomplete_count is '输入不完整的证券数量。';
comment on column regulation.calculation_run.not_applicable_count is '不适用规则的证券数量。';
comment on column regulation.calculation_run.started_at is '实际开始时间（带时区）。';
comment on column regulation.calculation_run.completed_at is '计算完成时间（带时区）。';

-- regulation.event
comment on table regulation.event is '交易所监管公告来源事件；保存公布时点、适用窗口、重置日期及不可变内容摘要。';
comment on column regulation.event.event_id is '来源事件唯一标识。';
comment on column regulation.event.symbol is '标准证券代码，格式如 SSE:600000、SZSE:000001、BSE:920000；保留代码前导零。';
comment on column regulation.event.exchange is '交易所代码：SSE、SZSE 或 BSE，具体范围由表约束确定。';
comment on column regulation.event.segment is '规则适用的交易所板块代码。';
comment on column regulation.event.event_type is '领域事件类型代码。';
comment on column regulation.event.event_level is '官方监管事件级别。';
comment on column regulation.event.direction is '方向代码；具体允许值见所属领域与表约束。';
comment on column regulation.event.period_start_date is '来源事件统计期间起始日期。';
comment on column regulation.event.period_end_date is '来源事件统计期间结束日期。';
comment on column regulation.event.published_at is '官方公告公布时间（带时区）。';
comment on column regulation.event.effective_reset_date is '监管事件生效并重置统计窗口的日期。';
comment on column regulation.event.source_event_id is '来源系统事件标识。';
comment on column regulation.event.source_title is '官方公告标题。';
comment on column regulation.event.source_url is '官方来源地址。';
comment on column regulation.event.source_content_hash is '来源公告规范化内容摘要。';
comment on column regulation.event.source_code is '实际数据来源代码；单个成功采集批次不合并多个来源。';
comment on column regulation.event.explicit_rule_codes is '来源明确指出的规则代码集合；不从缺失信息推断。';
comment on column regulation.event.observed_at is '本次来源事实观测时间（带时区）；不等于计划时间或入库时间。';
comment on column regulation.event.ingestion_id is '采集批次标识，关联 ingestion.ingestion_run，用于追溯来源和 Raw 对象。';
comment on column regulation.event.created_at is '记录首次写入时间（带时区）。';

-- regulation.monitor_context
comment on table regulation.monitor_context is '监管监测计算上下文；关联父批次、官方覆盖与特别处理名单水位。';
comment on column regulation.monitor_context.calculation_id is '计算批次标识；用于追溯算法版本及冻结输入，同一逻辑结果不混用计算批次。';
comment on column regulation.monitor_context.parent_calculation_id is '父计算批次标识；保留计算上下文关联。';
comment on column regulation.monitor_context.source is '监管监测上下文来源代码。';
comment on column regulation.monitor_context.official_coverage is '官方事件输入覆盖说明。';
comment on column regulation.monitor_context.st_watermark is '特别处理证券名单输入水位；历史未捕获值保留原语义。';

-- regulation.monitor_input
comment on table regulation.monitor_input is '监管监测冻结输入；保存候选原因与标准化计算输入。';
comment on column regulation.monitor_input.calculation_id is '计算批次标识；用于追溯算法版本及冻结输入，同一逻辑结果不混用计算批次。';
comment on column regulation.monitor_input.symbol is '标准证券代码，格式如 SSE:600000、SZSE:000001、BSE:920000；保留代码前导零。';
comment on column regulation.monitor_input.name is '冻结监测输入时的证券名称。';
comment on column regulation.monitor_input.candidate_reasons is '证券进入监测候选集合的客观原因代码集合。';
comment on column regulation.monitor_input.payload is '冻结的标准化监管监测输入JSON对象。';

-- regulation.rule
comment on table regulation.rule is '交易监管客观规则目录；保存适用范围、窗口、阈值、有效期和官方出处。';
comment on column regulation.rule.rule_id is '监管规则标识。';
comment on column regulation.rule.rule_code is '质量或计算规则代码。';
comment on column regulation.rule.exchange is '交易所代码：SSE、SZSE 或 BSE，具体范围由表约束确定。';
comment on column regulation.rule.segment is '规则适用的交易所板块代码。';
comment on column regulation.rule.rule_name is '监管规则名称。';
comment on column regulation.rule.level is '监管规则级别，如普通异动或严重异动。';
comment on column regulation.rule.kind is '监管规则或计算事件类别。';
comment on column regulation.rule.direction is '方向代码；具体允许值见所属领域与表约束。';
comment on column regulation.rule.window_days is '规则统计窗口交易日数。';
comment on column regulation.rule.threshold_pct is '规则主阈值，百分点。';
comment on column regulation.rule.comparison_window_days is '比较窗口交易日数。';
comment on column regulation.rule.ratio_threshold is '规则比值阈值，无量纲倍数。';
comment on column regulation.rule.secondary_threshold_pct is '规则次阈值，百分点。';
comment on column regulation.rule.count_window_days is '事件计数窗口交易日数。';
comment on column regulation.rule.required_count is '规则要求达到的事件次数。';
comment on column regulation.rule.counted_event_kind is '参与计数的事件类别。';
comment on column regulation.rule.reset_level is '使统计窗口重置的监管事件级别。';
comment on column regulation.rule.benchmark_symbol is '基准指数标准代码。';
comment on column regulation.rule.rule_set_version is '监管规则集版本。';
comment on column regulation.rule.effective_date is '生效日期。';
comment on column regulation.rule.expire_date is '规则失效日期；为空表示尚未设置失效日。';
comment on column regulation.rule.source_document is '规则来源文件名称。';
comment on column regulation.rule.source_clause is '规则来源条款定位。';
comment on column regulation.rule.source_url is '官方来源地址。';
comment on column regulation.rule.enabled is '该规则是否启用。';
comment on column regulation.rule.dimension_window_days is '规则维度统计窗口交易日数。';
comment on column regulation.rule.dimension_comparison_window_days is '规则维度比较窗口交易日数。';
comment on column regulation.rule.created_at is '记录首次写入时间（带时区）。';
comment on column regulation.rule.updated_at is '记录最后更新时间（带时区）。';

-- regulation.rule_result
comment on table regulation.rule_result is '监管规则逐证券评估结果；保存命中结论、窗口、阈值与缺失原因。';
comment on column regulation.rule_result.calculation_id is '计算批次标识；用于追溯算法版本及冻结输入，同一逻辑结果不混用计算批次。';
comment on column regulation.rule_result.symbol is '标准证券代码，格式如 SSE:600000、SZSE:000001、BSE:920000；保留代码前导零。';
comment on column regulation.rule_result.rule_id is '监管规则标识。';
comment on column regulation.rule_result.evaluation_state is '单规则评估状态。';
comment on column regulation.rule_result.triggered is '是否达到本规则触发条件；数据缺失时遵循领域缺失语义。';
comment on column regulation.rule_result.window_start_date is '规则评估窗口起始交易日期。';
comment on column regulation.rule_result.window_end_date is '规则评估窗口结束交易日期。';
comment on column regulation.rule_result.observed_window_days is '本次评估实际观测到的窗口交易日数。';
comment on column regulation.rule_result.current_value is '规则当前计算值；量纲由对应规则类别定义。';
comment on column regulation.rule_result.threshold is '规则触发阈值；量纲与规则当前值一致。';
comment on column regulation.rule_result.distance is '当前值距触发阈值的距离；符号和量纲由对应规则定义。';
comment on column regulation.rule_result.secondary_current_value is '规则次条件当前计算值。';
comment on column regulation.rule_result.secondary_threshold is '规则次条件阈值，与次条件当前值同量纲。';
comment on column regulation.rule_result.event_count is '窗口内符合规则条件的事件次数。';
comment on column regulation.rule_result.required_count is '规则要求达到的事件次数。';
comment on column regulation.rule_result.selected_reset_date is '本次评估实际采用的重置日期。';
comment on column regulation.rule_result.data_completeness is '本结果所需输入的数据完整性状态。';
comment on column regulation.rule_result.incomplete_reason is '输入不完整或无法评估的具体原因。';
comment on column regulation.rule_result.created_at is '记录首次写入时间（带时区）。';

-- regulation.st_day_snapshot
comment on table regulation.st_day_snapshot is '每日特别处理证券名单来源快照；保留指定交易日名单及采集血缘。';
comment on column regulation.st_day_snapshot.trade_date is '交易所本地交易日期；以统一交易日历为准。';
comment on column regulation.st_day_snapshot.symbols is '该交易日特别处理证券的标准代码集合。';
comment on column regulation.st_day_snapshot.symbol_count is '名单内证券数量。';
comment on column regulation.st_day_snapshot.source_code is '实际数据来源代码；单个成功采集批次不合并多个来源。';
comment on column regulation.st_day_snapshot.ingestion_id is '采集批次标识，关联 ingestion.ingestion_run，用于追溯来源和 Raw 对象。';
comment on column regulation.st_day_snapshot.created_at is '记录首次写入时间（带时区）。';

-- regulation.status
comment on table regulation.status is '单证券每日监管客观状态计算结果；保存数据完整度、偏离值及异动计数。';
comment on column regulation.status.calculation_id is '计算批次标识；用于追溯算法版本及冻结输入，同一逻辑结果不混用计算批次。';
comment on column regulation.status.trade_date is '交易所本地交易日期；以统一交易日历为准。';
comment on column regulation.status.symbol is '标准证券代码，格式如 SSE:600000、SZSE:000001、BSE:920000；保留代码前导零。';
comment on column regulation.status.exchange is '交易所代码：SSE、SZSE 或 BSE，具体范围由表约束确定。';
comment on column regulation.status.segment is '规则适用的交易所板块代码。';
comment on column regulation.status.applicability is '规则适用性结论。';
comment on column regulation.status.applicability_reason is '规则适用或不适用原因。';
comment on column regulation.status.data_completeness is '本结果所需输入的数据完整性状态。';
comment on column regulation.status.calculated_state is '依据冻结输入计算的客观状态。';
comment on column regulation.status.announced_state is '已公布官方事件对应的状态；与计算状态分开保存。';
comment on column regulation.status.close is '收盘价，人民币元。';
comment on column regulation.status.stock_daily_return_pct is '证券当日涨跌幅，百分点。';
comment on column regulation.status.benchmark_symbol is '基准指数标准代码。';
comment on column regulation.status.benchmark_close is '基准指数收盘点位。';
comment on column regulation.status.benchmark_daily_return_pct is '基准指数当日涨跌幅，百分点。';
comment on column regulation.status.daily_deviation_pct is '证券当日涨跌幅减基准涨跌幅，百分点。';
comment on column regulation.status.abnormal_count_10d is '近10交易日异动次数。';
comment on column regulation.status.abnormal_count_10d_up is '近10交易日向上异动次数。';
comment on column regulation.status.abnormal_count_10d_down is '近10交易日向下异动次数。';
comment on column regulation.status.abnormal_reset_date is '普通异动统计采用的重置日期。';
comment on column regulation.status.serious_reset_date is '严重异动统计采用的重置日期。';
comment on column regulation.status.created_at is '记录首次写入时间（带时区）。';

-- regulation.warning
comment on table regulation.warning is '监管规则客观触发边界结果；保存下一交易日场景与触发价格，不表达交易建议。';
comment on column regulation.warning.calculation_id is '计算批次标识；用于追溯算法版本及冻结输入，同一逻辑结果不混用计算批次。';
comment on column regulation.warning.trade_date is '交易所本地交易日期；以统一交易日历为准。';
comment on column regulation.warning.next_trade_date is '下一交易日期，用于下一交易日客观边界计算。';
comment on column regulation.warning.symbol is '标准证券代码，格式如 SSE:600000、SZSE:000001、BSE:920000；保留代码前导零。';
comment on column regulation.warning.rule_id is '监管规则标识。';
comment on column regulation.warning.warning_type is '客观监管边界提示类型。';
comment on column regulation.warning.level is '本条客观触发边界对应的监管级别。';
comment on column regulation.warning.direction is '方向代码；具体允许值见所属领域与表约束。';
comment on column regulation.warning.current_value is '规则当前计算值；量纲由对应规则类别定义。';
comment on column regulation.warning.threshold is '规则触发阈值；量纲与规则当前值一致。';
comment on column regulation.warning.distance is '当前值距触发阈值的距离；符号和量纲由对应规则定义。';
comment on column regulation.warning.scenario_code is '下一交易日基准涨跌幅场景代码。';
comment on column regulation.warning.scenario_index_pct is '下一交易日基准涨跌幅场景值，百分点。';
comment on column regulation.warning.next_day_reference_price is '下一交易日计算参考价格，人民币元。';
comment on column regulation.warning.raw_trigger_price is '按规则推导但尚未按最小价位取整的触发价格，人民币元。';
comment on column regulation.warning.next_day_trigger_price is '按规则及最小价位处理后的下一交易日触发价格，人民币元。';
comment on column regulation.warning.next_day_trigger_pct is '下一交易日触发涨跌幅，百分点。';
comment on column regulation.warning.price_limit_ratio is '下一交易日涨跌停幅度，小数比例。';
comment on column regulation.warning.lower_limit_price is '下一交易日跌停价格，人民币元。';
comment on column regulation.warning.upper_limit_price is '下一交易日涨停价格，人民币元。';
comment on column regulation.warning.reachability is '客观触发价格在涨跌停约束下是否可达。';
comment on column regulation.warning.window_start_date is '规则评估窗口起始交易日期。';
comment on column regulation.warning.window_end_date is '规则评估窗口结束交易日期。';
comment on column regulation.warning.requires_official_event_confirmation is '该结论是否仍需官方事件确认。';
comment on column regulation.warning.message_template_code is '结果说明文本模板代码。';
comment on column regulation.warning.message is '可读结果或诊断说明。';
comment on column regulation.warning.created_at is '记录首次写入时间（带时区）。';

-- stock_pool.calculation_quality
comment on table stock_pool.calculation_quality is '股票池计算质量结果；记录拒绝或缺失原因。';
comment on column stock_pool.calculation_quality.calculation_id is '计算批次标识；用于追溯算法版本及冻结输入，同一逻辑结果不混用计算批次。';
comment on column stock_pool.calculation_quality.rule_code is '质量或计算规则代码。';
comment on column stock_pool.calculation_quality.severity is '质量问题严重级别。';
comment on column stock_pool.calculation_quality.symbol is '标准证券代码，格式如 SSE:600000、SZSE:000001、BSE:920000；保留代码前导零。';
comment on column stock_pool.calculation_quality.message is '可读结果或诊断说明。';
comment on column stock_pool.calculation_quality.created_at is '记录首次写入时间（带时区）。';

-- stock_pool.member
comment on table stock_pool.member is '不可变通用股票池成员；保存证券和涨跌停方向。';
comment on column stock_pool.member.snapshot_id is '不可变快照标识。';
comment on column stock_pool.member.symbol is '标准证券代码，格式如 SSE:600000、SZSE:000001、BSE:920000；保留代码前导零。';
comment on column stock_pool.member.direction is '方向代码；具体允许值见所属领域与表约束。';

-- stock_pool.snapshot
comment on table stock_pool.snapshot is '不可变通用股票池快照头；记录基准交易日、精确生效日、版本和计算血缘。';
comment on column stock_pool.snapshot.snapshot_id is '不可变快照标识。';
comment on column stock_pool.snapshot.calculation_id is '计算批次标识；用于追溯算法版本及冻结输入，同一逻辑结果不混用计算批次。';
comment on column stock_pool.snapshot.pool_code is '通用股票池代码。';
comment on column stock_pool.snapshot.basis_trade_date is '生成股票池所依据的基准交易日期。';
comment on column stock_pool.snapshot.effective_trade_date is '股票池精确生效交易日期；不得回退到较早日期。';
comment on column stock_pool.snapshot.version is '同一自然范围内的快照版本号。';
comment on column stock_pool.snapshot.status is '记录或运行状态；允许值及终态约束见本表 CHECK 约束。';
comment on column stock_pool.snapshot.member_count is '快照或聚合范围内的成员数量。';
comment on column stock_pool.snapshot.candidate_count is '计算候选证券数量。';
comment on column stock_pool.snapshot.rejected_count is '被质量规则拒绝的候选数量。';
comment on column stock_pool.snapshot.content_hash is '规范化记录或快照内容的校验摘要。';
comment on column stock_pool.snapshot.input_hash is '规范化计算输入或采集内容的校验摘要。';
comment on column stock_pool.snapshot.rule_version is '客观计算规则版本。';
comment on column stock_pool.snapshot.algorithm_version is '计算算法版本；用于重现结果。';
comment on column stock_pool.snapshot.generated_at is '快照生成时间（带时区）。';

-- today_limit_down.calculation_quality
comment on table today_limit_down.calculation_quality is '同日跌停快照计算质量结果。';
comment on column today_limit_down.calculation_quality.snapshot_id is '不可变快照标识。';
comment on column today_limit_down.calculation_quality.rule_code is '质量或计算规则代码。';
comment on column today_limit_down.calculation_quality.severity is '质量问题严重级别。';
comment on column today_limit_down.calculation_quality.symbol is '标准证券代码，格式如 SSE:600000、SZSE:000001、BSE:920000；保留代码前导零。';
comment on column today_limit_down.calculation_quality.message is '可读结果或诊断说明。';
comment on column today_limit_down.calculation_quality.created_at is '记录首次写入时间（带时区）。';

-- today_limit_down.member
comment on table today_limit_down.member is '同日跌停不可变快照成员；保存当日行情、自由流通市值、封板观测及收盘盘口。';
comment on column today_limit_down.member.snapshot_id is '不可变快照标识。';
comment on column today_limit_down.member.symbol is '标准证券代码，格式如 SSE:600000、SZSE:000001、BSE:920000；保留代码前导零。';
comment on column today_limit_down.member.code is '交易所六位证券代码字符串；保留前导零。';
comment on column today_limit_down.member.historical_name is '该交易日适用的历史证券名称。';
comment on column today_limit_down.member.previous_close is '上一交易日收盘价，人民币元；来源事实不补造缺失值。';
comment on column today_limit_down.member.close is '收盘价，人民币元。';
comment on column today_limit_down.member.limit_price is '对应方向的涨停或跌停价格，人民币元。';
comment on column today_limit_down.member.change_percent is '涨跌幅，百分点；1表示1%。';
comment on column today_limit_down.member.free_float_shares is '自由流通股本，股；与流通股本口径不同。';
comment on column today_limit_down.member.free_float_market_cap_cny is '同日未复权收盘价乘同日自由流通股本，人民币元。';
comment on column today_limit_down.member.first_limit_down_at is '来源报告的当日首次封跌停时间（带时区）。';
comment on column today_limit_down.member.last_limit_down_at is '来源报告的当日末次封跌停时间（带时区）。';
comment on column today_limit_down.member.open_count is '来源报告的当日开板次数。';
comment on column today_limit_down.member.consecutive_limit_down_days is '来源报告的连续跌停交易日数。';
comment on column today_limit_down.member.limit_down_duration_seconds is '当日累计跌停时长，秒；缺少完整事件流时为空，不由首末封板时间推算。';
comment on column today_limit_down.member.duration_semantics is '封板时长语义；unavailable_without_event_stream表示缺少事件流，不能推算时长。';
comment on column today_limit_down.member.source_reported_sealed_funds_cny is '来源报告封单资金，人民币元；不等同于收盘盘口推导封单额。';
comment on column today_limit_down.member.closing_ask1_price is '收盘卖1档报价，人民币元。';
comment on column today_limit_down.member.closing_ask1_volume_shares is '收盘卖1档委托数量，股。';
comment on column today_limit_down.member.closing_ask2_price is '收盘卖2档报价，人民币元。';
comment on column today_limit_down.member.closing_ask2_volume_shares is '收盘卖2档委托数量，股。';
comment on column today_limit_down.member.closing_ask3_price is '收盘卖3档报价，人民币元。';
comment on column today_limit_down.member.closing_ask3_volume_shares is '收盘卖3档委托数量，股。';
comment on column today_limit_down.member.closing_ask4_price is '收盘卖4档报价，人民币元。';
comment on column today_limit_down.member.closing_ask4_volume_shares is '收盘卖4档委托数量，股。';
comment on column today_limit_down.member.closing_ask5_price is '收盘卖5档报价，人民币元。';
comment on column today_limit_down.member.closing_ask5_volume_shares is '收盘卖5档委托数量，股。';
comment on column today_limit_down.member.closing_ask1_sealing_amount_cny is '收盘卖一封单金额，人民币元；与来源报告封单资金分开保存。';
comment on column today_limit_down.member.daily_bar_ingestion_id is '当日日行情事实所属采集批次标识。';
comment on column today_limit_down.member.indicator_ingestion_id is '当日每日指标事实所属采集批次标识。';
comment on column today_limit_down.member.name_ingestion_id is '历史证券名称事实所属采集批次标识。';
comment on column today_limit_down.member.pool_calculation_id is '股票池派生事实所属计算批次标识。';
comment on column today_limit_down.member.source_observation_ingestion_id is '涨跌停来源观测所属采集批次标识。';
comment on column today_limit_down.member.source_observation_raw_id is '涨跌停来源观测所属原始对象标识。';
comment on column today_limit_down.member.order_book_ingestion_id is '收盘盘口来源事实所属采集批次标识。';
comment on column today_limit_down.member.created_at is '记录首次写入时间（带时区）。';

-- today_limit_down.snapshot
comment on table today_limit_down.snapshot is '同日跌停不可变快照头；记录精确交易日、版本、质量及计算血缘。';
comment on column today_limit_down.snapshot.snapshot_id is '不可变快照标识。';
comment on column today_limit_down.snapshot.calculation_id is '计算批次标识；用于追溯算法版本及冻结输入，同一逻辑结果不混用计算批次。';
comment on column today_limit_down.snapshot.trade_date is '交易所本地交易日期；以统一交易日历为准。';
comment on column today_limit_down.snapshot.version is '同一自然范围内的快照版本号。';
comment on column today_limit_down.snapshot.status is '记录或运行状态；允许值及终态约束见本表 CHECK 约束。';
comment on column today_limit_down.snapshot.member_count is '快照或聚合范围内的成员数量。';
comment on column today_limit_down.snapshot.candidate_count is '计算候选证券数量。';
comment on column today_limit_down.snapshot.rejected_count is '被质量规则拒绝的候选数量。';
comment on column today_limit_down.snapshot.content_hash is '规范化记录或快照内容的校验摘要。';
comment on column today_limit_down.snapshot.input_hash is '规范化计算输入或采集内容的校验摘要。';
comment on column today_limit_down.snapshot.rule_version is '客观计算规则版本。';
comment on column today_limit_down.snapshot.algorithm_version is '计算算法版本；用于重现结果。';
comment on column today_limit_down.snapshot.source_ingestion_id is '本快照采用的来源观测采集批次标识。';
comment on column today_limit_down.snapshot.generated_at is '快照生成时间（带时区）。';

-- today_limit_down.source_observation
comment on table today_limit_down.source_observation is '同日跌停池来源观测；保存首次/末次封板、开板次数及来源报告封单资金。';
comment on column today_limit_down.source_observation.ingestion_id is '采集批次标识，关联 ingestion.ingestion_run，用于追溯来源和 Raw 对象。';
comment on column today_limit_down.source_observation.raw_id is '不可变原始数据对象标识，关联 ingestion.raw_manifest。';
comment on column today_limit_down.source_observation.symbol is '标准证券代码，格式如 SSE:600000、SZSE:000001、BSE:920000；保留代码前导零。';
comment on column today_limit_down.source_observation.trade_date is '交易所本地交易日期；以统一交易日历为准。';
comment on column today_limit_down.source_observation.source_code is '实际数据来源代码；单个成功采集批次不合并多个来源。';
comment on column today_limit_down.source_observation.observed_at is '本次来源事实观测时间（带时区）；不等于计划时间或入库时间。';
comment on column today_limit_down.source_observation.source_name is '来源返回的证券名称。';
comment on column today_limit_down.source_observation.first_limit_down_at is '来源报告的当日首次封跌停时间（带时区）。';
comment on column today_limit_down.source_observation.last_limit_down_at is '来源报告的当日末次封跌停时间（带时区）。';
comment on column today_limit_down.source_observation.open_count is '来源报告的当日开板次数。';
comment on column today_limit_down.source_observation.consecutive_limit_down_days is '来源报告的连续跌停交易日数。';
comment on column today_limit_down.source_observation.source_reported_sealed_funds_cny is '来源报告封单资金，人民币元；不等同于收盘盘口推导封单额。';
comment on column today_limit_down.source_observation.created_at is '记录首次写入时间（带时区）。';

-- today_limit_up.calculation_quality
comment on table today_limit_up.calculation_quality is '同日涨停快照计算质量结果。';
comment on column today_limit_up.calculation_quality.snapshot_id is '不可变快照标识。';
comment on column today_limit_up.calculation_quality.rule_code is '质量或计算规则代码。';
comment on column today_limit_up.calculation_quality.severity is '质量问题严重级别。';
comment on column today_limit_up.calculation_quality.symbol is '标准证券代码，格式如 SSE:600000、SZSE:000001、BSE:920000；保留代码前导零。';
comment on column today_limit_up.calculation_quality.message is '可读结果或诊断说明。';
comment on column today_limit_up.calculation_quality.created_at is '记录首次写入时间（带时区）。';

-- today_limit_up.member
comment on table today_limit_up.member is '同日涨停不可变快照成员；保存当日行情、自由流通市值、封板观测及收盘盘口。';
comment on column today_limit_up.member.snapshot_id is '不可变快照标识。';
comment on column today_limit_up.member.symbol is '标准证券代码，格式如 SSE:600000、SZSE:000001、BSE:920000；保留代码前导零。';
comment on column today_limit_up.member.code is '交易所六位证券代码字符串；保留前导零。';
comment on column today_limit_up.member.historical_name is '该交易日适用的历史证券名称。';
comment on column today_limit_up.member.previous_close is '上一交易日收盘价，人民币元；来源事实不补造缺失值。';
comment on column today_limit_up.member.close is '收盘价，人民币元。';
comment on column today_limit_up.member.limit_price is '对应方向的涨停或跌停价格，人民币元。';
comment on column today_limit_up.member.change_percent is '涨跌幅，百分点；1表示1%。';
comment on column today_limit_up.member.free_float_shares is '自由流通股本，股；与流通股本口径不同。';
comment on column today_limit_up.member.free_float_market_cap_cny is '同日未复权收盘价乘同日自由流通股本，人民币元。';
comment on column today_limit_up.member.first_limit_up_at is '来源报告的当日首次封涨停时间（带时区）。';
comment on column today_limit_up.member.last_limit_up_at is '来源报告的当日末次封涨停时间（带时区）。';
comment on column today_limit_up.member.open_count is '来源报告的当日开板次数。';
comment on column today_limit_up.member.limit_up_duration_seconds is '当日累计涨停时长，秒；缺少完整事件流时为空，不由首末封板时间推算。';
comment on column today_limit_up.member.duration_semantics is '封板时长语义；unavailable_without_event_stream表示缺少事件流，不能推算时长。';
comment on column today_limit_up.member.source_reported_sealed_funds_cny is '来源报告封单资金，人民币元；不等同于收盘盘口推导封单额。';
comment on column today_limit_up.member.closing_bid1_price is '收盘买1档报价，人民币元。';
comment on column today_limit_up.member.closing_bid1_volume_shares is '收盘买1档委托数量，股。';
comment on column today_limit_up.member.closing_bid2_price is '收盘买2档报价，人民币元。';
comment on column today_limit_up.member.closing_bid2_volume_shares is '收盘买2档委托数量，股。';
comment on column today_limit_up.member.closing_bid3_price is '收盘买3档报价，人民币元。';
comment on column today_limit_up.member.closing_bid3_volume_shares is '收盘买3档委托数量，股。';
comment on column today_limit_up.member.closing_bid4_price is '收盘买4档报价，人民币元。';
comment on column today_limit_up.member.closing_bid4_volume_shares is '收盘买4档委托数量，股。';
comment on column today_limit_up.member.closing_bid5_price is '收盘买5档报价，人民币元。';
comment on column today_limit_up.member.closing_bid5_volume_shares is '收盘买5档委托数量，股。';
comment on column today_limit_up.member.closing_bid1_sealing_amount_cny is '收盘买一封单金额，人民币元；与来源报告封单资金分开保存。';
comment on column today_limit_up.member.daily_bar_ingestion_id is '当日日行情事实所属采集批次标识。';
comment on column today_limit_up.member.indicator_ingestion_id is '当日每日指标事实所属采集批次标识。';
comment on column today_limit_up.member.name_ingestion_id is '历史证券名称事实所属采集批次标识。';
comment on column today_limit_up.member.pool_calculation_id is '股票池派生事实所属计算批次标识。';
comment on column today_limit_up.member.source_observation_ingestion_id is '涨跌停来源观测所属采集批次标识。';
comment on column today_limit_up.member.source_observation_raw_id is '涨跌停来源观测所属原始对象标识。';
comment on column today_limit_up.member.order_book_ingestion_id is '收盘盘口来源事实所属采集批次标识。';
comment on column today_limit_up.member.created_at is '记录首次写入时间（带时区）。';
comment on column today_limit_up.member.volume is '成交量，股。';
comment on column today_limit_up.member.amount_cny is '成交额，人民币元。';
comment on column today_limit_up.member.free_float_turnover_rate_pct is '自由流通股本换手率，百分点；1表示1%。';
comment on column today_limit_up.member.consecutive_limit_up_days is '来源报告的连续涨停交易日数。';

-- today_limit_up.snapshot
comment on table today_limit_up.snapshot is '同日涨停不可变快照头；记录精确交易日、版本、质量及计算血缘。';
comment on column today_limit_up.snapshot.snapshot_id is '不可变快照标识。';
comment on column today_limit_up.snapshot.calculation_id is '计算批次标识；用于追溯算法版本及冻结输入，同一逻辑结果不混用计算批次。';
comment on column today_limit_up.snapshot.trade_date is '交易所本地交易日期；以统一交易日历为准。';
comment on column today_limit_up.snapshot.version is '同一自然范围内的快照版本号。';
comment on column today_limit_up.snapshot.status is '记录或运行状态；允许值及终态约束见本表 CHECK 约束。';
comment on column today_limit_up.snapshot.member_count is '快照或聚合范围内的成员数量。';
comment on column today_limit_up.snapshot.candidate_count is '计算候选证券数量。';
comment on column today_limit_up.snapshot.rejected_count is '被质量规则拒绝的候选数量。';
comment on column today_limit_up.snapshot.content_hash is '规范化记录或快照内容的校验摘要。';
comment on column today_limit_up.snapshot.input_hash is '规范化计算输入或采集内容的校验摘要。';
comment on column today_limit_up.snapshot.rule_version is '客观计算规则版本。';
comment on column today_limit_up.snapshot.algorithm_version is '计算算法版本；用于重现结果。';
comment on column today_limit_up.snapshot.source_ingestion_id is '本快照采用的来源观测采集批次标识。';
comment on column today_limit_up.snapshot.generated_at is '快照生成时间（带时区）。';

-- today_limit_up.source_observation
comment on table today_limit_up.source_observation is '同日涨停池来源观测；保存首次/末次封板、开板次数及来源报告封单资金。';
comment on column today_limit_up.source_observation.ingestion_id is '采集批次标识，关联 ingestion.ingestion_run，用于追溯来源和 Raw 对象。';
comment on column today_limit_up.source_observation.raw_id is '不可变原始数据对象标识，关联 ingestion.raw_manifest。';
comment on column today_limit_up.source_observation.symbol is '标准证券代码，格式如 SSE:600000、SZSE:000001、BSE:920000；保留代码前导零。';
comment on column today_limit_up.source_observation.trade_date is '交易所本地交易日期；以统一交易日历为准。';
comment on column today_limit_up.source_observation.source_code is '实际数据来源代码；单个成功采集批次不合并多个来源。';
comment on column today_limit_up.source_observation.observed_at is '本次来源事实观测时间（带时区）；不等于计划时间或入库时间。';
comment on column today_limit_up.source_observation.source_name is '来源返回的证券名称。';
comment on column today_limit_up.source_observation.first_limit_up_at is '来源报告的当日首次封涨停时间（带时区）。';
comment on column today_limit_up.source_observation.last_limit_up_at is '来源报告的当日末次封涨停时间（带时区）。';
comment on column today_limit_up.source_observation.open_count is '来源报告的当日开板次数。';
comment on column today_limit_up.source_observation.source_reported_sealed_funds_cny is '来源报告封单资金，人民币元；不等同于收盘盘口推导封单额。';
comment on column today_limit_up.source_observation.created_at is '记录首次写入时间（带时区）。';

-- PostgreSQL 的 COMMENT 不自动继承到已有分区，按父表元数据补齐所有现存分区。
-- 只遍历本项目两张竞价序列表；已有分区注释保留，不触碰其他数据库对象。
do $comments$
declare
    partition_record record;
    column_record record;
begin
    for partition_record in
        select child.oid, child.oid::regclass as relation,
               parent.oid as parent_oid, obj_description(parent.oid, 'pg_class') as description
        from pg_catalog.pg_inherits inheritance
        join pg_catalog.pg_class child on child.oid = inheritance.inhrelid
        join pg_catalog.pg_class parent on parent.oid = inheritance.inhparent
        where parent.oid in (
            'realtime.call_auction_market_series_snapshot'::regclass,
            'realtime.call_auction_market_series_snapshot_history'::regclass
        )
        order by child.oid
    loop
        if obj_description(partition_record.oid, 'pg_class') is null then
            execute format('comment on table %s is %L', partition_record.relation,
                           partition_record.description || '（按交易日划分的分区）');
        end if;
        for column_record in
            select child_column.attname,
                   col_description(partition_record.parent_oid, parent_column.attnum) as description
            from pg_catalog.pg_attribute child_column
            join pg_catalog.pg_attribute parent_column
              on parent_column.attrelid = partition_record.parent_oid
             and parent_column.attname = child_column.attname
             and not parent_column.attisdropped
            where child_column.attrelid = partition_record.oid
              and child_column.attnum > 0 and not child_column.attisdropped
              and col_description(partition_record.oid, child_column.attnum) is null
        loop
            execute format('comment on column %s.%I is %L', partition_record.relation,
                           column_record.attname, column_record.description);
        end loop;
    end loop;
end;
$comments$;
