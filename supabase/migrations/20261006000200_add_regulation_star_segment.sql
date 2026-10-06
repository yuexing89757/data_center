-- Extend regulation monitoring to the SSE STAR Market (688/689).
-- STAR keeps the SSE mainboard rule parameters (same thresholds, windows and
-- counts) but benchmarks against the SSE STAR 50 index (SSE:000688) and uses
-- the +/-20% STAR price-limit board. The seed rules stay on the existing
-- rule_set_version because validate_regulation_rules requires every active
-- rule to share one version; the changed rule-set contents are distinguished
-- by rule_set_hash recorded on each calculation run.

alter table regulation.rule drop constraint regulation_rule_exchange_segment_check;
alter table regulation.rule add constraint regulation_rule_exchange_segment_check check (
    (exchange = 'SSE' and segment in ('SSE_MAIN', 'STAR'))
    or (exchange = 'SZSE' and segment in ('SZSE_MAIN', 'GEM'))
);

alter table regulation.rule drop constraint regulation_rule_kind_parameters_check;
alter table regulation.rule add constraint regulation_rule_kind_parameters_check check (
    (
        kind = 'CUMULATIVE_DEVIATION'
        and direction in ('UP', 'DOWN')
        and window_days > 0 and threshold_pct is not null
        and comparison_window_days is null and ratio_threshold is null
        and secondary_threshold_pct is null and count_window_days is null
        and required_count is null and counted_event_kind is null
        and benchmark_symbol is not null and btrim(benchmark_symbol) <> ''
    )
    or (
        kind = 'TURNOVER_COMPOSITE'
        and direction = 'NONE' and window_days > 0
        and threshold_pct is null and comparison_window_days > 0
        and ratio_threshold > 0 and secondary_threshold_pct > 0
        and count_window_days is null and required_count is null
        and counted_event_kind is null and benchmark_symbol is null
        and segment in ('SSE_MAIN', 'SZSE_MAIN', 'STAR')
    )
    or (
        kind = 'EVENT_COUNT'
        and direction in ('UP', 'DOWN') and window_days is null
        and threshold_pct is null and comparison_window_days is null
        and ratio_threshold is null and secondary_threshold_pct is null
        and count_window_days > 0 and required_count > 0
        and counted_event_kind = 'PRICE_DEVIATION_ABNORMAL'
        and benchmark_symbol is null
    )
);

alter table regulation.event drop constraint regulation_event_identity_check;
alter table regulation.event add constraint regulation_event_identity_check check (
    symbol ~ '^(SSE|SZSE):[0-9]{6}$'
    and ((exchange = 'SSE' and segment in ('SSE_MAIN', 'STAR')
          and source_code = 'sse_official')
      or (exchange = 'SZSE' and segment in ('SZSE_MAIN', 'GEM')
          and source_code = 'szse_official'))
    and btrim(source_event_id) <> '' and btrim(source_title) <> ''
    and btrim(source_url) <> '' and source_content_hash ~ '^[0-9a-f]{64}$'
);

alter table regulation.status drop constraint regulation_status_identity_check;
alter table regulation.status add constraint regulation_status_identity_check check (
    symbol ~ '^(SSE|SZSE):[0-9]{6}$'
    and ((exchange = 'SSE' and segment in ('SSE_MAIN', 'STAR'))
      or (exchange = 'SZSE' and segment in ('SZSE_MAIN', 'GEM')))
);

insert into regulation.rule (
    rule_code, exchange, segment, rule_name, level, kind, direction,
    window_days, threshold_pct, comparison_window_days, ratio_threshold,
    secondary_threshold_pct, count_window_days, required_count, counted_event_kind,
    reset_level, benchmark_symbol, rule_set_version, effective_date, expire_date,
    source_document, source_clause, source_url, enabled
) values
-- official-rule: 27
('STAR_ABNORMAL_3D_DEV_UP', 'SSE', 'STAR', '科创板3日向上价格偏离异常波动', 'ABNORMAL', 'CUMULATIVE_DEVIATION', 'UP', 3, 20, null, null, null, null, null, null, 'ABNORMAL', 'SSE:000688', 'cn-a-share-regulation-2026-07-06.v1', date '2026-07-06', null, '上海证券交易所交易规则（2026年修订）', '5.4.2(1)', 'https://www.sse.com.cn/lawandrules/sselawsrules2025/trade/universal/c/c_20260424_10816492.shtml', true),
-- official-rule: 28
('STAR_ABNORMAL_3D_DEV_DOWN', 'SSE', 'STAR', '科创板3日向下价格偏离异常波动', 'ABNORMAL', 'CUMULATIVE_DEVIATION', 'DOWN', 3, -20, null, null, null, null, null, null, 'ABNORMAL', 'SSE:000688', 'cn-a-share-regulation-2026-07-06.v1', date '2026-07-06', null, '上海证券交易所交易规则（2026年修订）', '5.4.2(1)', 'https://www.sse.com.cn/lawandrules/sselawsrules2025/trade/universal/c/c_20260424_10816492.shtml', true),
-- official-rule: 29
('STAR_ABNORMAL_TURNOVER', 'SSE', 'STAR', '科创板3日换手率复合异常波动', 'ABNORMAL', 'TURNOVER_COMPOSITE', 'NONE', 3, null, 5, 30, 20, null, null, null, 'ABNORMAL', null, 'cn-a-share-regulation-2026-07-06.v1', date '2026-07-06', null, '上海证券交易所交易规则（2026年修订）', '5.4.2(2)', 'https://www.sse.com.cn/lawandrules/sselawsrules2025/trade/universal/c/c_20260424_10816492.shtml', true),
-- official-rule: 30
('STAR_SERIOUS_10D_COUNT_UP', 'SSE', 'STAR', '科创板10日向上多次异常严重异常波动', 'SERIOUS_ABNORMAL', 'EVENT_COUNT', 'UP', null, null, null, null, null, 10, 4, 'PRICE_DEVIATION_ABNORMAL', 'SERIOUS_ABNORMAL', null, 'cn-a-share-regulation-2026-07-06.v1', date '2026-07-06', null, '上海证券交易所交易规则（2026年修订）', '5.4.3(1)', 'https://www.sse.com.cn/lawandrules/sselawsrules2025/trade/universal/c/c_20260424_10816492.shtml', true),
-- official-rule: 31
('STAR_SERIOUS_10D_COUNT_DOWN', 'SSE', 'STAR', '科创板10日向下多次异常严重异常波动', 'SERIOUS_ABNORMAL', 'EVENT_COUNT', 'DOWN', null, null, null, null, null, 10, 4, 'PRICE_DEVIATION_ABNORMAL', 'SERIOUS_ABNORMAL', null, 'cn-a-share-regulation-2026-07-06.v1', date '2026-07-06', null, '上海证券交易所交易规则（2026年修订）', '5.4.3(1)', 'https://www.sse.com.cn/lawandrules/sselawsrules2025/trade/universal/c/c_20260424_10816492.shtml', true),
-- official-rule: 32
('STAR_SERIOUS_10D_DEV_UP', 'SSE', 'STAR', '科创板10日向上价格偏离严重异常波动', 'SERIOUS_ABNORMAL', 'CUMULATIVE_DEVIATION', 'UP', 10, 100, null, null, null, null, null, null, 'SERIOUS_ABNORMAL', 'SSE:000688', 'cn-a-share-regulation-2026-07-06.v1', date '2026-07-06', null, '上海证券交易所交易规则（2026年修订）', '5.4.3(2)', 'https://www.sse.com.cn/lawandrules/sselawsrules2025/trade/universal/c/c_20260424_10816492.shtml', true),
-- official-rule: 33
('STAR_SERIOUS_10D_DEV_DOWN', 'SSE', 'STAR', '科创板10日向下价格偏离严重异常波动', 'SERIOUS_ABNORMAL', 'CUMULATIVE_DEVIATION', 'DOWN', 10, -50, null, null, null, null, null, null, 'SERIOUS_ABNORMAL', 'SSE:000688', 'cn-a-share-regulation-2026-07-06.v1', date '2026-07-06', null, '上海证券交易所交易规则（2026年修订）', '5.4.3(2)', 'https://www.sse.com.cn/lawandrules/sselawsrules2025/trade/universal/c/c_20260424_10816492.shtml', true),
-- official-rule: 34
('STAR_SERIOUS_30D_DEV_UP', 'SSE', 'STAR', '科创板30日向上价格偏离严重异常波动', 'SERIOUS_ABNORMAL', 'CUMULATIVE_DEVIATION', 'UP', 30, 200, null, null, null, null, null, null, 'SERIOUS_ABNORMAL', 'SSE:000688', 'cn-a-share-regulation-2026-07-06.v1', date '2026-07-06', null, '上海证券交易所交易规则（2026年修订）', '5.4.3(3)', 'https://www.sse.com.cn/lawandrules/sselawsrules2025/trade/universal/c/c_20260424_10816492.shtml', true),
-- official-rule: 35
('STAR_SERIOUS_30D_DEV_DOWN', 'SSE', 'STAR', '科创板30日向下价格偏离严重异常波动', 'SERIOUS_ABNORMAL', 'CUMULATIVE_DEVIATION', 'DOWN', 30, -70, null, null, null, null, null, null, 'SERIOUS_ABNORMAL', 'SSE:000688', 'cn-a-share-regulation-2026-07-06.v1', date '2026-07-06', null, '上海证券交易所交易规则（2026年修订）', '5.4.3(3)', 'https://www.sse.com.cn/lawandrules/sselawsrules2025/trade/universal/c/c_20260424_10816492.shtml', true);
