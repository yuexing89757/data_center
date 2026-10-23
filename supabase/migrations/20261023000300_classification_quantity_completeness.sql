-- ADR-0068 / Issue #92: completeness is independent for volume and amount.
-- Legacy coverage is unknown: never infer it from old totals or price coverage.
alter table metrics.classification_daily_metric
    alter column total_volume drop not null,
    alter column total_amount drop not null,
    add column volume_member_count integer,
    add column amount_member_count integer,
    add constraint classification_metric_quantity_coverage_check check (
        (volume_member_count is null or volume_member_count between 0 and member_count)
        and (amount_member_count is null or amount_member_count between 0 and member_count)
        and (volume_member_count is null or
            (total_volume is not null) = (volume_member_count = member_count))
        and (amount_member_count is null or
            (total_amount is not null) = (amount_member_count = member_count))
    );

-- Existing columns retain their order; new metadata is appended.
create or replace view api_v1.classification_daily_metrics as
select metric.namespace, metric.classification_type, metric.classification_code,
       metric.membership_snapshot_date, metric.trade_date, metric.member_count,
       metric.priced_member_count, metric.advancing_count, metric.declining_count,
       metric.unchanged_count,
       case when run.algorithm_version = '1.2.0'
                 and metric.volume_member_count = metric.member_count
            then metric.total_volume end as total_volume,
       (case when run.algorithm_version = '1.2.0'
                 and metric.amount_member_count = metric.member_count
            then metric.total_amount end)::numeric(38, 4) as total_amount,
       metric.equal_weight_return,
       (case when run.algorithm_version='1.2.0' then metric.total_market_cap end)::numeric(38,4) as total_market_cap,
       metric.market_cap_member_count, run.calculation_id,
       run.algorithm_version, run.start_date as calculation_start_date,
       run.end_date as calculation_end_date, run.input_hash, run.calculated_at,
       metric.volume_member_count, metric.amount_member_count,
       jsonb_build_object(
           'unit_contract_version', 'stock-quantity.v1',
           'status', case
               when run.algorithm_version <> '1.2.0'
                    or metric.volume_member_count is null or metric.amount_member_count is null
                   then 'unverified'
               when metric.volume_member_count = metric.member_count
                    and metric.amount_member_count = metric.member_count
                    and metric.market_cap_member_count = metric.member_count then 'certified'
               else 'incomplete' end,
           'reason', case
               when run.algorithm_version <> '1.2.0'
                    or metric.volume_member_count is null or metric.amount_member_count is null
                   then 'legacy_algorithm_unverified'
               when metric.volume_member_count = metric.member_count
                    and metric.amount_member_count = metric.member_count
                    and metric.market_cap_member_count = metric.member_count
                   then 'complete_member_coverage'
               else 'incomplete_member_coverage' end,
           'field_groups', jsonb_build_object(
               'classification_volume', jsonb_build_object(
                   'status', case
                       when run.algorithm_version <> '1.2.0'
                            or metric.volume_member_count is null then 'unverified'
                       when metric.volume_member_count = metric.member_count then 'certified'
                       else 'incomplete' end,
                   'reason',case
                       when run.algorithm_version <> '1.2.0' or metric.volume_member_count is null then 'legacy_algorithm_unverified'
                       when metric.volume_member_count=metric.member_count then 'complete_member_coverage'
                       else 'incomplete_member_coverage' end),
               'classification_amount', jsonb_build_object(
                   'status', case
                       when run.algorithm_version <> '1.2.0'
                            or metric.amount_member_count is null then 'unverified'
                       when metric.amount_member_count = metric.member_count then 'certified'
                       else 'incomplete' end,
                   'reason',case
                       when run.algorithm_version <> '1.2.0' or metric.amount_member_count is null then 'legacy_algorithm_unverified'
                       when metric.amount_member_count=metric.member_count then 'complete_member_coverage'
                       else 'incomplete_member_coverage' end),
               'classification_market_cap', jsonb_build_object(
                   'status', case
                       when run.algorithm_version <> '1.2.0' then 'unverified'
                       when metric.market_cap_member_count = metric.member_count then 'certified'
                       else 'incomplete' end,
                   'reason',case when run.algorithm_version <> '1.2.0' then 'legacy_algorithm_unverified'
                       when metric.market_cap_member_count=metric.member_count then 'complete_member_coverage'
                       else 'incomplete_member_coverage' end)
           )
       ) as quantity_quality
from metrics.classification_daily_metric metric
join derived.calculation_run run using (calculation_id)
where run.status = 'succeeded';

comment on column metrics.classification_daily_metric.total_volume is
    '完整 active.members 成交股数合计；任一成员缺日K或volume为NULL则为NULL；显式0参与合计，不按停牌或生命周期自动剔除成员。';
comment on column metrics.classification_daily_metric.total_amount is
    '完整 active.members 成交额合计，人民币元；任一成员缺日K或amount为NULL则为NULL；与volume独立判断完整性。';
comment on column metrics.classification_daily_metric.volume_member_count is
    'active.members 中当日具有非NULL标准成交股数的成员数；显式0计入，旧算法覆盖未知为NULL。';
comment on column metrics.classification_daily_metric.amount_member_count is
    'active.members 中当日具有非NULL标准成交额的成员数；显式0计入，旧算法覆盖未知为NULL。';
comment on column api_v1.classification_daily_metrics.total_volume is
    '完整成员的标准成交股数合计；100表示100股。缺成员行情、缺量、未认证输入或旧算法结果为NULL，见volume_member_count和quantity_quality。';
comment on column api_v1.classification_daily_metrics.total_amount is
    '完整成员的标准成交额合计，人民币元；缺成员行情、缺额、未认证输入或旧算法结果为NULL，见amount_member_count和quantity_quality。';
comment on column api_v1.classification_daily_metrics.volume_member_count is
    '当日具有非NULL已认证成交股数的成员数，0股也计入；旧算法未知为NULL。';
comment on column api_v1.classification_daily_metrics.amount_member_count is
    '当日具有非NULL已认证成交额的成员数，0元也计入；旧算法未知为NULL。';
comment on column api_v1.classification_daily_metrics.quantity_quality is
    'stock-quantity.v1数量单位与完整性状态；新算法量/额/市值均覆盖完整为certified，任一不完整为incomplete，旧算法为unverified；不改变成员、价格及收益率分母。';

-- CREATE OR REPLACE retains the existing view grants and security properties.
notify pgrst, 'reload schema';
