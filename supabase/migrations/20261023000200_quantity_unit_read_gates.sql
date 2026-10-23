-- ADR-0068 / Issue #92. These gates never rewrite source facts or Raw objects.
create function api_v1.quantity_unit_contract_version()
returns text language sql immutable security invoker
set search_path = pg_catalog set statement_timeout = '1s'
as $$ select 'stock-quantity.v1'::text $$;
revoke all on function api_v1.quantity_unit_contract_version() from public,anon;
grant execute on function api_v1.quantity_unit_contract_version() to market_data_api,authenticated;

create function audit.quantity_unit_is_certified(p_ingestion_id uuid, p_field_group text)
returns boolean language sql stable security definer
set search_path = pg_catalog, pg_temp set statement_timeout = '5s'
as $$
    select exists (
        select 1
        from audit.quantity_unit_certification certificate
        join ingestion.ingestion_run run using (ingestion_id)
        cross join lateral (
            select coalesce(jsonb_agg(distinct manifest.content_sha256), '[]'::jsonb) hashes
            from ingestion.raw_manifest manifest
            where manifest.ingestion_id = run.ingestion_id
               or manifest.raw_id = run.replayed_from_raw_id
        ) current_raw
        where certificate.ingestion_id = p_ingestion_id
          and certificate.unit_contract_version = 'stock-quantity.v1'
          and certificate.field_group = p_field_group
          and certificate.status = 'certified'
          and run.status in ('succeeded', 'partial')
          and jsonb_array_length(current_raw.hashes) > 0
          and certificate.raw_hashes @> current_raw.hashes
          and current_raw.hashes @> certificate.raw_hashes
          and not exists (
              select 1 from jsonb_array_elements_text(certificate.raw_hashes) h(value)
              where h.value !~ '^[0-9a-f]{64}$'
          )
    );
$$;

create function audit.quantity_quality(p_ingestion_id uuid, p_field_group text)
returns jsonb language sql stable security definer
set search_path = pg_catalog, pg_temp set statement_timeout = '5s'
as $$
    select jsonb_build_object(
        'unit_contract_version', 'stock-quantity.v1',
        'status', case when certified then 'certified' else 'unverified' end,
        'reason', case when certified then 'matching_raw_evidence' else 'quantity_units_unverified' end,
        'field_groups', jsonb_build_object(p_field_group, jsonb_build_object(
            'status', case when certified then 'certified' else 'unverified' end,
            'reason', case when certified then 'matching_raw_evidence' else 'quantity_units_unverified' end
        ))
    ) from (select audit.quantity_unit_is_certified(p_ingestion_id,p_field_group) certified) fact;
$$;

-- Source projections have exactly the original column types and order, followed
-- by quality. Only the explicit table/field whitelist below is transformed.
do $gates$
declare
    route record;
    projection text;
begin
    for route in select * from (values
        ('core.daily_bar','unit_safe_daily_bar','daily_bar.volume',
         array['volume','amount']),
        ('capital.share_capital','unit_safe_share_capital','capital.shares',
         array['total_shares','restricted_shares','circulating_shares','listed_a_shares']),
        ('capital.rights_issue','unit_safe_rights_issue','capital.rights_issue.quantities',array['base_shares','proceeds']),
        ('core.stock_daily_indicator','unit_safe_stock_daily_indicator','stock_daily_indicator.shares',
         array['total_shares','circulating_shares','free_float_shares','total_market_value','circulating_market_value']),
        ('core.top10_float_holder','unit_safe_top10_float_holder','top10_float_holder.hold_amount',
         array['hold_amount','hold_change']),
        ('core.board_index_daily_bar','unit_safe_board_index_daily_bar','board_index.quantities',
         array['volume','amount']),
        ('convertible_bond.bond','unit_safe_convertible_bond','convertible_bond.bond.quantities',array['issue_size']),
        ('convertible_bond.daily_bar','unit_safe_convertible_bond_daily_bar','convertible_bond_daily_bar.volume',
         array['volume','amount','remain_size']),
        ('realtime.stock_quote_snapshot','unit_safe_stock_quote_snapshot','stock_quote_snapshot.quantities',
         array['cumulative_volume','cumulative_amount','bid1_volume','bid2_volume','bid3_volume','bid4_volume','bid5_volume',
               'ask1_volume','ask2_volume','ask3_volume','ask4_volume','ask5_volume']),
        ('realtime.five_level_quote_snapshot','unit_safe_five_level_quote_snapshot','five_level_quote.quantities',
         array['cumulative_volume','cumulative_amount','bid1_volume','bid2_volume','bid3_volume','bid4_volume','bid5_volume',
               'ask1_volume','ask2_volume','ask3_volume','ask4_volume','ask5_volume']),
        ('realtime.call_auction_indicative_detail','unit_safe_call_auction_indicative_detail',
         'call_auction_indicative_detail.displayed_volume_shares',array['displayed_volume_shares']),
        ('realtime.call_auction_market_snapshot','unit_safe_call_auction_market_snapshot','call_auction_market_snapshot.quantities',
         array['cumulative_volume','cumulative_amount','bid1_volume','bid2_volume','bid3_volume','bid4_volume','bid5_volume',
               'ask1_volume','ask2_volume','ask3_volume','ask4_volume','ask5_volume']),
        ('realtime.call_auction_market_series_snapshot','unit_safe_call_auction_market_series_snapshot','call_auction_market_series.quantities',
         array['cumulative_volume','cumulative_amount','bid1_volume','bid2_volume','bid3_volume','bid4_volume','bid5_volume',
               'ask1_volume','ask2_volume','ask3_volume','ask4_volume','ask5_volume']),
        ('realtime.call_auction_market_series_snapshot_history','unit_safe_call_auction_market_series_snapshot_history',
         'call_auction_market_series.quantities',
         array['cumulative_volume','cumulative_amount','bid1_volume','bid2_volume','bid3_volume','bid4_volume','bid5_volume',
               'ask1_volume','ask2_volume','ask3_volume','ask4_volume','ask5_volume']),
        ('realtime.eod_quote_snapshot','unit_safe_eod_quote_snapshot','eod_quote_snapshot.quantities',
         array['bid1_volume','bid2_volume','bid3_volume','bid4_volume','bid5_volume',
               'ask1_volume','ask2_volume','ask3_volume','ask4_volume','ask5_volume','seal_amount'])
    ) routes(source_relation,safe_relation,quantity_group,quantity_fields)
    loop
        select string_agg(
            case when attribute.attname = any(route.quantity_fields) then
                format('(case when audit.quantity_unit_is_certified(source.ingestion_id,%L) then source.%I end)::%s as %I',
                    case when route.source_relation='core.daily_bar' and attribute.attname='amount'
                         then 'daily_bar.amount'
                         when route.source_relation='convertible_bond.daily_bar'
                         then 'convertible_bond_daily_bar.' || attribute.attname
                         when route.source_relation='capital.rights_issue' and attribute.attname='proceeds'
                         then 'capital.rights_issue.proceeds'
                         else route.quantity_group end,
                    attribute.attname, format_type(attribute.atttypid,attribute.atttypmod), attribute.attname)
            else format('source.%I',attribute.attname) end, ', ' order by attribute.attnum)
        into projection
        from pg_attribute attribute
        where attribute.attrelid=route.source_relation::regclass
          and attribute.attnum>0 and not attribute.attisdropped;
        if route.source_relation='core.daily_bar' then
            projection := projection || ', audit.quantity_quality(source.ingestion_id,''daily_bar.volume'') || '
                || 'jsonb_build_object(''status'',case when audit.quantity_unit_is_certified(source.ingestion_id,''daily_bar.volume'') '
                || 'and audit.quantity_unit_is_certified(source.ingestion_id,''daily_bar.amount'') then ''certified'' else ''unverified'' end,'
                || '''reason'',case when audit.quantity_unit_is_certified(source.ingestion_id,''daily_bar.volume'') '
                || 'and audit.quantity_unit_is_certified(source.ingestion_id,''daily_bar.amount'') then ''matching_raw_evidence'' else ''quantity_units_unverified'' end,'
                || '''field_groups'',(audit.quantity_quality(source.ingestion_id,''daily_bar.volume'')->''field_groups'') || '
                || '(audit.quantity_quality(source.ingestion_id,''daily_bar.amount'')->''field_groups'')) as quantity_quality';
        elsif route.source_relation='capital.rights_issue' then
            projection := projection || ', audit.quantity_quality(source.ingestion_id,''capital.rights_issue.quantities'') || '
                || 'jsonb_build_object(''status'',case when audit.quantity_unit_is_certified(source.ingestion_id,''capital.rights_issue.quantities'') '
                || 'and audit.quantity_unit_is_certified(source.ingestion_id,''capital.rights_issue.proceeds'') then ''certified'' else ''unverified'' end,'
                || '''reason'',''independent_rights_quantity_admission'',''field_groups'', '
                || '(audit.quantity_quality(source.ingestion_id,''capital.rights_issue.quantities'')->''field_groups'') || '
                || '(audit.quantity_quality(source.ingestion_id,''capital.rights_issue.proceeds'')->''field_groups'')) as quantity_quality';
        elsif route.source_relation='convertible_bond.daily_bar' then
            projection := projection || ', audit.quantity_quality(source.ingestion_id,''convertible_bond_daily_bar.volume'') || '
                || 'jsonb_build_object(''status'',case when audit.quantity_unit_is_certified(source.ingestion_id,''convertible_bond_daily_bar.volume'') '
                || 'and audit.quantity_unit_is_certified(source.ingestion_id,''convertible_bond_daily_bar.amount'') '
                || 'and audit.quantity_unit_is_certified(source.ingestion_id,''convertible_bond_daily_bar.remain_size'') then ''certified'' else ''unverified'' end,'
                || '''reason'',''independent_bond_quantity_admission'',''field_groups'', '
                || '(audit.quantity_quality(source.ingestion_id,''convertible_bond_daily_bar.volume'')->''field_groups'') || '
                || '(audit.quantity_quality(source.ingestion_id,''convertible_bond_daily_bar.amount'')->''field_groups'') || '
                || '(audit.quantity_quality(source.ingestion_id,''convertible_bond_daily_bar.remain_size'')->''field_groups'')) as quantity_quality';
        else
            projection := projection || format(', audit.quantity_quality(source.ingestion_id,%L) as quantity_quality',route.quantity_group);
        end if;
        execute format('create view audit.%I as select %s from %s source',route.safe_relation,projection,route.source_relation);
        execute format('revoke all on audit.%I from public,anon,authenticated,market_data_api',route.safe_relation);
        execute format('grant select on audit.%I to market_data_worker',route.safe_relation);
    end loop;
end;
$gates$;

create view audit.unit_safe_auction_quote_metric as
select metric.session_id,metric.symbol,metric.sample_seq,metric.spread,metric.mid_price,
       case when audit.quantity_unit_is_certified(quote.ingestion_id,'five_level_quote.quantities') then metric.bid_depth_5 end as bid_depth_5,
       case when audit.quantity_unit_is_certified(quote.ingestion_id,'five_level_quote.quantities') then metric.ask_depth_5 end as ask_depth_5,
       case when audit.quantity_unit_is_certified(quote.ingestion_id,'five_level_quote.quantities') then metric.imbalance_5 end as imbalance_5,
       case when audit.quantity_unit_is_certified(quote.ingestion_id,'five_level_quote.quantities') then metric.seal_amount end as seal_amount,
       metric.calculated_at,metric.algorithm_version,metric.price_limit_rule_version
from derived.auction_quote_metric metric
join realtime.five_level_quote_snapshot quote using(session_id,symbol,sample_seq);

create view audit.unit_safe_stock_technical_factor_snapshot as
select symbol,trade_date,market,source_code,'stock-technical-factor.v2'::text as factor_catalog_version,
       case when audit.quantity_unit_is_certified(ingestion_id,'stock_technical_factor.quantities') then unadjusted
            else unadjusted || '{"obv":null,"emv":null,"maemv":null}'::jsonb end as unadjusted,
       case when audit.quantity_unit_is_certified(ingestion_id,'stock_technical_factor.quantities') then forward_adjusted
            else forward_adjusted || '{"obv":null,"emv":null,"maemv":null}'::jsonb end as forward_adjusted,
       case when audit.quantity_unit_is_certified(ingestion_id,'stock_technical_factor.quantities') then backward_adjusted
            else backward_adjusted || '{"obv":null,"emv":null,"maemv":null}'::jsonb end as backward_adjusted,
       common,ingestion_id,created_at,updated_at,
       factor_catalog_version as source_factor_catalog_version,
       audit.quantity_quality(ingestion_id,'stock_technical_factor.quantities') as quantity_quality
from core.stock_technical_factor_snapshot;

-- Quantity-independent prices remain public while provenance stays private.
create or replace view api_v1.daily_bars as
select symbol,trade_date,open,high,low,close,previous_close,volume,amount,trade_status,is_st,quantity_quality
from audit.unit_safe_daily_bar;
create or replace view api_v1.share_capital as
select symbol,effective_date,total_shares,restricted_shares,circulating_shares,
       listed_a_shares,change_reason,quantity_quality from audit.unit_safe_share_capital;
create or replace view api_v1.rights_issues as
select symbol,record_date,announcement_date,ex_date,payment_start_date,payment_end_date,
       listing_date,rights_ratio,rights_price,base_shares,proceeds,quantity_quality
from audit.unit_safe_rights_issue;
create or replace view api_v1.board_index_daily_bars as
select board_id,trade_date,market,open,high,low,close,volume,amount,quantity_quality
from audit.unit_safe_board_index_daily_bar;
create or replace view api_v1.convertible_bonds as
select symbol,bond_code,bond_short_name,bond_full_name,underlying_symbol,exchange,
       par_value,issue_size,issue_date,value_date,maturity_years,maturity_date,
       convert_price_initial,convert_price,convert_start_date,convert_end_date,
       coupon_rate,redeem_clause,sell_back_clause,lifecycle_status,quantity_quality
from audit.unit_safe_convertible_bond;
create or replace view api_v1.convertible_bond_daily_bars as
select symbol,trade_date,open,high,low,close,previous_close,volume,amount,
       pct_chg,convert_value,convert_premium_pct,convert_price,remain_size,trade_status,quantity_quality
from audit.unit_safe_convertible_bond_daily_bar;

create or replace view api_v1.market_capitalizations as
select metric.symbol,metric.trade_date,
       (case when run.algorithm_version='1.2.0' then metric.total_market_cap end)::numeric(38,4) as total_market_cap,
       (case when run.algorithm_version='1.2.0' then metric.circulating_market_cap end)::numeric(38,4) as circulating_market_cap,
       run.calculation_id,run.algorithm_version,run.start_date as calculation_start_date,
       run.end_date as calculation_end_date,run.input_hash,run.calculated_at,
       jsonb_build_object('unit_contract_version','stock-quantity.v1',
           'status',case when run.algorithm_version='1.2.0' then 'certified' else 'unverified' end,
           'reason',case when run.algorithm_version='1.2.0' then 'certified_calculation_inputs' else 'legacy_algorithm_unverified' end,
           'field_groups',jsonb_build_object('capital.shares',jsonb_build_object(
               'status',case when run.algorithm_version='1.2.0' then 'certified' else 'unverified' end))) as quantity_quality
from derived.market_capitalization metric join derived.calculation_run run using(calculation_id)
where run.status='succeeded';

-- Retain this view's existing projection (including source fields), then append quality.
do $indicator_view$
declare definition text;
begin
    select pg_get_viewdef('api_v1.stock_daily_indicators'::regclass,true) into definition;
    definition := replace(definition,'core.stock_daily_indicator','audit.unit_safe_stock_daily_indicator stock_daily_indicator');
    definition := regexp_replace(definition,'\s+FROM ', ', stock_daily_indicator.quantity_quality FROM ', 'i');
    execute 'create or replace view api_v1.stock_daily_indicators as ' || definition;
end;
$indicator_view$;

-- Add metadata to quantity-bearing JSON objects without changing prices, keys,
-- ordering, pagination, or the already-masked numeric values.
create function audit.quantity_payload(p_payload jsonb,p_quality jsonb)
returns jsonb language plpgsql immutable
set search_path = pg_catalog, pg_temp
as $$
declare result jsonb; item record;
begin
    if jsonb_typeof(p_payload)='array' then
        select coalesce(jsonb_agg(audit.quantity_payload(value,p_quality) order by ordinality),'[]'::jsonb)
        into result from jsonb_array_elements(p_payload) with ordinality;
        return result;
    elsif jsonb_typeof(p_payload)<>'object' then
        return p_payload;
    end if;
    result := p_payload;
    for item in select key,value from jsonb_each(p_payload) where key <> 'quantity_quality' loop
        if jsonb_typeof(item.value) in ('object','array') then
            result := jsonb_set(result,array[item.key],audit.quantity_payload(item.value,p_quality));
        end if;
    end loop;
    if p_payload ?| array['volume','amount','cumulative_volume','cumulative_amount','displayed_volume_shares',
        'volume_shares','cumulative_volume_shares','cumulative_amount_cny','total_shares','free_float_shares',
        'bid_levels','ask_levels','bid1_volume','ask1_volume','closing_bid1_volume_shares','closing_ask1_volume_shares']
        and not p_payload ? 'quantity_quality' then
        result := result || jsonb_build_object('quantity_quality',p_quality);
    end if;
    return result;
end;
$$;

-- A snapshot is a versioned whole. Certification of a later Core revision never
-- certifies copied quantities with old ingestion pointers.
create index quantity_repair_original_changed_volume_idx
on audit.quantity_unit_repair_item(original_ingestion_id)
where table_name='core.daily_bar' and field_name='volume' and after_state is not null
  and before_state->'volume' is distinct from after_state->'volume';

create function audit.snapshot_quantity_is_certified(p_domain text,p_snapshot_id uuid)
returns boolean language plpgsql stable security definer
set search_path = pg_catalog, pg_temp set statement_timeout = '5s'
as $$
begin
    if p_domain='today_limit_up' then
        return exists(select 1 from today_limit_up.snapshot where snapshot_id=p_snapshot_id) and not exists (
            select 1 from today_limit_up.member member where member.snapshot_id=p_snapshot_id and (
                exists (
                    select 1 from audit.quantity_unit_repair_item repair
                    where repair.original_ingestion_id=member.daily_bar_ingestion_id
                      and repair.table_name='core.daily_bar' and repair.field_name='volume'
                      and repair.after_state is not null
                      and repair.before_state->'volume' is distinct from repair.after_state->'volume'
                )
                or not audit.quantity_unit_is_certified(member.daily_bar_ingestion_id,'daily_bar.volume')
                or not audit.quantity_unit_is_certified(member.daily_bar_ingestion_id,'daily_bar.amount')
                or not audit.quantity_unit_is_certified(member.indicator_ingestion_id,'stock_daily_indicator.shares')
                or ((member.order_book_ingestion_id is not null or member.closing_bid1_volume_shares is not null
                     or member.closing_bid2_volume_shares is not null or member.closing_bid3_volume_shares is not null
                     or member.closing_bid4_volume_shares is not null or member.closing_bid5_volume_shares is not null
                     or member.closing_bid1_sealing_amount_cny is not null)
                    and not audit.quantity_unit_is_certified(member.order_book_ingestion_id,'eod_quote_snapshot.quantities'))
                or (member.source_reported_sealed_funds_cny is not null and not audit.quantity_unit_is_certified(member.source_observation_ingestion_id,'today_limit_up.source_observation.quantities'))
            )
        );
    elsif p_domain='today_limit_down' then
        return exists(select 1 from today_limit_down.snapshot where snapshot_id=p_snapshot_id) and not exists (
            select 1 from today_limit_down.member member where member.snapshot_id=p_snapshot_id and (
                not audit.quantity_unit_is_certified(member.indicator_ingestion_id,'stock_daily_indicator.shares')
                or ((member.order_book_ingestion_id is not null or member.closing_ask1_volume_shares is not null
                     or member.closing_ask2_volume_shares is not null or member.closing_ask3_volume_shares is not null
                     or member.closing_ask4_volume_shares is not null or member.closing_ask5_volume_shares is not null
                     or member.closing_ask1_sealing_amount_cny is not null)
                    and not audit.quantity_unit_is_certified(member.order_book_ingestion_id,'eod_quote_snapshot.quantities'))
                or (member.source_reported_sealed_funds_cny is not null and not audit.quantity_unit_is_certified(member.source_observation_ingestion_id,'today_limit_down.source_observation.quantities'))
            )
        );
    end if;
    return false;
end;
$$;

create view audit.unit_safe_today_limit_up_snapshot as
select (jsonb_populate_record(null::today_limit_up.snapshot,to_jsonb(source) ||
           case when admitted then '{}'::jsonb else '{"status":"deferred","member_count":0}'::jsonb end)).*,
       jsonb_build_object('unit_contract_version','stock-quantity.v1',
           'status',case when admitted then 'certified' else 'unverified' end,
           'reason',case when admitted then 'certified_snapshot_inputs' else 'quantity_units_unverified' end,
           'field_groups',jsonb_build_object('today_limit_up.quantities',jsonb_build_object(
               'status',case when admitted then 'certified' else 'unverified' end,
               'reason',case when admitted then 'certified_snapshot_inputs' else 'quantity_units_unverified' end))) as quantity_quality
from today_limit_up.snapshot source
cross join lateral (select audit.snapshot_quantity_is_certified('today_limit_up',source.snapshot_id) admitted) gate;
create view audit.unit_safe_today_limit_down_snapshot as
select (jsonb_populate_record(null::today_limit_down.snapshot,to_jsonb(source) ||
           case when admitted then '{}'::jsonb else '{"status":"deferred","member_count":0}'::jsonb end)).*,
       jsonb_build_object('unit_contract_version','stock-quantity.v1',
           'status',case when admitted then 'certified' else 'unverified' end,
           'reason',case when admitted then 'certified_snapshot_inputs' else 'quantity_units_unverified' end,
           'field_groups',jsonb_build_object('today_limit_down.quantities',jsonb_build_object(
               'status',case when admitted then 'certified' else 'unverified' end,
               'reason',case when admitted then 'certified_snapshot_inputs' else 'quantity_units_unverified' end))) as quantity_quality
from today_limit_down.snapshot source
cross join lateral (select audit.snapshot_quantity_is_certified('today_limit_down',source.snapshot_id) admitted) gate;

-- Use stored definitions to preserve the latest bounds, ordering, ACLs,
-- SECURITY mode, search_path and timeout. Only this read-only RPC whitelist is
-- rewritten; write bridges and routines outside the list are never touched.
do $rpc_gates$
declare
    routine record;
    route record;
    definition text;
    original text;
    domain_name text;
    quality_expression text;
    routine_signature text;
begin
    for routine in
        select p.oid,p.proname,p.prorettype,p.proowner,obj_description(p.oid,'pg_proc') as description
        from pg_proc p join pg_namespace n on n.oid=p.pronamespace
        where n.nspname='api_v1' and p.proname=any(array[
            'query_daily_bars','query_recent_daily_bars','query_latest_stock_daily_indicators',
            'query_auction_quotes','query_call_auction_indicative_details',
            'query_call_auction_market_snapshots','query_call_auction_market_series_snapshots',
            'query_auction_one_price_limits','query_call_auction_one_price_patterns','query_call_auction_grab_lines',
            'query_stock_pool_snapshot','query_limit_up_pool','query_daily_limit_up_list','query_daily_limit_down_list',
            'query_stock_technical_factors','query_stock_technical_factors_by_codes','query_market_snapshot',
            'query_board_index_bias_latest'
        ])
    loop
        original := pg_get_functiondef(routine.oid);
        routine_signature := routine.oid::regprocedure::text;
        definition := original;
        for route in select * from (values
            ('realtime.call_auction_market_series_snapshot_history','audit.unit_safe_call_auction_market_series_snapshot_history'),
            ('realtime.call_auction_market_series_snapshot','audit.unit_safe_call_auction_market_series_snapshot'),
            ('realtime.call_auction_market_snapshot','audit.unit_safe_call_auction_market_snapshot'),
            ('realtime.call_auction_indicative_detail','audit.unit_safe_call_auction_indicative_detail'),
            ('realtime.five_level_quote_snapshot','audit.unit_safe_five_level_quote_snapshot'),
            ('realtime.stock_quote_snapshot','audit.unit_safe_stock_quote_snapshot'),
            ('realtime.eod_quote_snapshot','audit.unit_safe_eod_quote_snapshot'),
            ('derived.auction_quote_metric','audit.unit_safe_auction_quote_metric'),
            ('core.stock_technical_factor_snapshot','audit.unit_safe_stock_technical_factor_snapshot'),
            ('core.stock_daily_indicator','audit.unit_safe_stock_daily_indicator'),
            ('core.board_index_daily_bar','audit.unit_safe_board_index_daily_bar'),
            ('core.daily_bar','audit.unit_safe_daily_bar'),
            ('capital.share_capital','audit.unit_safe_share_capital'),
            ('today_limit_up.snapshot','audit.unit_safe_today_limit_up_snapshot'),
            ('today_limit_down.snapshot','audit.unit_safe_today_limit_down_snapshot')
        ) replacements(source_name,safe_name) loop
            -- A longer history name must not be transformed a second time.
            definition := regexp_replace(definition,
                '\m' || replace(route.source_name,'.','\.') || '\M',route.safe_name,'g');
        end loop;

        if routine.proname='query_recent_daily_bars' then
            definition := replace(definition,'bar.is_st','bar.is_st, bar.quantity_quality');
        elsif routine.proname='query_daily_bars' then
            definition := replace(definition,'is_st boolean)', 'is_st boolean, quantity_quality jsonb)');
            definition := replace(definition,'bar.is_st','bar.is_st, bar.quantity_quality');
            -- PostgreSQL requires DROP when adding an OUT column. Restore the
            -- exact pre-existing function ACL below after recreating it.
        elsif routine.proname='query_auction_quotes' then
            definition := replace(definition,'metric_algorithm_version text)', 'metric_algorithm_version text, quantity_quality jsonb)');
            definition := replace(definition,'metric.seal_amount, metric.algorithm_version',
                'metric.seal_amount, metric.algorithm_version, quote.quantity_quality');
        elsif routine.proname='query_market_snapshot' then
            definition := replace(definition,'calculated_at timestamp with time zone)',
                'calculated_at timestamp with time zone, quantity_quality jsonb)');
            definition := replace(definition,'daily.input_hash, daily.calculated_at',
                'daily.input_hash, daily.calculated_at, market.quantity_quality');
        elsif routine.proname='query_latest_stock_daily_indicators' then
            definition := replace(definition,'indicator.trade_date,','indicator.quantity_quality, indicator.trade_date,');
            definition := replace(definition,'''symbol'', latest.symbol',
                '''quantity_quality'',latest.quantity_quality,''symbol'', latest.symbol');
        elsif routine.proname='query_call_auction_indicative_details' then
            definition := replace(definition,'''displayed_volume_shares'', detail.displayed_volume_shares',
                '''quantity_quality'',detail.quantity_quality,''displayed_volume_shares'', detail.displayed_volume_shares');
        elsif routine.proname='query_call_auction_market_series_snapshots' then
            definition := replace(definition,'''symbol'', matched.symbol',
                '''quantity_quality'',matched.quantity_quality,''symbol'', matched.symbol');
        elsif routine.proname='query_call_auction_market_snapshots' then
            definition := replace(definition,'snapshot.symbol,','snapshot.quantity_quality, snapshot.symbol,');
            definition := replace(definition,'''symbol'', matched.symbol',
                '''quantity_quality'',matched.quantity_quality,''symbol'', matched.symbol');
        elsif routine.proname='query_stock_technical_factors' then
            definition := replace(definition,'''factor_catalog_version'',f.factor_catalog_version,',
                '''factor_catalog_version'',f.factor_catalog_version,''source_factor_catalog_version'',f.source_factor_catalog_version,''quantity_quality'',f.quantity_quality,');
        elsif routine.proname='query_stock_pool_snapshot' then
            definition := replace(definition,'indicator.free_float_shares, indicator.circulating_market_value',
                'indicator.free_float_shares, indicator.circulating_market_value, indicator.quantity_quality');
        elsif routine.proname='query_limit_up_pool' then
            definition := replace(definition,'indicator.free_float_shares,',
                'indicator.quantity_quality, indicator.free_float_shares,');
            definition := replace(definition,'select symbol, code, name, free_float_market_cap_cny',
                'select symbol, code, name, free_float_market_cap_cny, quantity_quality');
            definition := replace(definition,'''snapshot_id'', selected.snapshot_id,',
                '''quantity_quality'',jsonb_build_object(''unit_contract_version'',''stock-quantity.v1'',''status'',case when missing_free_float_shares_count=0 then ''certified'' else ''unverified'' end,''reason'',''source_quantity_admission'',''field_groups'',''{}''::jsonb),''snapshot_id'', selected.snapshot_id,');
        end if;

        if routine.proname in ('query_daily_limit_up_list','query_daily_limit_down_list') then
            domain_name := case when routine.proname='query_daily_limit_up_list' then 'today_limit_up' else 'today_limit_down' end;
            if position(format('join %I.member m on m.snapshot_id = s.snapshot_id',domain_name) in definition)=0 then
                raise exception 'quantity gate cannot find immutable member page in %',routine.proname;
            end if;
            definition := replace(definition,
                format('join %I.member m on m.snapshot_id = s.snapshot_id',domain_name),
                format('join %I.member m on m.snapshot_id = s.snapshot_id and s.quantity_quality->>''status''=''certified''',domain_name));
            definition := replace(definition,'with selected_snapshot as (',format(
                'if p_version is not null and exists (select 1 from %I.snapshot source where source.trade_date=p_trade_date and source.version=p_version and not audit.snapshot_quantity_is_certified(%L,source.snapshot_id)) then raise exception ''quantity_units_unverified'' using errcode=''PT409''; end if; with selected_snapshot as (',
                domain_name,domain_name));
            definition := replace(definition,'''snapshot_id'', s.snapshot_id,',
                '''quantity_quality'',s.quantity_quality,''snapshot_id'', s.snapshot_id,');
            definition := replace(definition,'''symbol'', p.symbol,',
                '''quantity_quality'',jsonb_build_object(''unit_contract_version'',''stock-quantity.v1'',''status'',''certified'',''reason'',''certified_snapshot_inputs'',''field_groups'',''{}''::jsonb),''symbol'', p.symbol,');
        end if;

        if routine.proname='query_auction_one_price_limits' then
            quality_expression := 'audit.quantity_quality(selected_ingestion,''call_auction_market_series.quantities'')';
            definition := replace(definition,'return payload;',format('return audit.quantity_payload(payload,%s);',quality_expression));
        end if;

        if routine.proname in ('query_daily_bars','query_auction_quotes','query_market_snapshot') then
            -- Copy ACLs before dropping: no new execute permissions are invented.
            create temporary table if not exists quantity_gate_acl (grantee text, privilege text, grantable boolean) on commit drop;
            truncate quantity_gate_acl;
            insert into quantity_gate_acl select case when acl.grantee=0 then 'PUBLIC' else pg_get_userbyid(acl.grantee) end,
                acl.privilege_type,acl.is_grantable
            from pg_proc proc cross join lateral aclexplode(coalesce(proc.proacl,acldefault('f',proc.proowner))) acl
            where proc.oid=routine.oid;
            execute format('drop function %s',routine_signature);
            execute definition;
            execute format('revoke all on function %s from public',routine_signature);
            -- Resolve by name because the old OID is no longer valid.
            for route in select * from quantity_gate_acl loop
                execute format('grant %s on function %s to %s%s',route.privilege,
                    routine_signature,
                    case when route.grantee='PUBLIC' then 'PUBLIC' else quote_ident(route.grantee) end,
                    case when route.grantable then ' with grant option' else '' end);
            end loop;
            execute format('alter function %s owner to %I',routine_signature,pg_get_userbyid(routine.proowner));
            if routine.description is not null then
                execute format('comment on function %s is %L',routine_signature,routine.description);
            end if;
        else
            execute definition;
        end if;
    end loop;
end;
$rpc_gates$;

revoke all on audit.unit_safe_auction_quote_metric,audit.unit_safe_stock_technical_factor_snapshot,
    audit.unit_safe_today_limit_up_snapshot,audit.unit_safe_today_limit_down_snapshot from public,anon,authenticated,market_data_api;
grant select on audit.unit_safe_auction_quote_metric,audit.unit_safe_stock_technical_factor_snapshot,
    audit.unit_safe_today_limit_up_snapshot,audit.unit_safe_today_limit_down_snapshot to market_data_worker;
revoke all on function audit.quantity_unit_is_certified(uuid,text),audit.quantity_quality(uuid,text),
    audit.snapshot_quantity_is_certified(text,uuid),audit.quantity_payload(jsonb,jsonb) from public;
grant execute on function audit.quantity_unit_is_certified(uuid,text),audit.quantity_quality(uuid,text),
    audit.snapshot_quantity_is_certified(text,uuid),audit.quantity_payload(jsonb,jsonb)
    to market_data_worker,market_data_api,anon,authenticated;
comment on function api_v1.quantity_unit_contract_version() is
    'Read-only deployment preflight for stock-quantity.v1; no ingestion, repair, or schema mutation.';
comment on column api_v1.daily_bars.volume is
    '股票当日成交股数；100表示100股，不是100手；缺量或未通过当前Raw证据认证为NULL，见quantity_quality。指数按独立契约认证。';
comment on column api_v1.daily_bars.amount is
    '当日成交额，人民币元；缺额或未通过对应字段组的当前Raw证据认证为NULL，不以0替代。';
comment on column api_v1.daily_bars.quantity_quality is
    'stock-quantity.v1量额字段组的独立单位认证状态与原因；保留未认证记录的价格及日期，不泄漏Raw路径或凭据。';
comment on column api_v1.share_capital.quantity_quality is
    '股本以股表示；未认证股本返回NULL；保留有效日期与非数量事实。';
comment on column api_v1.rights_issues.quantity_quality is
    '配股基准股数及募集金额的数量单位认证状态；数量缺失保持NULL，不改配股价格与比例。';
comment on column api_v1.stock_daily_indicators.quantity_quality is
    '来源股本股数及市值元的单位认证状态；换手率、量比等独立来源比例不由本库日K重算。';
comment on column api_v1.board_index_daily_bars.quantity_quality is
    '指数数量采用独立来源契约；未认证量额为NULL，不套用普通股票乘100规则。';
comment on column api_v1.convertible_bonds.quantity_quality is
    '可转债发行规模采用独立来源金额契约；未认证为NULL，不从股票股数证书推断。';
comment on column api_v1.convertible_bond_daily_bars.quantity_quality is
    '可转债成交张数、成交额及剩余规模采用独立来源契约；未认证为NULL，不套用股票股数换算。';
comment on column api_v1.market_capitalizations.quantity_quality is
    '新1.2.0结果依赖已认证股本输入；旧算法市值保留内部审计，公开为NULL及unverified。';

-- Exact quantity-field comment scope; no price/strategy or unknown index-unit inference.
do $quantity_comments$
declare route record; quantity_column record; business text;
begin
    for route in select * from (values
        ('realtime.stock_quote_snapshot','source_timestamp时点行情'),
        ('realtime.five_level_quote_snapshot','quote_semantics指定的竞价指示或已验证盘口'),
        ('realtime.call_auction_market_snapshot','开盘集合竞价时点行情'),
        ('realtime.call_auction_market_series_snapshot','value_semantics指定的当轮竞价指示或开盘成交行情'),
        ('realtime.call_auction_market_series_snapshot_history','归档value_semantics指定的竞价指示或开盘成交行情'),
        ('realtime.eod_quote_snapshot','收盘时点盘口')
    ) targets(relation_name,meaning) loop
        for quantity_column in
            select attname from pg_attribute where attrelid=route.relation_name::regclass
              and attnum>0 and not attisdropped
              and (attname ~ '^(bid|ask)[1-5]_volume$'
                   or attname in ('cumulative_volume','cumulative_amount','seal_amount'))
        loop
            business := case
                when quantity_column.attname ~ '^bid[1-5]_volume$' then
                    format('买%s档委托数量，标准单位股；',substring(quantity_column.attname from 4 for 1))
                when quantity_column.attname ~ '^ask[1-5]_volume$' then
                    format('卖%s档委托数量，标准单位股；',substring(quantity_column.attname from 4 for 1))
                when quantity_column.attname='cumulative_volume' then
                    '来源累计或指示数量，标准单位股；必须按所属语义区分，竞价指示不等于实际逐笔成交；'
                when quantity_column.attname='cumulative_amount' then
                    '来源累计或指示金额，标准单位人民币元；必须按所属语义区分，竞价指示金额不等于实际逐笔成交额；'
                else '封单金额，人民币元；对应封单档价（元/股）×委托股数；' end;
            execute format('comment on column %s.%I is %L',route.relation_name,quantity_column.attname,
                route.meaning || '：' || business || '缺失为NULL，显式0保持0，未认证历史不得冒充标准数量发布。');
        end loop;
    end loop;
end;
$quantity_comments$;

comment on column core.daily_bar.volume is
    '未复权日K来源数量；股票契约单位为整数股，100股就是100股；指数采用独立数量契约，未认证历史不得按股发布；NULL与0不同，不补缺失或停牌行情。';
comment on column core.daily_bar.amount is
    '未复权日K来源成交金额；股票契约单位人民币元，Decimal精确保存；独立认证金额字段组，NULL不补零，指数金额需独立来源证据。';
comment on column core.stock_daily_indicator.total_shares is
    '本交易日来源总股本，标准单位股；source-reported事实，未认证不发布标准数量，不用成交量推算。';
comment on column core.stock_daily_indicator.circulating_shares is
    '本交易日来源流通股本，标准单位股；不与自由流通股本互换；未认证或缺失保持公开NULL。';
comment on column core.stock_daily_indicator.free_float_shares is
    '本交易日来源自由流通股本，标准单位股；用于自由流通口径，不与流通股本分母互换；未认证或缺失保持公开NULL。';
comment on column capital.share_capital.total_shares is
    'effective_date生效的总股本，标准单位股；证据未认证的较新修订不能被旧股本回退替代。';
comment on column capital.share_capital.restricted_shares is
    'effective_date生效的限售股本，标准单位股；缺失为NULL，未认证不得按标准股数发布。';
comment on column capital.share_capital.circulating_shares is
    'effective_date生效的流通股本，标准单位股；与listed_a_shares/free_float口径独立，缺失为NULL。';
comment on column capital.share_capital.listed_a_shares is
    'effective_date生效的上市A股股本，标准单位股；原市值公式优先使用此字段，否则使用流通股本，不改变既有分母。';
comment on column core.top10_float_holder.hold_amount is
    '公告修订对应股东持有的流通股份数量，标准单位股；按独立来源证据认证，未认证为公开NULL。';
comment on column core.top10_float_holder.hold_change is
    '公告来源持股数量变动，标准单位股，可为负；有符号变化不套非负成交量规则，未认证为公开NULL。';
comment on column realtime.call_auction_indicative_detail.displayed_volume_shares is
    '集合竞价展示匹配数量，标准单位股；不是实际逐笔成交量，也不是委托明细；保留来源显示分类及观测时点，未认证为公开NULL。';
comment on column derived.auction_quote_metric.bid_depth_5 is
    '对应买一至买五委托股数之和，单位股；输入数量证据未认证则不公开该派生深度。';
comment on column derived.auction_quote_metric.ask_depth_5 is
    '对应卖一至卖五委托股数之和，单位股；缺档沿原完整性语义，不伪造零。';
comment on column derived.auction_quote_metric.imbalance_5 is
    '(买五档深度-卖五档深度)/(买五档深度+卖五档深度)，无量纲；分母0或缺失为NULL，未认证数量不参与公开派生。';
comment on column derived.auction_quote_metric.seal_amount is
    '对应方向和竞价时点的封单档价（元/股）×委托股数，单位人民币元；不把竞价指示改称实际成交，未认证输入不公开。';
comment on column today_limit_up.member.volume is
    '不可变涨停快照复制的当日实际成交股数，单位股；依赖原日K输入血缘，原批次任一数量修复后旧快照不得被再认证复活。';
comment on column today_limit_up.member.amount_cny is
    '不可变涨停快照复制的当日实际成交额，人民币元；独立金额认证，原对象保留审计，不原地重写。';
comment on column convertible_bond.daily_bar.volume is
    '可转债本交易日成交数量，标准单位张；采用独立来源契约及字段组，不使用股票股数证书或股票乘100规则；未认证为公开NULL。';
comment on column convertible_bond.daily_bar.amount is
    '可转债本交易日成交金额，标准单位人民币元；金额独立认证，不能仅因成交张数已认证而公开。';
comment on column convertible_bond.daily_bar.remain_size is
    '可转债剩余面值规模，标准单位人民币元；独立来源金额/规模契约，不能从股票或成交张数单位推断。';
comment on column convertible_bond.bond.issue_size is
    '可转债发行面值规模，标准单位人民币元；来源规模单位需独立证明，未认证为公开NULL。';
comment on column core.board_index_daily_bar.volume is
    '指数/THS来源数量，量纲采用独立来源契约；未经认证不推断为股票股数或统一乘100，公开NULL并附质量原因。';
comment on column core.board_index_daily_bar.amount is
    '指数/THS来源金额；来源单位与缩放必须独立认证，未知量纲不得冒称人民币元，公开NULL并附质量原因。';
comment on column core.stock_technical_factor_snapshot.unadjusted is
    '原始目录的不复权来源因子JSON；逐叶保留公式/量纲；未认证obv/emv/maemv仅在公开v2响应保留键并置NULL，不篡改源JSON或统一缩放。';
comment on column core.stock_technical_factor_snapshot.forward_adjusted is
    '原始目录的前复权来源因子JSON；逐叶保留公式/量纲，数量敏感叶子按独立字段组认证，源数据保持不可变。';
comment on column core.stock_technical_factor_snapshot.backward_adjusted is
    '原始目录的后复权来源因子JSON；逐叶保留公式/量纲，数量敏感叶子按独立字段组认证，源数据保持不可变。';
notify pgrst,'reload schema';
