-- ADR-0068 clarification accepted 2026-10-24: independent source-factor scale.
-- Preserve the view's shape, owner and grants; do not alter any source facts.
create or replace view audit.unit_safe_stock_technical_factor_snapshot as
select symbol,trade_date,market,source_code,'stock-technical-factor.v3'::text as factor_catalog_version,
       unadjusted,forward_adjusted,backward_adjusted,common,ingestion_id,created_at,updated_at,
       factor_catalog_version as source_factor_catalog_version,
       jsonb_build_object(
           'unit_contract_version','stock-quantity.v1',
           'status','source_native',
           'reason','provider_native_factor_scale',
           'field_groups',jsonb_build_object('stock_technical_factor.source_factors',
               jsonb_build_object('status','source_native','reason','provider_native_factor_scale'))
       ) as quantity_quality
from core.stock_technical_factor_snapshot;

comment on view audit.unit_safe_stock_technical_factor_snapshot is
    '技术因子公开v3：保持Tushare原生指标尺度，含OBV/EMV/MAEMV；不声明为股或无量纲，不作手股转换；来源缺失保留NULL。';
comment on column core.stock_technical_factor_snapshot.unadjusted is
    '不复权Tushare来源技术因子JSON；OBV/EMV/MAEMV按原生指标尺度保存，不作手股转换，不声明为股或无量纲；缺失为NULL。';
comment on column core.stock_technical_factor_snapshot.forward_adjusted is
    '前复权Tushare来源技术因子JSON；原生指标尺度，不作手股转换，缺失为NULL。';
comment on column core.stock_technical_factor_snapshot.backward_adjusted is
    '后复权Tushare来源技术因子JSON；原生指标尺度，不作手股转换，缺失为NULL。';
