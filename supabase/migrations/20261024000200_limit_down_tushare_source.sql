-- ADR-0056/0068, Issue #93: retain old facts and admit the explicitly chosen provider.
alter table today_limit_down.source_observation
    drop constraint source_observation_source_code_check;
alter table today_limit_down.source_observation
    add constraint source_observation_source_code_check
    check (source_code in ('akshare','tushare'));

comment on column today_limit_down.source_observation.source_code is
    '真实来源提供商；历史akshare保留，当前采集使用tushare limit_list_d(limit_type=D)，不得拼接或冒用来源。';
comment on column today_limit_down.source_observation.source_reported_sealed_funds_cny is
    '来源报告封单金额，人民币元；Tushare limit_list_d D的fd_amount按元原值保存，不是limit_amount板上成交额，不作手股换算；缺失为NULL，0保留0。';
comment on column today_limit_down.member.source_reported_sealed_funds_cny is
    '不可变快照中的来源封单金额，人民币元；当前来源为Tushare limit_list_d D.fd_amount，按元原值冻结，与收盘卖一派生金额分列；缺失为NULL。';
