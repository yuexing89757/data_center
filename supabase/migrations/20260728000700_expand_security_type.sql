alter table core.security
    drop constraint security_type_check,
    add constraint security_type_check check (
        security_type in (
            'stock',
            'index',
            'other',
            'convertible_bond',
            'etf',
            'unknown'
        )
    );
