-- ADR-0068 / Issue #92. Preserve the accepted narrow live write bridge and
-- its ACL. Only newly created, bounded standard rows receive a certificate.
-- Reused historical snapshots are deliberately not retroactively certified.
do $bridge$
declare
    signature regprocedure := 'api_v1.persist_call_auction_indicative_details(uuid,uuid,text,date,timestamp with time zone,text,text,text,bigint,integer,jsonb)'::regprocedure;
    definition text;
    marker text := 'return jsonb_build_object(''outcome'',''created'',''version'',next_version,';
    admission text := $admission$
    update ingestion.ingestion_run set request_params = request_params || jsonb_build_object(
        'provider_schema_version','eastmoney.call_auction_indicative_detail.v1',
        'quantity_unit_contract_version','stock-quantity.v1',
        'quantity_normalizer_version','quantity-normalizer.v1',
        'source_quantity_unit','lot','standard_quantity_unit','share'
    ) where ingestion_id=p_ingestion_id;
    insert into audit.quantity_unit_certification (
        ingestion_id,unit_contract_version,field_group,status,raw_hashes,
        normalizer_version,evidence_hash
    ) values (
        p_ingestion_id,'stock-quantity.v1',
        'call_auction_indicative_detail.displayed_volume_shares','certified',
        jsonb_build_array(p_content_sha256),'quantity-normalizer.v1',
        encode(extensions.digest(
            p_ingestion_id::text || ':' || p_raw_id::text || ':' || p_content_sha256 ||
            ':eastmoney.call_auction_indicative_detail.v1:quantity-normalizer.v1:lot:share',
            'sha256'),'hex')
    );
    $admission$;
begin
    select pg_get_functiondef(signature) into definition;
    if position(marker in definition)=0 then
        raise exception 'controlled live bridge signature changed; review unit admission';
    end if;
    definition := replace(definition,marker,admission || marker);
    execute definition;
end;
$bridge$;
