{{ config (
    materialized = 'view'
) }}

SELECT
    token_address,
    asset_id,
    symbol,
    NAME,
    decimals,
    blockchain,
    blockchain_name,
    blockchain_id,
    is_deprecated,
    provider,
    source,
    _inserted_timestamp,
    inserted_timestamp,
    modified_timestamp,
    complete_token_asset_metadata_id,
    _invocation_id
FROM
    {{ source(
        'crosschain_silver',
        'complete_token_asset_metadata'
    ) }}
WHERE
    blockchain ILIKE 'aptos' {# AND token_address LIKE '%:%' #}
