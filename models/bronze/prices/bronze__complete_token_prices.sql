{{ config (
    materialized = 'view'
) }}

SELECT
    HOUR,
    token_address,
    asset_id,
    symbol,
    NAME,
    decimals,
    price,
    blockchain,
    blockchain_name,
    blockchain_id,
    is_imputed,
    is_deprecated,
    provider,
    source,
    _inserted_timestamp,
    inserted_timestamp,
    modified_timestamp,
    complete_token_prices_id,
    _invocation_id,
    is_verified
FROM
    {{ source(
        'crosschain_silver',
        'complete_token_prices'
    ) }}
