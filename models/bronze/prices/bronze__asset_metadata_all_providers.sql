{{ config (
    materialized = 'view',
    tags = ['noncore']
) }}

SELECT
    token_address,
    id,
    symbol,
    blockchain,
    provider,
    _unique_key,
    _inserted_timestamp,
    modified_timestamp,
    inserted_timestamp
FROM
    {{ source(
        'crosschain_silver',
        'asset_metadata_all_providers'
    ) }}
WHERE
    blockchain = 'aptos'
    AND token_address LIKE '%:%'
