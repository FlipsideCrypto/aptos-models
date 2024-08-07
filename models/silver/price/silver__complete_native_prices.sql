{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'complete_native_prices_id',
    cluster_by = ['HOUR::DATE'],
    tags = ['non_realtime']
) }}

SELECT
    HOUR,
    asset_id,
    '0x1::aptos_coin::aptoscoin' AS token_address,
    symbol,
    NAME,
    8 AS decimals,
    price,
    blockchain,
    is_imputed,
    is_deprecated,
    provider,
    source,
    _inserted_timestamp,
    inserted_timestamp,
    modified_timestamp,
    complete_native_prices_id,
    _invocation_id
FROM
    {{ ref(
        'bronze__complete_native_prices'
    ) }}

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(
                modified_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}
