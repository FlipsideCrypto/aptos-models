{{ config(
    materialized = 'incremental',
    unique_key = ['token_address'],
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(token_address,symbol);",
    tags = ['core','full_test']
) }}

SELECT
    coin_type AS token_address,
    NAME,
    symbol,
    decimals,
    coin_type_hash,
    creator_address,
    transaction_created_timestamp,
    transaction_version_created,
    {{ dbt_utils.generate_surrogate_key(
        ['token_address']
    ) }} AS dim_token_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
FROM
    {{ ref(
        'silver__coin_info'
    ) }}

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
UNION ALL
SELECT
    token_address,
    NAME,
    symbol,
    decimals,
    NULL AS coin_type_hash,
    NULL AS creator_address,
    NULL AS transaction_created_timestamp,
    NULL AS transaction_version_created,
    {{ dbt_utils.generate_surrogate_key(
        ['token_address']
    ) }} AS dim_token_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
FROM
    {{ ref('silver__fungible_asset_metadata') }}

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
