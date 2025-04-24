{{ config(
    materialized = 'incremental',
    unique_key = "token_address",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['core']
) }}

SELECT
    metadata_address AS token_address,
    creator_address,
    symbol,
    NAME,
    decimals,
    {{ dbt_utils.generate_surrogate_key(
        ['metadata_address']
    ) }} AS fungible_asset_metadata_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('bronze_api__aptoslabs_fungible_asset_metadata_by_pk') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY metadata_address
ORDER BY
    _inserted_timestamp DESC)) = 1

{% if is_incremental() %}
{% else %}
    UNION ALL
    SELECT
        '0xa' AS token_address,
        '0x0000000000000000000000000000000000000000000000000000000000000001' AS creator_address,
        'APT' AS symbol,
        'Aptos Coin' AS NAME,
        8 AS decimals,
        {{ dbt_utils.generate_surrogate_key(
            ['token_address']
        ) }} AS fungible_asset_metadata_id,
        SYSDATE() AS inserted_timestamp,
        SYSDATE() AS modified_timestamp,
        '1900-01-01' AS _inserted_timestamp,
        '{{ invocation_id }}' AS _invocation_id
    {% endif %}
