{{ config(
    materialized = 'incremental',
    unique_key = "token_address",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['core']
) }}

SELECT
    metadata_address AS token_address,
    symbol,
    NAME,
    decimals,
    icon_uri,
    project_uri,
    {{ dbt_utils.generate_surrogate_key(
        ['metadata_address']
    ) }} AS fungible_asset_metadata_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('bronze_api__fungible_asset_metadata') }}
WHERE
    symbol IS NOT NULL

{% if is_incremental() %}
AND _inserted_timestamp >= (
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
