{{ config(
    materialized = 'incremental',
    unique_key = 'token_address',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['modified_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(token_address, symbol);",
    tags = ['gold', 'defi', 'stablecoins', 'curated']
) }}

SELECT
    token_address,
    symbol,
    name,
    decimals,
    is_verified,
    is_verified_modified_timestamp,
    stablecoins_metadata_id AS dim_stablecoins_id,
    inserted_timestamp,
    modified_timestamp
FROM {{ ref('silver__stablecoins_metadata') }}

{% if is_incremental() %}
WHERE modified_timestamp >= (
    SELECT MAX(modified_timestamp)
    FROM {{ this }}
)
{% endif %}
