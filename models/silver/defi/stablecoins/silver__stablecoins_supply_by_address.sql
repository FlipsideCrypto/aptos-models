{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ["stablecoins_supply_by_address_id"],
    cluster_by = ['block_date'],
    tags = ['silver','defi','stablecoins','curated_daily']
) }}

WITH verified_stablecoins AS (
    SELECT
        token_address,
        symbol,
        decimals,
        name
    FROM {{ ref('defi__dim_stablecoins') }}
    WHERE is_verified = TRUE
      AND token_address IS NOT NULL
)

SELECT
    b.block_date,
    b.address,
    b.token_address AS contract_address,
    b.symbol,
    b.decimals,
    b.name,
    b.post_balance,
    b.balance,
    {{ dbt_utils.generate_surrogate_key(
        ['block_date','address','contract_address']
    ) }} AS stablecoins_supply_by_address_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    b._inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM {{ ref('silver__balances') }} b
INNER JOIN verified_stablecoins s
    ON b.token_address = s.token_address

{% if is_incremental() %}
WHERE b.modified_timestamp >= (
    SELECT MAX(modified_timestamp)
    FROM {{ this }}
)
{% endif %}
