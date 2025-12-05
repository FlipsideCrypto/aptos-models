{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ["stablecoins_supply_contracts_id"],
    cluster_by = ['block_date'],
    tags = ['silver','defi','stablecoins','curated_daily']
) }}

WITH contracts AS (
    SELECT address, contract_type
    FROM {{ ref('silver__stablecoins_supply_contracts_list') }}
),

balances AS (
    SELECT
        s.block_date,
        s.address,
        s.contract_address,
        s.symbol,
        s.decimals,
        s.balance,
        COALESCE(CASE WHEN c0.address IS NOT NULL THEN s.balance ELSE 0 END, 0) AS bridge_balance,
        COALESCE(CASE WHEN c1.address IS NOT NULL THEN s.balance ELSE 0 END, 0) AS dex_balance,
        COALESCE(CASE WHEN c2.address IS NOT NULL THEN s.balance ELSE 0 END, 0) AS lending_pool_balance,
        COALESCE(CASE WHEN c3.address IS NOT NULL THEN s.balance ELSE 0 END, 0) AS cex_balance,
        COALESCE(CASE WHEN c4.address IS NOT NULL THEN s.balance ELSE 0 END, 0) AS contracts_balance,
        s.modified_timestamp
    FROM {{ ref('silver__stablecoins_supply_by_address_imputed') }} s
    LEFT JOIN contracts c0 ON s.address = c0.address AND c0.contract_type = 'bridge'
    LEFT JOIN contracts c1 ON s.address = c1.address AND c1.contract_type = 'dex'
    LEFT JOIN contracts c2 ON s.address = c2.address AND c2.contract_type = 'lending'
    LEFT JOIN contracts c3 ON s.address = c3.address AND c3.contract_type = 'cex'
    LEFT JOIN contracts c4 ON s.address = c4.address AND c4.contract_type = 'contract'
    WHERE c0.address IS NOT NULL
       OR c1.address IS NOT NULL
       OR c2.address IS NOT NULL
       OR c3.address IS NOT NULL
       OR c4.address IS NOT NULL

    {% if is_incremental() %}
    AND s.modified_timestamp >= (
        SELECT MAX(modified_timestamp)
        FROM {{ this }}
    )
    {% endif %}
)

SELECT
    block_date,
    address,
    contract_address,
    symbol,
    decimals,
    bridge_balance,
    dex_balance,
    lending_pool_balance,
    cex_balance,
    contracts_balance,
    {{ dbt_utils.generate_surrogate_key(
        ['block_date','address','contract_address']
    ) }} AS stablecoins_supply_contracts_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM balances
