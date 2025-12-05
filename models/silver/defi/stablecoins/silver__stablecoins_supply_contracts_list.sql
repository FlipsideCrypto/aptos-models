{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ["stablecoins_supply_contracts_list_id"],
    tags = ['silver','defi','stablecoins','curated']
) }}

WITH bridge_addresses AS (
    SELECT DISTINCT
        bridge_address AS address,
        'bridge' AS contract_type
    FROM {{ ref('silver__bridge_metadata') }}

    {% if is_incremental() %}
    WHERE modified_timestamp >= (
        SELECT MAX(modified_timestamp)
        FROM {{ this }}
    )
    {% endif %}
),
dex_pool_addresses AS (
    SELECT DISTINCT
        event_address AS address,
        'dex' AS contract_type
    FROM {{ ref('defi__ez_dex_swaps') }}
    WHERE event_address IS NOT NULL

    {% if is_incremental() %}
    AND modified_timestamp >= (
        SELECT MAX(modified_timestamp)
        FROM {{ this }}
    )
    {% endif %}
),
lending_addresses AS (
    SELECT DISTINCT
        event_address AS address,
        'lending' AS contract_type
    FROM {{ ref('defi__ez_lending_deposits') }}
    WHERE event_address IS NOT NULL

    {% if is_incremental() %}
    AND modified_timestamp >= (
        SELECT MAX(modified_timestamp)
        FROM {{ this }}
    )
    {% endif %}
),
cex_addresses AS (
    -- Actual CEX address by assuming hot/cold wallet labels
    SELECT DISTINCT
        address,
        'cex' AS contract_type
    FROM {{ ref('core__dim_labels') }}
    WHERE label_type = 'cex'
      AND label_subtype IN ('hot_wallet', 'cold_wallet')
      AND address IS NOT NULL

    {% if is_incremental() %}
    AND modified_timestamp >= (
        SELECT MAX(modified_timestamp)
        FROM {{ this }}
    )
    {% endif %}
),
contract_list AS (
    -- Fetching ALL of contracts since Aptos doesn't yet have core__dim_contracts
    SELECT DISTINCT
        SPLIT_PART(payload_function, '::', 1) AS address,
        'contract' AS contract_type
    FROM {{ ref('core__fact_transactions') }}
    WHERE payload_function IS NOT NULL
      AND payload_function != ''
      AND success = TRUE

    {% if is_incremental() %}
    AND modified_timestamp >= (
        SELECT MAX(modified_timestamp)
        FROM {{ this }}
    )
    {% endif %}
),

all_contracts AS (
    SELECT * FROM bridge_addresses
    UNION ALL
    SELECT * FROM dex_pool_addresses
    UNION ALL
    SELECT * FROM lending_addresses
    UNION ALL
    SELECT * FROM cex_addresses
    UNION ALL
    SELECT * FROM contract_list
)

SELECT
    address,
    contract_type,
    {{ dbt_utils.generate_surrogate_key(['address','contract_type']) }} AS stablecoins_supply_contracts_list_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM all_contracts
