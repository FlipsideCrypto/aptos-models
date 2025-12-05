{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ["stablecoins_transfers_id"],
    cluster_by = ['block_date'],
    tags = ['silver','defi','stablecoins']
) }}

WITH verified_stablecoins AS (
    SELECT
        token_address,
        symbol,
        name,
        decimals
    FROM {{ ref('defi__dim_stablecoins') }}
    WHERE is_verified = TRUE
),

stablecoin_transfers AS (
    SELECT
        t.block_number,
        t.block_timestamp,
        t.block_timestamp::DATE AS block_date,
        t.tx_hash,
        t.version,
        t.success,
        t.event_index,
        t.transfer_event,
        t.account_address, 
        t.store_address,    
        t.token_address AS contract_address,
        t.is_fungible,      
        s.symbol,
        s.name,
        s.decimals,
        t.amount AS amount_unadj,
        t.amount / POW(10, s.decimals) AS amount,
        t.modified_timestamp
    FROM {{ ref('core__fact_transfers') }} t
    INNER JOIN verified_stablecoins s
        ON t.token_address = s.token_address
    WHERE t.success = TRUE
      AND t.amount IS NOT NULL
      AND t.amount > 0

    {% if is_incremental() %}
    AND t.modified_timestamp >= (
        SELECT MAX(modified_timestamp)
        FROM {{ this }}
    )
    {% endif %}
)

SELECT
    block_number,
    block_timestamp,
    block_date,
    tx_hash,
    version,
    success,
    event_index,
    transfer_event,  -- 'DepositEvent' or 'WithdrawEvent'
    account_address,
    store_address,
    contract_address,
    is_fungible,
    symbol,
    name,
    decimals,
    amount_unadj,
    amount,
    {{ dbt_utils.generate_surrogate_key(['tx_hash', 'event_index']) }} AS stablecoins_transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM stablecoin_transfers
