{{ config(
    materialized = 'incremental',
    unique_key = ['tx_hash', 'event_index'],
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver', 'defi', 'stablecoins']
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
        t.tx_hash,
        t.version,
        t.success,
        t.event_index,
        t.transfer_event,
        t.store_address,
        t.account_address,
        t.token_address AS metadata_address,
        t.amount,
        t.modified_timestamp AS source_modified_timestamp,
        d.symbol,
        d.name,
        d.decimals
    FROM {{ ref('core__fact_transfers') }} t
    INNER JOIN verified_stablecoins d
        ON t.token_address = d.token_address
    WHERE t.success = TRUE
        -- Bridge exclusion
        AND t.account_address NOT IN (
            SELECT bridge_address FROM {{ ref('silver__bridge_addresses_seed') }}
        )
        AND (t.tx_hash, t.token_address) NOT IN (
            SELECT tx_hash, token_address FROM {{ ref('silver__bridge_combined') }}
        )

    {% if is_incremental() %}
    AND t.modified_timestamp >= (
        SELECT MAX(source_modified_timestamp)
        FROM {{ this }}
    )
    {% endif %}
),

tx_event_summary AS (
    SELECT
        tx_hash,
        metadata_address,
        SUM(CASE WHEN transfer_event = 'DepositEvent' THEN 1 ELSE 0 END) AS deposit_count,
        SUM(CASE WHEN transfer_event = 'WithdrawEvent' THEN 1 ELSE 0 END) AS withdraw_count,
        SUM(CASE WHEN transfer_event = 'DepositEvent' THEN amount ELSE 0 END) AS total_deposit_amount,
        SUM(CASE WHEN transfer_event = 'WithdrawEvent' THEN amount ELSE 0 END) AS total_withdraw_amount
    FROM stablecoin_transfers
    GROUP BY tx_hash, metadata_address
),

mint_txs AS (
    SELECT
        tx_hash,
        metadata_address,
        total_deposit_amount - total_withdraw_amount AS net_mint_amount
    FROM tx_event_summary
    WHERE deposit_count > withdraw_count
        AND (total_deposit_amount - total_withdraw_amount) > 0
),

burn_txs AS (
    SELECT
        tx_hash,
        metadata_address,
        total_withdraw_amount - total_deposit_amount AS net_burn_amount
    FROM tx_event_summary
    WHERE withdraw_count > deposit_count
        AND (total_withdraw_amount - total_deposit_amount) > 0
),

mint_events AS (
    SELECT
        t.block_number,
        t.block_timestamp,
        t.tx_hash,
        t.version,
        t.success,
        t.event_index,
        'Mint' AS event_type,
        t.transfer_event AS event_resource,
        t.metadata_address,
        t.symbol,
        t.name,
        t.decimals,
        t.store_address AS sender_address, 
        t.account_address AS receiver_address,
        t.account_address AS owner_address,
        m.net_mint_amount AS amount_unadj,
        m.net_mint_amount / POW(10, t.decimals) AS amount,
        t.source_modified_timestamp
    FROM stablecoin_transfers t
    INNER JOIN mint_txs m
        ON t.tx_hash = m.tx_hash
        AND t.metadata_address = m.metadata_address
    WHERE t.transfer_event = 'DepositEvent'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY t.tx_hash, t.metadata_address
        ORDER BY t.event_index
    ) = 1
),

burn_events AS (
    SELECT
        t.block_number,
        t.block_timestamp,
        t.tx_hash,
        t.version,
        t.success,
        t.event_index,
        'Burn' AS event_type,
        t.transfer_event AS event_resource,
        t.metadata_address,
        t.symbol,
        t.name,
        t.decimals,
        t.account_address AS sender_address,
        t.store_address AS receiver_address,
        t.account_address AS owner_address,
        b.net_burn_amount AS amount_unadj,
        b.net_burn_amount / POW(10, t.decimals) AS amount,
        t.source_modified_timestamp
    FROM stablecoin_transfers t
    INNER JOIN burn_txs b
        ON t.tx_hash = b.tx_hash
        AND t.metadata_address = b.metadata_address
    WHERE t.transfer_event = 'WithdrawEvent'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY t.tx_hash, t.metadata_address
        ORDER BY t.event_index
    ) = 1
)

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    version,
    success,
    event_index,
    event_type,
    event_resource,
    metadata_address,
    symbol,
    name,
    decimals,
    sender_address,
    receiver_address,
    owner_address,
    amount_unadj,
    amount,
    {{ dbt_utils.generate_surrogate_key(['tx_hash', 'event_index']) }} AS stablecoins_mint_burn_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id,
    source_modified_timestamp
FROM mint_events

UNION ALL

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    version,
    success,
    event_index,
    event_type,
    event_resource,
    metadata_address,
    symbol,
    name,
    decimals,
    sender_address,
    receiver_address,
    owner_address,
    amount_unadj,
    amount,
    {{ dbt_utils.generate_surrogate_key(['tx_hash', 'event_index']) }} AS stablecoins_mint_burn_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id,
    source_modified_timestamp
FROM burn_events
