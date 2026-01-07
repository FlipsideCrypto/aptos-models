{{ config(
    materialized = 'view',
    tags = ['core']
) }}

{#
  PERFORMANCE OPTIMIZATION: Pre-compute LOWER() and DATE_TRUNC() values in CTEs
  Original issues:
  - LOWER() on both sides of JOIN prevents index usage (non-sargable)
  - DATE_TRUNC() computed for every row before join comparison
#}

WITH transfers_base AS (
    SELECT
        block_number,
        block_timestamp,
        DATE_TRUNC('hour', block_timestamp) AS block_hour,
        tx_hash,
        version,
        success,
        event_index,
        transfer_event,
        account_address,
        amount,
        token_address,
        LOWER(token_address) AS token_address_lower,
        fact_transfers_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('core__fact_transfers') }}
),

tokens_base AS (
    SELECT
        token_address,
        LOWER(token_address) AS token_address_lower,
        symbol,
        decimals
    FROM
        {{ ref('core__dim_tokens') }}
),

prices_base AS (
    SELECT
        hour,
        token_address,
        LOWER(token_address) AS token_address_lower,
        symbol,
        decimals,
        price,
        is_verified
    FROM
        {{ ref('price__ez_prices_hourly') }}
)

SELECT
    A.block_number,
    A.block_timestamp,
    A.tx_hash,
    A.version,
    A.success,
    A.event_index,
    A.transfer_event,
    A.account_address,
    A.amount AS amount_unadj,
    A.amount / CASE
        WHEN COALESCE(
            b.decimals,
            p.decimals
        ) IS NOT NULL THEN pow(10, COALESCE(b.decimals, p.decimals))
    END AS amount,
    (
        A.amount / CASE
            WHEN COALESCE(
                b.decimals,
                p.decimals
            ) IS NOT NULL THEN pow(10, COALESCE(b.decimals, p.decimals))
        END
    ) * p.price AS amount_usd,
    A.token_address,
    COALESCE(
        p.symbol,
        b.symbol
    ) AS symbol,
    COALESCE(
        p.is_verified,
        FALSE
    ) AS token_is_verified,
    A.fact_transfers_id AS ez_transfers_id,
    A.inserted_timestamp,
    A.modified_timestamp
FROM
    transfers_base A
    LEFT JOIN tokens_base b
        ON A.token_address_lower = b.token_address_lower
    LEFT JOIN prices_base p
        ON A.token_address_lower = p.token_address_lower
        AND A.block_hour = p.hour
