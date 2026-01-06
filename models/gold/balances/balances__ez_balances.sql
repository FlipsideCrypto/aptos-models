{{ config(
    materialized = 'view',
    tags = ['balances']
) }}

{# Use the same snapshot date as the silver snapshot model #}
{% set snapshot_date = var('SNAPSHOT_DATE', '2025-09-02') %}

WITH snapshot_balances AS (
    -- Historical balances from snapshot (state as of snapshot date)
    SELECT
        snapshot_date AS balance_date,
        address,
        token_address,
        balance,
        frozen,
        block_timestamp AS last_balance_change,
        balances_snapshot_id AS ez_balances_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('silver__balances_snapshot') }}
),

recent_balances AS (
    -- Balances that occurred after the snapshot date
    SELECT
        block_date AS balance_date,
        address,
        token_address,
        balance,
        frozen,
        block_timestamp AS last_balance_change,
        balances_id AS ez_balances_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('silver__balances') }}
    WHERE
        block_date > '{{ snapshot_date }}'::DATE
),

combined_balances AS (
    SELECT * FROM snapshot_balances
    UNION ALL
    SELECT * FROM recent_balances
),

prices AS (
    SELECT
        token_address,
        hour::DATE AS price_date,
        price,
        is_verified
    FROM
        {{ ref('price__ez_prices_hourly') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY token_address, hour::DATE
        ORDER BY hour DESC
    ) = 1
)

SELECT
    b.balance_date,
    b.address,
    b.token_address,
    b.balance AS balance_raw,
    b.balance / NULLIF(POW(10, COALESCE(t.decimals, 0)), 0) AS balance,
    (b.balance / NULLIF(POW(10, COALESCE(t.decimals, 0)), 0)) * p.price AS balance_usd,
    t.symbol,
    t.name AS token_name,
    t.decimals,
    b.frozen,
    b.last_balance_change,
    p.is_verified AS token_is_verified,
    b.ez_balances_id,
    b.inserted_timestamp,
    b.modified_timestamp
FROM
    combined_balances b
    LEFT JOIN {{ ref('core__dim_tokens') }} t
        ON LOWER(b.token_address) = LOWER(t.token_address)
    LEFT JOIN prices p
        ON LOWER(b.token_address) = LOWER(p.token_address)
        AND b.balance_date = p.price_date
