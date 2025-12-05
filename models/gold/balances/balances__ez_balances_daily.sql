{{ config(
    materialized = 'view',
    tags = ['daily_balances']
) }}

WITH end_of_day_prices AS (
    SELECT
        token_address,
        hour::DATE AS price_date,
        price
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
    b.balance_changed_on_date,
    b.balances_daily_id AS ez_balances_daily_id,
    b.inserted_timestamp,
    b.modified_timestamp
FROM
    {{ ref('silver__bals_daily') }} b
    LEFT JOIN {{ ref('core__dim_tokens') }} t
        ON LOWER(b.token_address) = LOWER(t.token_address)
    LEFT JOIN end_of_day_prices p
        ON LOWER(b.token_address) = LOWER(p.token_address)
        AND b.balance_date = p.price_date
