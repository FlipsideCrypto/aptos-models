{{ config(
    materialized = 'view',
    tags = ['core']
) }}

SELECT
    f.block_date,
    f.address,
    f.token_address,
    f.balance_unadj,
    f.balance,
    f.balance * p.price AS balance_usd,
    f.symbol,
    f.name,
    f.decimals,
    COALESCE(p.is_verified, FALSE) AS token_is_verified,
    f.frozen,
    f.fact_balances_id AS ez_balances_id,
    f.inserted_timestamp,
    f.modified_timestamp
FROM
    {{ ref('core__fact_balances') }} f
    LEFT JOIN {{ ref('price__ez_prices_hourly') }} p
        ON LOWER(f.token_address) = LOWER(p.token_address)
        AND p.hour = f.block_date::TIMESTAMP
