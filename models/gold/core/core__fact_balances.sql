{{ config(
    materialized = 'view',
    tags = ['core']
) }}

WITH balances_with_last_positive AS (
    SELECT
        block_date,
        address,
        token_address,
        post_balance,
        frozen,
        is_verified,
        balances_id,
        inserted_timestamp,
        modified_timestamp,
        LAST_VALUE(CASE WHEN post_balance > 0 THEN block_date END IGNORE NULLS) OVER (
            PARTITION BY address, token_address
            ORDER BY block_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS last_positive_date
    FROM {{ ref('silver__balances') }}
)

SELECT
    b.block_date,
    b.address,
    b.token_address,
    b.post_balance AS balance_unadj,
    b.post_balance / NULLIF(POW(10, COALESCE(t.decimals, 0)), 0) AS balance,
    COALESCE(t.symbol, NULL) AS symbol,
    t.name,
    t.decimals,
    b.is_verified AS token_is_verified,
    b.frozen,
    b.balances_id AS fact_balances_id,
    b.inserted_timestamp,
    b.modified_timestamp
FROM
    balances_with_last_positive b
    LEFT JOIN {{ ref('core__dim_tokens') }} t
        ON LOWER(b.token_address) = LOWER(t.token_address)
WHERE
    b.post_balance > 0
    OR (b.post_balance = 0 AND DATEDIFF('day', b.last_positive_date, b.block_date) <= 1) -- reducing 3d grace period for zero balances to only just 1d of 0's
