{{ config(
    materialized = 'view',
    tags = ['core']
) }}

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
    {{ ref(
        'core__fact_transfers'
    ) }} A
    LEFT JOIN {{ ref('core__dim_tokens') }}
    b
    ON LOWER(
        A.token_address
    ) = LOWER(
        b.token_address
    )
    LEFT JOIN {{ ref('price__ez_prices_hourly') }}
    p
    ON LOWER(
        A.token_address
    ) = LOWER(
        p.token_address
    )
    AND DATE_TRUNC(
        'hour',
        A.block_timestamp
    ) = p.hour
