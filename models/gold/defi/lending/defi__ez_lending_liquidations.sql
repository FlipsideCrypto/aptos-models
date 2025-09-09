{{ config(
    materialized = 'incremental',
    unique_key = ['ez_lending_liquidations_id'],
    incremental_strategy = 'merge',
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::DATE"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash);",
    tags = ['noncore']
) }}

{% if execute %}
    {% if is_incremental() %}
    {% set max_modified_query %}
    SELECT
        MAX(modified_timestamp) AS modified_timestamp
    FROM
        {{ this }}
    {% endset %}
    {% set max_modified_timestamp = run_query(max_modified_query)[0][0] %}
    {% endif %}
{% endif %}


WITH liquidations AS (
SELECT 
    'echelon' as platform,
    'echelon' as protocol,
    'v1' as protocol_version,
    block_number,
    block_timestamp,
    version,
    tx_hash,
    event_index,
    event_address,
    liquidator,
    borrower,
    amount as amount_raw,
    collateral_token,
    debt_token,
    lending_echelon_liquidations_id as ez_lending_liquidations_id
    FROM {{ ref('silver__lending_echelon_liquidations') }}
    {% if is_incremental() %}
WHERE
    modified_timestamp >= '{{ max_modified_timestamp }}'
{% endif %}
    UNION ALL
SELECT 
    'echo' as platform,
    'echo' as protocol,
    'v1' as protocol_version,
    block_number,
    block_timestamp,
    version,
    tx_hash,
    event_index,
    event_address,
    liquidator,
    borrower,
    amount as amount_raw,
    collateral_token,
    debt_token,
    lending_echo_liquidations_id as ez_lending_liquidations_id
FROM
    {{ ref('silver__lending_echo_liquidations') }} a
    {% if is_incremental() %}
WHERE
    modified_timestamp >= '{{ max_modified_timestamp }}'
{% endif %}
    UNION ALL
SELECT 
    'aries' as platform,
    'aries' as protocol,
    'v1' as protocol_version,
    block_number,
    block_timestamp,
    version,
    tx_hash,
    event_index,
    event_address,
    liquidator,
    borrower,
    amount as amount_raw,
    collateral_token,
    debt_token,
    lending_aries_liquidations_id as ez_lending_liquidations_id
FROM
    {{ ref('silver__lending_aries_liquidations') }} a
    {% if is_incremental() %}
WHERE
    modified_timestamp >= '{{ max_modified_timestamp }}'
{% endif %}
),
prices AS (
    SELECT
        HOUR,
        token_address,
        symbol,
        price,
        decimals,
        is_verified
    FROM
        {{ ref('price__ez_prices_hourly') }}
    WHERE
        hour >= (
            SELECT
                MIN(DATE_TRUNC('hour', block_timestamp))
            FROM
                liquidations
        )

)
SELECT 
    a.platform,
    a.protocol,
    a.protocol_version,
    a.block_number,
    a.block_timestamp,
    a.version,
    a.tx_hash,
    a.event_index,
    a.event_address,
    a.liquidator,
    a.borrower,
    a.collateral_token,
    a.debt_token,
    collateral_prices.symbol as collateral_token_symbol,
    COALESCE(
        collateral_prices.is_verified,
        FALSE
    ) AS collateral_token_is_verified,
    debt_prices.symbol as debt_token_symbol,
    COALESCE(
        debt_prices.is_verified,
        FALSE
    ) AS debt_token_is_verified,
    a.amount_raw,
    CASE
        WHEN COALESCE(
            debt_t.decimals,
            debt_prices.decimals
        ) IS NOT NULL THEN amount_raw / pow(10, COALESCE(debt_t.decimals, debt_prices.decimals))
    END AS amount,
        ROUND(
      amount * debt_prices.price,
      2
    ) AS amount_usd,
    ez_lending_liquidations_id,
    sysdate() AS inserted_timestamp,
    sysdate() AS modified_timestamp
FROM liquidations a
LEFT JOIN {{ ref('core__dim_tokens') }} debt_t
    ON LOWER(
        A.debt_token
    ) = LOWER(
        debt_t.token_address
    )
LEFT JOIN prices collateral_prices
ON LOWER(a.collateral_token) = LOWER(collateral_prices.token_address)
    AND DATE_TRUNC(
        'hour',
        a.block_timestamp
    ) = collateral_prices.hour
LEFT JOIN prices debt_prices
ON LOWER(a.debt_token) = LOWER(debt_prices.token_address)
    AND DATE_TRUNC(
        'hour',
        a.block_timestamp
    ) = debt_prices.hour
