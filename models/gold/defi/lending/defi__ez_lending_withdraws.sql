{{ config(
    materialized = 'incremental',
    unique_key = ['ez_lending_withdraws_id'],
    incremental_strategy = 'merge',
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::DATE"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash,platform,depositor,token_address);",
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


WITH withdraws AS (
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
    depositor,
    token_address,
    amount as amount_raw,
    lending_echelon_withdraws_id as ez_lending_withdraws_id
    FROM {{ ref('silver__lending_echelon_withdraws') }}
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
    depositor,
    token_address,
    amount as amount_raw,
    lending_echo_withdraws_id as ez_lending_withdraws_id
FROM
    {{ ref('silver__lending_echo_withdraws') }} a
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
                withdraws
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
    a.depositor,
    a.token_address,
    b.symbol as token_symbol,
    COALESCE(
        b.is_verified,
        FALSE
    ) AS token_is_verified,
    a.amount_raw,
    CASE
        WHEN COALESCE(
            t.decimals,
            b.decimals
        ) IS NOT NULL THEN amount_raw / pow(10, COALESCE(t.decimals, b.decimals))
    END AS amount,
        ROUND(
      amount * b.price,
      2
    ) AS amount_usd,
    ez_lending_withdraws_id,
    sysdate() AS inserted_timestamp,
    sysdate() AS modified_timestamp
FROM withdraws a
LEFT JOIN {{ ref('core__dim_tokens') }} t
    ON LOWER(
        A.token_address
    ) = LOWER(
        t.token_address
    )
LEFT JOIN prices b
ON LOWER(a.token_address) = LOWER(b.token_address)
    AND DATE_TRUNC(
        'hour',
        a.block_timestamp
    ) = b.hour
