{{ config(
    materialized = 'incremental',
    unique_key = "lending_echo_deposits_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['modified_timestamp::DATE'],
    tags = ['noncore']
) }}

WITH events AS (
    SELECT 
        block_number,
        block_timestamp,
        version,
        tx_hash,
        success,
        payload_function,
        event_index,
        event_type,
        event_address,
        event_module,
        event_resource,
        event_data,
        event_data:user::string AS depositor,
        event_data:amount::number AS amount,
        event_data:reserve::string AS lending_market,  -- reserve is the market
        _inserted_timestamp
    FROM {{ ref('silver__events') }}
    WHERE event_address = '0xeab7ea4d635b6b6add79d5045c4a45d8148d88287b1cfa1c3b6a4b56f46839ed'
        AND event_module = 'supply_logic' 
        AND event_resource = 'Supply'
{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% else %}
    AND block_timestamp :: DATE >= '2024-08-26'
{% endif %}
)

SELECT 
    A.block_number,
    A.block_timestamp,
    A.version,
    A.tx_hash,
    A.event_index,
    A.event_address,
    A.event_resource,
    A.depositor,
    A.amount,
    A.lending_market,  -- The reserve/market identifier
    B.token AS token_address,
    A._inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['A.tx_hash', 'A.event_index']) }} AS lending_echo_deposits_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM events A
LEFT JOIN {{ ref('silver__lending_echo_markets') }} b
    ON A.lending_market = b.reserve