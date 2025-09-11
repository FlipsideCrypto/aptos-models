{{ config(
    materialized = 'incremental',
    unique_key = "lending_aries_withdraws_id",
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
        event_data:sender::string AS depositor,
        event_data:withdraw_amount::number AS amount,
        SUBSTRING(
            event_type, 
            POSITION('<' IN event_type) + 1,
            POSITION('>' IN event_type) - POSITION('<' IN event_type) - 1
        ) AS token_address,
        _inserted_timestamp
    FROM {{ ref('silver__events') }} 
    WHERE event_address = '0x9770fa9c725cbd97eb50b2be5f7416efdfd1f1554beb0750d4dae4c64e860da3'
        AND event_module = 'controller'
        AND event_resource LIKE 'WithdrawEvent%'
        AND event_data:borrow_amount::number = 0  -- removes borrows
        AND event_data:withdraw_amount::number > 0    

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% else %}
    AND block_timestamp :: DATE >= '2024-01-05'
{% endif %}
)

SELECT 
    block_number,
    block_timestamp,
    version,
    tx_hash,
    event_index,
    event_address,
    event_resource,
    depositor,
    amount,
    token_address,
    {{ dbt_utils.generate_surrogate_key(['tx_hash', 'event_index']) }} AS lending_aries_withdraws_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM events
