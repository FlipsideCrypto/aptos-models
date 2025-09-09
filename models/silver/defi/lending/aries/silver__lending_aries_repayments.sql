{{ config(
    materialized = 'incremental',
    unique_key = "lending_aries_repayments_id",
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
        event_data:receiver::string AS borrower, -- The person whose debt is being repaid
        CASE 
            WHEN event_data:repay_amount::number > 0 THEN event_data:repay_amount::number
            WHEN event_data:deposit_amount::number > 0 THEN event_data:deposit_amount::number
            ELSE 0
        END AS amount,
        SUBSTRING(
            event_type, 
            POSITION('<' IN event_type) + 1,
            POSITION('>' IN event_type) - POSITION('<' IN event_type) - 1
        ) AS token_address,
        _inserted_timestamp
    FROM {{ ref('silver__events') }} 
    WHERE event_address = '0x9770fa9c725cbd97eb50b2be5f7416efdfd1f1554beb0750d4dae4c64e860da3'
        AND event_module = 'controller'
        AND event_resource LIKE 'DepositRepayForEvent%'
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
),
tx_sender AS (
    SELECT 
        block_timestamp, 
        tx_hash, 
        sender
    FROM {{ ref('silver__transactions') }}
    WHERE tx_hash IN (SELECT DISTINCT tx_hash FROM events)
{% if is_incremental() %}
    AND _inserted_timestamp >= (
        SELECT MAX(_inserted_timestamp)
        FROM {{ this }}
    )
{% else %}
    AND block_timestamp::DATE >= '2024-01-05'
{% endif %}
)

SELECT 
    e.block_number,
    e.block_timestamp,
    e.version,
    e.tx_hash,
    e.event_index,
    e.event_address,
    t.sender AS payer,
    e.borrower,
    e.amount,
    e.token_address,
    e._inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['e.tx_hash', 'e.event_index']) }} AS lending_aries_repayments_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM events e
LEFT JOIN tx_sender t
    ON e.tx_hash = t.tx_hash
    AND e.block_timestamp::DATE = t.block_timestamp::DATE