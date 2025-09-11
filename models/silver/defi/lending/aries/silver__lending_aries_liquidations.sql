{{ config(
    materialized = 'incremental',
    unique_key = "lending_aries_liquidations_id",
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
        event_data:liquidator::string AS liquidator,
        event_data:liquidatee::string AS borrower,
        event_data:repay_amount::number AS amount,
        -- Parse the two tokens from event_type
        -- Format: LiquidateEvent<debt_token, collateral_token>
        SPLIT_PART(
            SUBSTRING(
                event_type, 
                POSITION('<' IN event_type) + 1,
                POSITION('>' IN event_type) - POSITION('<' IN event_type) - 1
            ), 
            ', ', 
            1
        ) AS debt_token,
        SPLIT_PART(
            SUBSTRING(
                event_type, 
                POSITION('<' IN event_type) + 1,
                POSITION('>' IN event_type) - POSITION('<' IN event_type) - 1
            ), 
            ', ', 
            2
        ) AS collateral_token,
        _inserted_timestamp
    FROM {{ ref('silver__events') }}
    WHERE event_address = '0x9770fa9c725cbd97eb50b2be5f7416efdfd1f1554beb0750d4dae4c64e860da3'
        AND event_module = 'controller'
        AND event_resource LIKE 'LiquidateEvent%'

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
    liquidator,
    borrower,
    amount,
    debt_token,
    collateral_token,
    {{ dbt_utils.generate_surrogate_key(['tx_hash', 'event_index']) }} AS lending_aries_liquidations_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM events