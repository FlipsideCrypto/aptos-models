{{ config(
    materialized = 'incremental',
    unique_key = "lending_echelon_repayments_id",
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
        event_data:repayer_addr::string AS payer,
        event_data:borrower_addr::string AS borrower,
        event_data:amount::number AS amount,
        -- Market object inner address points to the specific lending market
        event_data:market_obj:inner::string AS lending_market,
        _inserted_timestamp
    FROM {{ ref('silver__events') }}
    WHERE event_address = '0xc6bc659f1649553c1a3fa05d9727433dc03843baac29473c817d06d39e7621ba'
        AND event_module = 'lending' 
        AND event_resource = 'RepayEvent'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% else %}
    AND block_timestamp :: DATE >= '2024-03-16'
{% endif %}

),

changes AS (
    SELECT 
        block_number,
        block_timestamp,
        version,
        tx_hash,
        success,
        payload_function,
        change_data,
        change_type,
        address,
        inner_change_type,
        change_address,
        change_module,
        change_resource,
        CASE 
            WHEN change_resource = 'FungibleAssetInfo' THEN change_data:metadata:inner::string
            WHEN change_resource = 'CoinInfo' THEN change_data:type_name::string
        END AS token_address,
        _inserted_timestamp
    FROM {{ ref('silver__changes') }}
    WHERE change_address = '0xc6bc659f1649553c1a3fa05d9727433dc03843baac29473c817d06d39e7621ba'
        AND change_module = 'lending'
        AND change_resource IN ('FungibleAssetInfo', 'CoinInfo')
        AND tx_hash IN (SELECT DISTINCT tx_hash FROM events)
{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% else %}
    AND block_timestamp :: DATE >= '2024-03-16'
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
    A.payer,
    A.borrower,
    A.amount,
    A.lending_market,
    B.token_address,
    {{ dbt_utils.generate_surrogate_key(['A.tx_hash', 'A.event_index']) }} AS lending_echelon_repayments_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    A._inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM events A
LEFT JOIN changes B
    ON A.tx_hash = B.tx_hash
    AND A.lending_market = B.address