{{ config(
    materialized = 'incremental',
    unique_key = "lending_echelon_liquidations_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE', '_inserted_timestamp::DATE'],
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
        event_data:liquidator_addr::string AS liquidator,
        event_data:borrower_addr::string AS borrower,
        event_data:repay_amount::number AS amount,
        event_data:collateral_market_obj:inner::string AS collateral_lending_market,
        event_data:borrow_market_obj:inner::string AS debt_lending_market,
        _inserted_timestamp
    FROM {{ ref('silver__events') }}
    WHERE event_address = '0xc6bc659f1649553c1a3fa05d9727433dc03843baac29473c817d06d39e7621ba'
        AND event_module = 'lending' 
        AND event_resource = 'LiquidateEvent'
        
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
        tx_hash,
        address,
        CASE 
            WHEN change_resource = 'FungibleAssetInfo' THEN change_data:metadata:inner::string
            WHEN change_resource = 'CoinInfo' THEN change_data:type_name::string
        END AS token_address
    FROM {{ ref('silver__changes') }}
    WHERE change_module = 'lending'
        AND change_resource IN ('FungibleAssetInfo', 'CoinInfo')
        AND tx_hash IN (SELECT DISTINCT tx_hash FROM events)
        AND address IN (
            SELECT DISTINCT collateral_lending_market FROM events
            UNION
            SELECT DISTINCT debt_lending_market FROM events
        )
        
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
    A.liquidator,
    A.borrower,
    A.amount,
    A.collateral_lending_market,
    A.debt_lending_market,
    C1.token_address AS collateral_token,
    C2.token_address AS debt_token,
    {{ dbt_utils.generate_surrogate_key(['A.tx_hash', 'A.event_index']) }} AS lending_echelon_liquidations_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    A._inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM events A
LEFT JOIN changes C1
    ON A.tx_hash = C1.tx_hash
    AND A.collateral_lending_market = C1.address
LEFT JOIN changes C2
    ON A.tx_hash = C2.tx_hash
    AND A.debt_lending_market = C2.address