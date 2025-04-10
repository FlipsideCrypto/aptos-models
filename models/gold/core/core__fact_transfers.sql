{{ config(
    materialized = 'incremental',
    unique_key = ['tx_hash','event_index','block_timestamp::DATE'],
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE','modified_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash, version, account_address,token_address);",
    tags = ['core','full_test']
) }}

WITH silver_transfers AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        version,
        success,
        event_index,
        creation_number,
        transfer_event,
        account_address,
        amount,
        token_address,
        transfers_id AS fact_transfers_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('silver__transfers') }}
    WHERE
        amount <> 0
        {% if is_incremental() %}
        AND modified_timestamp >= (
            SELECT
                MAX(modified_timestamp)
            FROM
                {{ this }}
        )
        {% endif %}
),

silver_transfers_usdt AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        version,
        success,
        event_index,
        creation_number,
        transfer_event,
        account_address,
        amount,
        token_address,
        transfers_id AS fact_transfers_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('silver__transfers_usdt') }}
    WHERE
        amount <> 0
        {% if is_incremental() %}
        AND modified_timestamp >= (
            SELECT
                MAX(modified_timestamp)
            FROM
                {{ this }}
        )
        {% endif %}
),

combined_transfers AS (

    SELECT * FROM silver_transfers
    
    UNION ALL
    
    SELECT * FROM silver_transfers_usdt
)    

SELECT * FROM combined_transfers