{{ config(
    materialized = 'incremental',
    unique_key = "dex_swaps_thala_v2_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['modified_timestamp::DATE']
) }}

{% if execute %}

{% if is_incremental() %}
{% set max_mod_query %}

SELECT 
    COALESCE(MAX(modified_timestamp), '1970-01-01'::timestamp_ntz) as max_timestamp
FROM 
    {{ this }}

{% endset %}
{% set max_mod = run_query(max_mod_query).columns[0].values()[0] %}
{% endif %}

WITH tx AS (
    SELECT
        tx_hash,
        block_timestamp,
        sender,
        modified_timestamp
    FROM
        {{ ref('silver__transactions') }}
    WHERE
        success

{% if is_incremental() %}
AND modified_timestamp >= '{{max_mod}}'
{% endif %}
),
events AS (
    SELECT
        block_number,
        block_timestamp,
        version,
        tx_hash,
        event_index,
        event_type,
        event_address,
        event_resource,
        event_data,
        modified_timestamp
    FROM
        {{ ref('silver__events') }}
    WHERE
        success
        AND event_address = '0x7730cd28ee1cdc9e999336cbc430f99e7c44397c0aa77516f6f23a78559bb5'
        AND event_resource LIKE 'SwapEvent%'

{% if is_incremental() %}
AND modified_timestamp >= '{{max_mod}}'
{% endif %}
), 
joined AS (
    SELECT
        tx.tx_hash,
        tx.block_timestamp,
        tx.sender,
        e.block_number,
        e.version,
        e.event_index,
        e.event_type,
        e.event_address,
        e.event_resource,
        e.event_data,
        GREATEST(e.)
    FROM 
        tx
        JOIN events e USING(
            tx_hash, block_timestamp
        )
{% if is_incremental() %}
WHERE
    GREATEST(
        tx.modified_timestamp,
        e.modified_timestamp
    ) >= '{{max_mod}}'
{% endif %}
),
parsed AS (
    SELECT
        block_number,
        block_timestamp,
        version,
        tx_hash,
        event_index,
        event_address,
        sender AS swapper,
        event_data: idx_in :: INT AS idx_in,
        event_data: idx_out :: INT AS idx_out,
        
    FROM
        joined
)
