{{ config(
    materialized = 'incremental',
    unique_key = "dex_swaps_hyperfluid_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['modified_timestamp::DATE'],
    tags = ['noncore']
) }}

{% if execute %}

{% if is_incremental() %}
{% set max_mod_query %}

SELECT
    MAX(modified_timestamp) modified_timestamp
FROM
    {{ this }}

    {% endset %}
    {% set max_mod = run_query(max_mod_query) [0] [0] %}
    {% if not max_mod or max_mod == 'None' %}
        {% set max_mod = '2099-01-01' %}
    {% endif %}

    {% set min_block_date_query %}
SELECT
    MIN(
        block_timestamp :: DATE
    )
FROM
    (
        SELECT
            MIN(block_timestamp) block_timestamp
        FROM
            {{ ref('silver__transactions') }} A
        WHERE
            modified_timestamp >= '{{max_mod}}'
        UNION ALL
        SELECT
            MIN(block_timestamp) block_timestamp
        FROM
            {{ ref('silver__events') }} A
        WHERE
            modified_timestamp >= '{{max_mod}}'
    ) {% endset %}
    {% set min_bd = run_query(min_block_date_query) [0] [0] %}
    {% if not min_bd or min_bd == 'None' %}
        {% set min_bd = '2099-01-01' %}
    {% endif %}
{% endif %}
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
AND block_timestamp :: DATE >= '{{min_bd}}'
{% endif %}
),
events AS (
    SELECT
        block_number,
        block_timestamp,
        version,
        tx_hash,
        event_index,
        event_module,
        event_type,
        event_address,
        event_resource,
        event_data,
        modified_timestamp
    FROM
        {{ ref('silver__events') }}
    WHERE
        event_address = '0x8b4a2c4bb53857c718a04c020b98f8c2e1f99a68b0f57389a8bf5434cd22e05c'
        AND event_resource like 'SwapEvent%'
        AND event_module = 'pool_v3'
        AND success

{% if is_incremental() %}
AND block_timestamp :: DATE >= '{{min_bd}}'
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
        COALESCE(GREATEST(e.modified_timestamp, tx.modified_timestamp),  e.modified_timestamp, tx.modified_timestamp) as modified_timestamp
    FROM 
        tx
        JOIN events e USING(
            tx_hash, block_timestamp
        )
{% if is_incremental() %}
WHERE GREATEST(
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
        event_type,
        event_address,
        sender AS swapper,
        event_data:amount_in :: INT AS amount_in_unadj,
        event_data:amount_out :: INT AS amount_out_unadj,
        event_data:from_token:inner :: STRING AS token_in,
        event_data:to_token:inner :: STRING AS token_out,
        modified_timestamp
    FROM
        joined

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY tx_hash, event_index
        ORDER BY
            modified_timestamp DESC
    ) = 1
)
SELECT
    block_number,
    block_timestamp,
    version,
    tx_hash,
    event_index,
    event_address,
    swapper,
    token_in,
    token_out,
    amount_in_unadj,
    amount_out_unadj,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'event_index']
    )}} AS dex_swaps_hyperfluid_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    parsed


    
