{{ config(
    materialized = 'incremental',
    unique_key = "dex_swaps_thala_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['modified_timestamp::DATE'],
    tags = ['noncore']
) }}

WITH tx AS (

    SELECT
        tx_hash,
        block_timestamp,
        sender,
        modified_timestamp
    FROM
        {{ ref(
            'silver__transactions'
        ) }}
    WHERE
        success
),
evnts AS (
    SELECT
        block_number,
        block_timestamp,
        version,
        tx_hash,
        event_index,
        payload_function,
        event_address,
        event_resource,
        event_data,
        event_type,
        modified_timestamp
    FROM
        {{ ref(
            'silver__events'
        ) }}
    WHERE
        event_address IN (
            '0x48271d39d0b05bd6efca2278f22277d6fcc375504f9839fd73f74ace240861af',
            '0x6970b4878c3aea96732be3f31c2dded12d94d9455ff0c76c67d84859dce35136'
        )
        AND event_resource LIKE 'SwapEvent%'
        AND success
),
fin AS (
    SELECT
        block_number,
        block_timestamp,
        version,
        tx_hash,
        event_index,
        event_address,
        b.sender AS swapper,
        event_data :idx_in :: INT AS idx_in,
        event_data :idx_out :: INT AS idx_out,
        TRIM(SPLIT_PART(SPLIT(event_type, ',') [0], '<', 2), ' ') AS token_0,
        CASE
            WHEN event_data :idx_in :: INT > 0 THEN TRIM(SPLIT(event_type, ',') [1], ' ')
            WHEN event_data :idx_in :: INT = 0 THEN TRIM(SPLIT_PART(SPLIT(event_type, ',') [0], '<', 2), ' ')
            ELSE TRIM(SPLIT_PART(SPLIT(event_type, ',') [0], '<', 2), ' ')
        END AS token_in_old,
        CASE
            WHEN event_data :idx_out :: INT = 0 THEN TRIM(SPLIT_PART(SPLIT(event_type, ',') [0], '<', 2), ' ')
            WHEN event_data :idx_out :: INT > 0 THEN TRIM(SPLIT(event_type, ',') [1], ' ')
            ELSE TRIM(SPLIT(event_type, ',') [1], ' ')
        END AS token_out_old,
        CASE
            WHEN idx_in = 0 THEN token_0
            ELSE TRIM(SPLIT(event_type, ',') [idx_in], ' ')
        END AS token_in,
        CASE
            WHEN idx_out = 0 THEN token_0
            ELSE TRIM(SPLIT(event_type, ',') [idx_out], ' ')
        END AS token_out,
        event_data :amount_in :: INT AS amount_in_unadj,
        event_data :amount_out :: INT AS amount_out_unadj
    FROM
        evnts A
        JOIN tx b USING(
            tx_hash,
            block_timestamp
        )
    WHERE
        idx_in IS NOT NULL

{% if is_incremental() %}
AND GREATEST(
    A.modified_timestamp,
    b.modified_timestamp
) >= (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
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
        ['tx_hash','event_index']
    ) }} AS dex_swaps_thala_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    fin
