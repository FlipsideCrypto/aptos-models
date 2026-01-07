{{ config(
    materialized = 'incremental',
    unique_key = "dex_swaps_animeswap_id",
    incremental_strategy = 'merge',
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::DATE"],
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
        {{ ref(
            'silver__transactions'
        ) }}
    WHERE
        success

{% if is_incremental() %}
AND block_timestamp :: DATE >= '{{min_bd}}'
{% endif %}
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
        event_address = '0x16fe2df00ea7dde4a63409201f7f4e536bde7bb7335526a35d05111e68aa322c'
        AND event_resource ILIKE 'SwapEvent%'
        AND success

{% if is_incremental() %}
AND block_timestamp :: DATE >= '{{min_bd}}'
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    version,
    tx_hash,
    event_index,
    event_address,
    b.sender AS swapper,
    CASE
        WHEN A.event_data :amount_x_in :: INT = 0 THEN TRIM(SPLIT_PART(SPLIT(A.event_type, ',') [1], '>', 1) :: STRING, ' ')
        WHEN A.event_data :amount_x_in :: INT != 0 THEN TRIM(SPLIT_PART(SPLIT(A.event_type, ',') [0], '<', 2) :: STRING, ' ')
    END AS token_in,
    CASE
        WHEN A.event_data :amount_y_in :: INT = 0 THEN TRIM(SPLIT_PART(SPLIT(A.event_type, ',') [1], '>', 1) :: STRING, ' ')
        WHEN A.event_data :amount_y_in :: INT != 0 THEN TRIM(SPLIT_PART(SPLIT(A.event_type, ',') [0], '<', 2) :: STRING, ' ')
    END AS token_out,
    CASE
        WHEN A.event_data: amount_x_in :: INT = 0 THEN A.event_data: amount_y_in :: INT
        ELSE A.event_data: amount_x_in :: INT
    END AS amount_in_unadj,
    CASE
        WHEN A.event_data: amount_y_out :: INT = 0 THEN A.event_data: amount_x_out :: INT
        ELSE A.event_data: amount_y_out :: INT
    END AS amount_out_unadj,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','event_index']
    ) }} AS dex_swaps_animeswap_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    evnts A
    JOIN tx b USING(
        tx_hash,
        block_timestamp
    )

{% if is_incremental() %}
WHERE
    GREATEST(
        A.modified_timestamp,
        b.modified_timestamp
    ) >= '{{max_mod}}'
{% endif %}
