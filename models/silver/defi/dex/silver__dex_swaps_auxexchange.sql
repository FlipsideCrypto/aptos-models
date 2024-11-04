{{ config(
    materialized = 'incremental',
    unique_key = "dex_swaps_aux_exchange_id",
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
        event_address = '0xbd35135844473187163ca197ca93b2ab014370587bb0ed3befff9e902d6bb541'
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
    event_data: in_coin_type :: STRING AS token_in,
    event_data: out_coin_type :: STRING AS token_out,
    event_data :in_au :: INT AS amount_in_unadj,
    event_data :out_au :: INT AS amount_out_unadj,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','event_index']
    ) }} AS dex_swaps_aux_exchange_id,
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
