{{ config(
    materialized = 'incremental',
    unique_key = ['tx_hash','event_index'],
    incremental_strategy = 'merge',
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::DATE"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash,version,swapper);",
    tags = ['noncore']
) }}

{% if execute %}

{% if is_incremental() %}
{% set query %}
CREATE
OR REPLACE temporary TABLE core.dex_swaps__mod_intermediate_tmp AS

SELECT
    platform,
    MAX(modified_timestamp) modified_timestamp
FROM
    {{ this }}
GROUP BY
    platform {% endset %}
    {% do run_query(
        query
    ) %}
    {% set min_block_date_query %}
SELECT
    MIN(block_timestamp)
FROM
    {{ ref('silver__dex_swaps_combined') }} A
    LEFT JOIN core.dex_swaps__mod_intermediate_tmp b
    ON A.platform = b.platform
WHERE
    A.platform <> 'thala'
    AND (
        A.modified_timestamp >= b.modified_timestamp
        OR b.modified_timestamp IS NULL
    ) {% endset %}
    {% set min_bd = run_query(min_block_date_query) [0] [0] %}
    {% if not min_bd or min_bd == 'None' %}
        {% set min_bd = '2099-01-01' %}
    {% endif %}
{% endif %}
{% endif %}

WITH base AS (
    SELECT
        A.block_number,
        A.block_timestamp,
        A.version,
        A.tx_hash,
        A.event_index,
        A.platform,
        A.event_address,
        A.swapper,
        A.token_in,
        A.token_out,
        A.amount_in_unadj,
        A.amount_out_unadj,
        A.dex_swaps_combined_id,
        MAX(
            CASE
                WHEN A.platform = 'hippo' THEN TRUE
                ELSE FALSE
            END
        ) over(
            PARTITION BY A.version,
            A.amount_in_unadj,
            A.amount_out_unadj,
            A.token_in,
            A.token_out
        ) AS has_hippo,
        COUNT(1) over(
            PARTITION BY A.version,
            A.amount_in_unadj,
            A.amount_out_unadj,
            A.token_in,
            A.token_out
        ) AS dupe_count
    FROM
        {{ ref('silver__dex_swaps_combined') }} A

{% if is_incremental() %}
WHERE
    block_timestamp :: DATE >= '{{min_bd}}'
{% endif %}
)
SELECT
    A.block_number,
    A.block_timestamp,
    A.version,
    A.tx_hash,
    A.event_index,
    A.platform,
    A.event_address,
    A.swapper,
    A.token_in,
    A.token_out,
    A.amount_in_unadj,
    A.amount_out_unadj,
    A.dex_swaps_combined_id AS fact_dex_swaps_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    base A
WHERE
    dupe_count = 1
    OR has_hippo = FALSE
    OR (
        has_hippo
        AND dupe_count > 1
        AND platform <> 'hippo'
    )
