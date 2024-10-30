{{ config(
    materialized = 'incremental',
    unique_key = 'ez_bridge_activity_id',
    incremental_strategy = 'merge',
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::DATE"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash,version,tx_sender, sender, receiver);",
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'BRIDGE' }} },
    tags = ['noncore']
) }}

SELECT
    block_number,
    block_timestamp,
    version,
    tx_hash,
    platform,
    bridge_address,
    event_name,
    direction,
    tx_sender,
    sender,
    receiver,
    source_chain_name AS soruce_chain,
    destination_chain_name AS destination_chain,
    A.token_address,
    COALESCE(
        t.symbol,
        p.symbol
    ) AS symbol,
    amount_unadj,
    CASE
        WHEN COALESCE(
            t.decimals,
            p.decimals
        ) IS NOT NULL THEN amount_unadj / pow(10, COALESCE(t.decimals, p.decimals))
    END AS amount,
    amount * p.price AS amount_in_usd,
    event_index,
    fact_bridge_activity_id AS ez_bridge_activity_id,
    A.inserted_timestamp,
    A.modified_timestamp
FROM
    {{ ref('defi__fact_bridge_activity') }} A
    LEFT JOIN {{ ref('core__dim_tokens') }}
    t
    ON LOWER(
        A.token_address
    ) = LOWER(
        t.token_address
    )
    LEFT JOIN {{ ref('price__ez_prices_hourly') }}
    p
    ON LOWER(
        A.token_address
    ) = LOWER(
        p.token_address
    )
    AND DATE_TRUNC(
        'hour',
        block_timestamp
    ) = p.hour

{% if is_incremental() %}
WHERE
    GREATEST(
        A.modified_timestamp,
        COALESCE(
            t.modified_timestamp,
            '2000-01-01'
        ),
        COALESCE(
            p.modified_timestamp,
            '2000-01-01'
        )
    ) >= (
        SELECT
            MAX(
                modified_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}
