{{ config(
    materialized = 'view',
    tags = ['noncore']
) }}

{% set models = [
    ('a', ref('silver__bridge_celer_transfers')),
    ('a', ref('silver__bridge_layerzero_transfers')),
    ('a', ref('silver__bridge_mover_transfers_view')),
    ('a', ref('silver__bridge_wormhole_transfers'))
]
 %}

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
    source_chain_id,
    source_chain_name,
    destination_chain_id,
    destination_chain_name,
    token_address,
    amount_unadj,
    event_index,
    bridge_celer_transfers_id AS bridge_combined_id,
    inserted_timestamp,
    modified_timestamp,
    _invocation_id
FROM
    ({% for models in models %}
    SELECT
     *
    FROM
        {{ models [1] }}

        {% if not loop.last %}

UNION ALL
{% endif %}
{% endfor %})