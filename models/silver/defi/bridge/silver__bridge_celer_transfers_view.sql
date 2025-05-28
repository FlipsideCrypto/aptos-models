{{ config(
  materialized = 'view'
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
    source_chain_id,
    source_chain_name,
    destination_chain_id,
    destination_chain_name,
    token_address,
    amount_unadj,
    event_index,
    bridge_celer_transfers_id,
    inserted_timestamp,
    modified_timestamp,
    _inserted_timestamp,
    _invocation_id
FROM
    {{ source(
        'aptos_silver',
        'bridge_celer_transfers'
    ) }}