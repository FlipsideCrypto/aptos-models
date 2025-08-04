{{ config(
  materialized = 'view'
) }}

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
  token_inoken_address,
  transfers_id,
  inserted_timestamp,
  modified_timestamp,
  _inserted_timestamp,
  _invocation_id
FROM
  {{ source(
    'silver',
    'transfers'
  ) }}
  e
