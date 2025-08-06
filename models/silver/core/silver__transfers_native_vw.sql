{{ config(
  materialized = 'view'
) }}

SELECT
  block_number,
  block_timestamp,
  tx_hash,
  version,
  success,
  from_address,
  to_address,
  amount,
  token_address,
  _transfer_key,
  transfers_native_id,
  inserted_timestamp,
  modified_timestamp,
  _inserted_timestamp,
  _invocation_id
FROM
  {{ source(
    'silver',
    'transfers_native'
  ) }}
