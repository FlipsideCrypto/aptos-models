-- Description: 
-- This view exposes transfer event data from the silver.transfers table, including block and transaction metadata, 
-- transfer details, and audit fields. It is intended for downstream consumption and simplifies access to transfer events.

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
  token_address,
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
