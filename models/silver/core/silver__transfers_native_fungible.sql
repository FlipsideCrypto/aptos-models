{{ config(
  materialized = 'incremental',
  unique_key = ['tx_hash','_transfer_key','block_timestamp::DATE'],
  incremental_strategy = 'merge',
  merge_exclude_columns = ["inserted_timestamp"],
  cluster_by = ['block_timestamp::DATE'],
  tags = ['core','full_test']
) }}
{# post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash, version, from_address, to_address);", #}
WITH xfer AS (

  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    version,
    success,
    event_index,
    transfer_event,
    owner_address account_address,
    amount,
    metadata_address AS token_address
  FROM
    {{ ref('silver__transfers_fungible') }}
  WHERE
    amount > 0
    AND metadata_address = '0xa'

{% if is_incremental() %}
AND modified_timestamp >= (
  SELECT
    MAX(modified_timestamp)
  FROM
    {{ this }}
)
{% endif %}
),
wth AS (
  SELECT
    *
  FROM
    xfer
  WHERE
    transfer_event = 'WithdrawEvent'
),
dep AS (
  SELECT
    *
  FROM
    xfer
  WHERE
    transfer_event = 'DepositEvent'
)
SELECT
  wth.block_number,
  wth.block_timestamp,
  wth.tx_hash,
  wth.version,
  wth.success,
  wth.account_address AS from_address,
  dep.account_address AS to_address,
  wth.amount,
  wth.token_address,
  wth.event_index || ':' || dep.event_index AS _transfer_key,
  {{ dbt_utils.generate_surrogate_key(
    ['wth.tx_hash','wth.event_index','dep.event_index']
  ) }} AS transfers_native_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
  wth
  JOIN dep
  ON wth.tx_hash = dep.tx_hash
  AND wth.amount = dep.amount
  AND wth.event_index + 1 = dep.event_index
WHERE
  wth.account_address <> dep.account_address
