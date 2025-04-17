{{ config(
  materialized = 'incremental',
  unique_key = ['tx_hash','event_index','block_timestamp::DATE'],
  incremental_strategy = 'merge',
  merge_exclude_columns = ["inserted_timestamp"],
  cluster_by = ['block_timestamp::DATE','modified_timestamp::DATE'],
  tags = ['core','full_test']
) }}

WITH events AS (

  SELECT
    block_number,
    version,
    success,
    block_timestamp,
    tx_hash,
    event_index,
    event_resource,
    event_data :amount :: bigint AS amount,
    event_data :store :: STRING AS store_address,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
  FROM
    {{ ref('silver__events') }}
  WHERE
    event_module = 'fungible_asset'
    AND event_resource IN (
      'WithdrawEvent',
      'DepositEvent',
      'Withdraw',
      'Deposit'
    )
    AND block_timestamp :: DATE BETWEEN '2025-04-10'
    AND '2025-04-11'

{% if is_incremental() %}
AND modified_timestamp >= (
  SELECT
    MAX(modified_timestamp)
  FROM
    {{ this }}
)
{% endif %}
),
owners AS (
  SELECT
    version,
    block_timestamp,
    store_address,
    owner_address
  FROM
    {{ ref('silver__fungiblestore_owners') }}
),
md AS (
  SELECT
    store_address,
    metadata_address
  FROM
    {{ ref('silver__fungiblestore_metadata') }}
)
SELECT
  e.block_number,
  e.block_timestamp,
  e.tx_hash,
  e.version,
  e.success,
  e.event_index,
  e.event_resource AS transfer_event,
  e.account_address,
  e.store_address,
  o.owner_address,
  m.metadata_address,
  e.amount,
  {{ dbt_utils.generate_surrogate_key(
    ['e.tx_hash','e.event_index']
  ) }} AS transfers_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
  events e asof
  JOIN owners o match_condition(
    e.version >= o.version
  )
  ON e.store_address = o.store_address
  LEFT JOIN md m
  ON e.store_address = m.store_address
