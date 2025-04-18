{{ config(
  materialized = 'incremental',
  unique_key = ['tx_hash','event_index','block_timestamp::DATE'],
  incremental_strategy = 'merge',
  merge_exclude_columns = ["inserted_timestamp"],
  cluster_by = ['block_timestamp::DATE','modified_timestamp::DATE'],
  tags = ['core','full_test','fungible_assets']
) }}

WITH events AS (
  -- Extract all fungible asset transfer events
  SELECT
    block_number,
    version,
    success,
    block_timestamp,
    block_timestamp::DATE AS block_date,
    tx_hash,
    event_index,
    event_resource,
    event_data:amount::BIGINT AS amount,
    -- Extract store address from event data if available, otherwise use account_address
    COALESCE(event_data:store::STRING, account_address) AS store_address,
    CASE
      WHEN event_resource IN ('WithdrawEvent', 'Withdraw') THEN 'Withdraw'
      WHEN event_resource IN ('DepositEvent', 'Deposit') THEN 'Deposit'
    END AS direction,
    creation_number,
    _inserted_timestamp
  FROM
    {{ ref('silver__events') }}
  WHERE
    event_module = 'fungible_asset'
    AND event_resource IN ('WithdrawEvent', 'DepositEvent', 'Withdraw', 'Deposit')

  {% if is_incremental() %}
  AND _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp)
    FROM
      {{ this }}
  )
  {% endif %}

)

SELECT
  e.block_number,
  e.block_timestamp,
  e.block_timestamp::DATE AS block_date,
  e.tx_hash,
  e.version,
  e.success,
  e.event_index,
  e.event_resource AS transfer_event,
  e.store_address,
  e.direction,
  e.amount,
  so.owner_address,
  fsm.metadata_address,
  {{ dbt_utils.generate_surrogate_key(['e.tx_hash']) }} AS transactions_id,
  {{ dbt_utils.generate_surrogate_key(['e.tx_hash','e.event_index']) }} AS transfers_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
  events e
  -- Join to get store owner at time of transfer
  LEFT JOIN {{ ref('silver__store_owners') }} so
    ON e.store_address = so.store_address
    AND e.block_timestamp >= so.block_timestamp
  -- Join to get metadata information
  LEFT JOIN {{ ref('silver__fungible_store_metadata') }} fsm
    ON e.store_address = fsm.store_address
QUALIFY 
  ROW_NUMBER() OVER (PARTITION BY e.tx_hash, e.event_index ORDER BY so.block_timestamp DESC) = 1
