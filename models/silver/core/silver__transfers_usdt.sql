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
    block_timestamp :: DATE AS block_date,
    tx_hash,
    event_index,
    event_resource,
    event_data :amount :: bigint AS amount,
    account_address,
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
),
chnges AS (
  SELECT
    block_timestamp :: DATE AS block_date,
    tx_hash,
    change_index,
    change_data:metadata:inner::string AS token_address
  FROM
    {{ ref('silver__changes') }}
  WHERE
    change_module = 'fungible_asset'
    AND change_data:metadata:inner::string = '0x357b0b74bc833e95a115ad22604854d6b0fca151cecd94111770e5d6ffc9dc2b'

{% if is_incremental() %}
AND _inserted_timestamp >= (
  SELECT
    MAX(_inserted_timestamp)
  FROM
    {{ this }}
)
{% endif %}
qualify(ROW_NUMBER() over(PARTITION BY tx_hash ORDER BY change_index DESC) = 1)
)

SELECT
  e.block_number,
  e.block_timestamp,
  e.tx_hash,
  e.version,
  e.success,
  e.event_index,
  e.creation_number,
  e.event_resource AS transfer_event,
  e.account_address,
  e.amount,
  c.token_address,
  {{ dbt_utils.generate_surrogate_key(
    ['e.tx_hash','e.event_index']
  ) }} AS transfers_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  e._inserted_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
  events e
  INNER JOIN chnges c
  ON e.block_date = c.block_date
  AND e.tx_hash = c.tx_hash
WHERE 
  e.event_resource IN ('DepositEvent', 'Deposit','WithdrawEvent', 'Withdraw')