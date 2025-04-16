{{ config(
  materialized = 'incremental',
  unique_key = ['tx_hash','change_index'],
  incremental_strategy = 'merge',
  tags = ['core','full_test']
) }}

SELECT
  block_timestamp,
  block_timestamp::DATE AS block_date,
  block_number,
  tx_hash,
  address AS store_address,
  change_data:owner::STRING AS owner_address,
  _inserted_timestamp
FROM
  {{ ref('silver__changes') }}
WHERE
  change_address = '0x1'
  AND change_module = 'object'
  AND change_resource = 'ObjectCore'
  {% if is_incremental() %}
  -- Use _inserted_timestamp for incremental logic
  AND _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp)
    FROM
      {{ this }}
  )
  {% endif %}
QUALIFY 
  ROW_NUMBER() OVER (PARTITION BY address ORDER BY block_number DESC) = 1