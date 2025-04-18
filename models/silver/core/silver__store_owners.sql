{{ config(
  materialized = 'incremental',
  unique_key = ['tx_hash','change_index'],
  incremental_strategy = 'merge',
  merge_exclude_columns = ["inserted_timestamp"],
  tags = ['core','full_test','fungible_assets']
) }}


-- This model tracks ownership of fungible stores over time
-- Each row represents a change in store ownership
SELECT
  block_timestamp,
  block_timestamp::DATE AS block_date,
  block_number,
  {{ dbt_utils.generate_surrogate_key( ['tx_hash'] ) }} AS transactions_id,
  change_index,
  address AS store_address,
  change_data:owner::STRING AS owner_address,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  '{{ invocation_id }}' AS _invocation_id
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
  ROW_NUMBER() OVER (PARTITION BY address, block_timestamp ORDER BY change_index DESC) = 1