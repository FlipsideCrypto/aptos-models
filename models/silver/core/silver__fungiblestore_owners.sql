{{ config(
  materialized = 'incremental',
  unique_key = ['tx_hash','change_index'],
  incremental_strategy = 'merge',
  cluster_by = ['block_timestamp::DATE'],
  post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(store_address);",
  tags = ['core','full_test']
) }}

WITH changes_source AS (
  SELECT
    block_timestamp,
    block_number,
    version,
    tx_hash,
    change_index,
    address AS store_address,
    change_data :owner :: STRING AS owner_address
  FROM
    {{ ref('silver__changes') }}
  WHERE
    change_address = '0x1'
    AND change_module = 'object'
    AND change_resource = 'ObjectCore'
  {% if is_incremental() %}
  AND modified_timestamp >= (
    SELECT
      MAX(modified_timestamp)
    FROM
      {{ this }}
  )
  {% endif %}
),

-- Capture temporary fungible store owners from deletion events
-- Use negative indices to avoid collision with change_index values
deletion_events_source AS (
  SELECT
    block_timestamp,
    block_number,
    version,
    tx_hash,
    -1 * (event_index + 1) AS change_index,
    event_data :store :: STRING AS store_address,
    event_data :owner :: STRING AS owner_address
  FROM
    {{ ref('silver__events') }}
  WHERE
    event_address = '0x1'
    AND event_module = 'fungible_asset'
    AND event_resource = 'FungibleStoreDeletion'
  {% if is_incremental() %}
  AND modified_timestamp >= (
    SELECT
      MAX(modified_timestamp)
    FROM
      {{ this }}
  )
  {% endif %}
),

combined AS (
  SELECT * FROM changes_source
  UNION ALL
  SELECT * FROM deletion_events_source
)

SELECT
  block_timestamp,
  block_number,
  version,
  tx_hash,
  change_index,
  store_address,
  owner_address,
  {{ dbt_utils.generate_surrogate_key(
    ['tx_hash','change_index']
  ) }} AS fungiblestore_owners_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
  combined
