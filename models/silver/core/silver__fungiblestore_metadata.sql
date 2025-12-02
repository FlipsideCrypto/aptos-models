{{ config(
  materialized = 'incremental',
  unique_key = ['store_address'],
  incremental_strategy = 'merge',
  merge_exclude_columns = ["inserted_timestamp","block_timestamp_first","block_number_first"],
  post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(store_address);",
  tags = ['core', 'full_test']
) }}

WITH changes_source AS (
  SELECT
    block_timestamp,
    block_number,
    address AS store_address,
    change_data :metadata :inner :: STRING AS metadata_address
  FROM
    {{ ref('silver__changes') }}
  WHERE
    change_module = 'fungible_asset'
    AND change_resource = 'FungibleStore'
  {% if is_incremental() %}
  AND modified_timestamp >= (
    SELECT
      MAX(modified_timestamp)
    FROM
      {{ this }}
  )
  {% endif %}
),

-- Capture temporary fungible stores from deletion events
deletion_events_source AS (
  SELECT
    block_timestamp,
    block_number,
    event_data :store :: STRING AS store_address,
    event_data :metadata :: STRING AS metadata_address
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
  block_timestamp AS block_timestamp_first,
  block_number AS block_number_first,
  store_address,
  metadata_address,
  CASE
    WHEN metadata_address = '0x357b0b74bc833e95a115ad22604854d6b0fca151cecd94111770e5d6ffc9dc2b' THEN TRUE
    ELSE FALSE
  END AS is_usdt,
  {{ dbt_utils.generate_surrogate_key(
    ['store_address']
  ) }} AS fungiblestore_metadata_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
  combined
qualify ROW_NUMBER() over (
  PARTITION BY store_address
  ORDER BY
    block_number
) = 1
