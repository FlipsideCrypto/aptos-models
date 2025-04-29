{{ config(
  materialized = 'incremental',
  unique_key = ['store_address'],
  incremental_strategy = 'merge',
  merge_exclude_columns = ["inserted_timestamp","block_timestamp_first","block_number_first"],
  post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(store_address);",
  tags = ['core', 'full_test']
) }}

SELECT
  block_timestamp AS block_timestamp_first,
  block_number AS block_number_first,
  address AS store_address,
  change_data :metadata :inner :: STRING AS metadata_address,
  CASE
    WHEN change_data :metadata :inner :: STRING = '0x357b0b74bc833e95a115ad22604854d6b0fca151cecd94111770e5d6ffc9dc2b' THEN TRUE
    ELSE FALSE
  END AS is_usdt,
  {{ dbt_utils.generate_surrogate_key(
    ['store_address']
  ) }} AS fungiblestore_metadata_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  '{{ invocation_id }}' AS _invocation_id
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

qualify ROW_NUMBER() over (
  PARTITION BY address
  ORDER BY
    block_number
) = 1
