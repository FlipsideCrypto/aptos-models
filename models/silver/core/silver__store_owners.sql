{{ config(
  materialized = 'incremental',
  unique_key = ['tx_hash','change_index'],
  incremental_strategy = 'merge',
  tags = ['core','full_test']
) }}

SELECT
  block_timestamp,
  block_number,
  version,
  tx_hash,
  change_index,
  change_data :owner :: STRING AS owner_address,
  address store_address
FROM
  {{ ref('silver__changes') }}
WHERE
  success
  AND change_address = '0x1'
  AND change_module = 'object'
  AND change_resource = 'ObjectCore'
  AND block_timestamp :: DATE > CURRENT_DATE - 14

{% if is_incremental() %}
AND _inserted_timestamp >= (
  SELECT
    MAX(_inserted_timestamp)
  FROM
    {{ this }}
)
{% endif %}
