{{ config(
  materialized = 'incremental',
  unique_key = ['store_address'],
  incremental_strategy = 'merge',
  tags = ['core', 'full_test', 'fungible_assets'],
  cluster_by = ['block_timestamp::DATE','modified_timestamp::DATE'],
  merge_exclude_columns = ["inserted_timestamp"],
) }}

-- This model identifies fungible stores that contain USDT tokens
-- Uses direct filtering with QUALIFY for better performance

SELECT
    block_timestamp,
    block_timestamp::DATE AS block_date,
    block_number,
    {{ dbt_utils.generate_surrogate_key( ['tx_hash'] ) }} AS transactions_id,
    address AS store_address,
    change_index,
    change_data:metadata:inner::STRING AS metadata_address,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('silver__changes') }}
WHERE
    change_module = 'fungible_asset'
    AND change_resource = 'FungibleStore'

    {% if is_incremental() %}
    AND _inserted_timestamp >= (
      SELECT
        MAX(_inserted_timestamp)
      FROM
        {{ this }}
    )
    {% endif %}
QUALIFY 
    ROW_NUMBER() OVER (PARTITION BY address ORDER BY block_number DESC) = 1
