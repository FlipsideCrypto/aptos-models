{{ config(
  materialized = 'incremental',
  unique_key = ['store_address'],
  incremental_strategy = 'merge',
  tags = ['core', 'fungible_assets', 'usdt']
) }}

-- This model identifies fungible stores that contain USDT tokens
-- Uses direct filtering with QUALIFY for better performance

SELECT
    block_timestamp,
    block_timestamp::DATE AS block_date,
    block_number,
    {{ dbt_utils.generate_surrogate_key( ['tx_hash'] ) }} AS transactions_id,
    address AS store_address,
    change_data:metadata:inner::STRING AS metadata_address,
    {{ dbt_utils.generate_surrogate_key( ['tx_hash'] ) }} AS transactions_id,
    
    -- Flag USDT stores for easy filtering
    CASE 
      WHEN change_data:metadata:inner::STRING = '0x357b0b74bc833e95a115ad22604854d6b0fca151cecd94111770e5d6ffc9dc2b' 
      THEN TRUE 
      ELSE FALSE 
    END AS is_usdt,
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
    AND is_usdt = TRUE  -- Only include USDT stores