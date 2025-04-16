{{ config(
  materialized = 'incremental',
  unique_key = ['store_address'],
  incremental_strategy = 'merge',
  tags = ['core', 'usdt', 'ownership']
) }}

-- This model joins USDT fungible stores with their owners
-- Connects object ownership with USDT tokens for comprehensive tracking

WITH store_owners AS (
  SELECT
    block_timestamp,
    block_number,
    tx_hash,
    store_address,
    owner_address
  FROM
    {{ ref('silver__store_owners') }}
),

fungible_stores AS (
  SELECT
    block_timestamp,
    block_number,
    tx_hash,
    store_address,
    metadata_address,
    is_usdt
  FROM
    {{ ref('silver__fungiblestore_usdt') }}
  WHERE
    is_usdt = TRUE
)

SELECT
  o.block_timestamp AS ownership_timestamp,
  o.block_timestamp::DATE AS ownership_date,
  o.block_number AS ownership_block,
  o.tx_hash AS ownership_tx_hash,  -- Transaction that last changed ownership
  o.store_address,
  o.owner_address,
  f.metadata_address,
  f.block_number AS store_metadata_block,
  f.tx_hash AS store_creation_tx_hash,  -- Transaction that created/updated the store
  f.is_usdt,
  CURRENT_TIMESTAMP() AS _inserted_timestamp
FROM
  store_owners o
JOIN
  fungible_stores f ON o.store_address = f.store_address
{% if is_incremental() %}
WHERE
  o.block_timestamp >= (
    SELECT
      MAX(ownership_timestamp)
    FROM
      {{ this }}
  )
{% endif %}