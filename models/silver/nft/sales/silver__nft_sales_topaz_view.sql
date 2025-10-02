{{ config(
  materialized = 'view'
) }}

SELECT
    block_timestamp,
    block_number,
    version,
    tx_hash,
    event_index,
    event_type,
    seller_address,
    buyer_address,
    nft_address,
    token_version,
    platform_address,
    project_name,
    tokenid,
    platform_name,
    platform_exchange_version,
    total_price_raw,
    creator_fee_raw,
    platform_fee_raw,
    total_fees_raw,
    nft_sales_topaz_id,
    inserted_timestamp,
    modified_timestamp,
    _inserted_timestamp,
    _invocation_id
FROM
  {{ source(
    'aptos_silver',
    'nft_sales_topaz'
  ) }}
