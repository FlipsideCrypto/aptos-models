{% docs nft__ez_nft_mints %}

## Description
This table provides enriched NFT mint data for Aptos, combining raw on-chain mint events with token metadata (symbols, decimals) and price information. It includes decimal-adjusted prices, token symbols, and USD values, making it suitable for analytics and reporting.

## Key Use Cases
- Analyzing NFT minting activity and project launches in USD
- Building dashboards for NFT mint volume, user activity, and project trends
- Comparing mint activity across projects and tokens
- Supporting NFT research, monitoring, and reporting
- Powering downstream models for NFT aggregations and analytics

## Important Relationships
- Sources raw mint data from `nft.fact_nft_mints`
- Joins token metadata from `core.dim_tokens` for symbol/decimals
- Joins price data from `price.ez_prices_hourly` for USD values
- Can be related to NFT sales data in `nft.ez_nft_sales` for lifecycle analysis

## Commonly-used Fields
- `tx_hash`, `event_index`: Unique identifiers for each mint event
- `nft_address`, `tokenid`: NFT contract and token identifiers
- `nft_from_address`, `nft_to_address`: Addresses involved in the mint
- `block_timestamp`: Time of the mint event
- `project_name`: Name of the NFT project
- `total_price`, `total_price_usd`: Decimal-adjusted and USD value of mint price

{% enddocs %} 