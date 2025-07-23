{% docs nft__ez_nft_sales %}

## Description
This table provides enriched NFT sales data for Aptos, combining raw on-chain sale events with token metadata (symbols, decimals) and price information. It includes decimal-adjusted prices, token symbols, and USD values, making it suitable for analytics and reporting.

## Key Use Cases
- Analyzing NFT sales activity and marketplace trends in USD
- Building dashboards for NFT sales volume, user activity, and project performance
- Comparing sales activity across projects, platforms, and tokens
- Supporting NFT research, monitoring, and reporting
- Powering downstream models for NFT aggregations and analytics

## Important Relationships
- Sources raw sales data from `nft.fact_nft_sales`
- Joins token metadata from `core.dim_tokens` for symbol/decimals
- Joins price data from `price.ez_prices_hourly` for USD values
- Can be related to NFT mint data in `nft.ez_nft_mints` for lifecycle analysis

## Commonly-used Fields
- `tx_hash`, `event_index`: Unique identifiers for each sale event
- `nft_address`, `tokenid`: NFT contract and token identifiers
- `seller_address`, `buyer_address`: Addresses involved in the sale
- `block_timestamp`: Time of the sale event
- `project_name`, `platform_name`: Names of the NFT project and marketplace
- `total_price`, `total_price_usd`: Decimal-adjusted and USD value of sale price
- `platform_fee`, `creator_fee`, `total_fees`: Fee amounts in both raw and USD terms

{% enddocs %} 