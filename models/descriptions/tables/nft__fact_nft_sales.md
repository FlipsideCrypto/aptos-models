{% docs nft__fact_nft_sales %}

## Description
This table records all raw on-chain NFT sale events on Aptos, capturing sales across supported marketplaces and protocols. It includes seller, buyer, NFT identifiers, project/platform names, and raw price/fee data, without enrichment or price attribution.

## Key Use Cases
- Analyzing NFT sales activity and marketplace trends
- Auditing raw NFT sale events for compliance or research
- Building dashboards for NFT sales volume, user activity, and project performance
- Supporting downstream enrichment models for NFT analytics
- Monitoring NFT project adoption and sales trends

## Important Relationships
- Serves as the source for enriched NFT sales data in `nft.ez_nft_sales`
- Can be joined with token metadata from `core.dim_tokens` for symbol/decimals
- Can be linked to price data in `price.ez_prices_hourly` for USD values
- Related to NFT mint data in `nft.fact_nft_mints` for lifecycle analysis

## Commonly-used Fields
- `tx_hash`, `event_index`: Unique identifiers for each sale event
- `nft_address`, `tokenid`: NFT contract and token identifiers
- `seller_address`, `buyer_address`: Addresses involved in the sale
- `block_timestamp`: Time of the sale event
- `project_name`, `platform_name`: Names of the NFT project and marketplace
- `total_price_raw`, `platform_fee_raw`, `creator_fee_raw`: Raw, non-decimal-adjusted price and fee amounts

{% enddocs %} 