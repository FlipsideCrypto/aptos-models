{% docs nft__fact_nft_mints %}

## Description
This table records all raw on-chain NFT mint events on Aptos, capturing the creation of new NFTs across supported marketplaces and protocols. It includes the addresses involved, NFT identifiers, project names, and raw price data, without enrichment or price attribution.

## Key Use Cases
- Analyzing NFT minting activity and project launches
- Auditing raw NFT mint events for compliance or research
- Building dashboards for NFT mint volume, user activity, and project trends
- Supporting downstream enrichment models for NFT analytics
- Monitoring NFT project adoption and minting trends

## Important Relationships
- Serves as the source for enriched NFT mint data in `nft.ez_nft_mints`
- Can be joined with token metadata from `core.dim_tokens` for symbol/decimals
- Can be linked to price data in `price.ez_prices_hourly` for USD values
- Related to NFT sales data in `nft.fact_nft_sales` for lifecycle analysis

## Commonly-used Fields
- `tx_hash`, `event_index`: Unique identifiers for each mint event
- `nft_address`, `tokenid`: NFT contract and token identifiers
- `nft_from_address`, `nft_to_address`: Addresses involved in the mint
- `block_timestamp`: Time of the mint event
- `project_name`: Name of the NFT project
- `total_price_raw`: Raw, non-decimal-adjusted mint price

{% enddocs %} 