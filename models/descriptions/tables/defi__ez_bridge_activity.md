{% docs defi__ez_bridge_activity %}

## Description
This table provides enriched bridge activity data for Aptos, combining raw on-chain bridge events from multiple protocols with token metadata (symbols, decimals) and price information. It includes decimal-adjusted amounts, token symbols, and USD values, making it suitable for analytics and reporting.

## Key Use Cases
- Analyzing cross-chain bridge flows and protocol usage in USD
- Building dashboards for bridge volume, user activity, and token flows
- Comparing bridge activity across protocols and tokens
- Supporting DeFi and cross-chain research, monitoring, and reporting
- Powering downstream models for DeFi and bridge aggregations

## Important Relationships
- Sources raw bridge data from `defi.fact_bridge_activity`
- Joins token metadata from `core.dim_tokens` for symbol/decimals
- Joins price data from `price.ez_prices_hourly` for USD values
- Can be related to DEX swap data in `defi.ez_dex_swaps` for DeFi flow analysis

## Commonly-used Fields
- `tx_hash`, `event_index`: Unique identifiers for each bridge event
- `platform`: Bridge protocol name
- `symbol`: Token symbol being bridged
- `amount`, `amount_usd`: Decimal-adjusted and USD value of bridge amount
- `sender`, `receiver`: Addresses involved in the bridge event
- `block_timestamp`: Time of the bridge event
- `source_chain`, `destination_chain`: Names of source and destination chains

{% enddocs %}