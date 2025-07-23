{% docs defi__ez_dex_swaps %}

## Description
This table provides enriched DEX swap data for Aptos, combining raw on-chain swap events from multiple protocols with token metadata (symbols, decimals) and price information. It includes decimal-adjusted amounts, token symbols, and USD values, making it suitable for analytics and reporting.

## Key Use Cases
- Analyzing DEX trading volumes and protocol market share in USD
- Building dashboards for token flows, swap trends, and user activity
- Comparing swap activity across protocols and tokens
- Supporting DeFi research, trading strategies, and liquidity analysis
- Powering downstream models for DeFi aggregations and reporting

## Important Relationships
- Sources raw swap data from `defi.fact_dex_swaps`
- Joins token metadata from `core.dim_tokens` for symbol/decimals
- Joins price data from `price.ez_prices_hourly` for USD values
- Can be related to bridge activity in `defi.ez_bridge_activity` for cross-protocol analysis

## Commonly-used Fields
- `tx_hash`, `event_index`: Unique identifiers for each swap event
- `platform`: DEX protocol name
- `symbol_in`, `symbol_out`: Token symbols for swap input/output
- `amount_in`, `amount_out`: Decimal-adjusted swap amounts
- `amount_in_usd`, `amount_out_usd`: USD value of swap amounts
- `swapper`: Address of the user executing the swap
- `block_timestamp`: Time of the swap event

{% enddocs %}