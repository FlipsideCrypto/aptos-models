{% docs defi__fact_dex_swaps %}

## Description
This table records all raw on-chain DEX swap events on Aptos for protocols including animeswap, auxexchange, batswap, cellana, cetus, hippo, liquidswap, pancake, sushi, thala, tsunami, hyperion and tapp exchange. It provides the unadjusted, protocol-level swap data, including token addresses, amounts, and swapper addresses, without any decimal adjustment, token symbol enrichment, or price attribution.

## Key Use Cases
- Analyzing DEX trading activity and protocol market share
- Building custom swap aggregations and liquidity analytics
- Auditing raw swap flows for compliance or research
- Powering dashboards for DEX volume, user activity, and token flows
- Supporting downstream enrichment models for DeFi analytics

## Important Relationships
- Serves as the source for enriched swap data in `defi.ez_dex_swaps`
- Token metadata is joined from `core.dim_tokens` for symbol/decimals
- Can be linked to price data in `price.ez_prices_hourly` for USD values
- Related to bridge activity in `defi.fact_bridge_activity` for cross-protocol analysis

## Commonly-used Fields
- `tx_hash`, `event_index`: Unique identifiers for each swap event
- `platform`: DEX protocol name
- `token_in`, `token_out`: Token addresses for swap input/output
- `amount_in_unadj`, `amount_out_unadj`: Raw, non-decimal-adjusted swap amounts
- `swapper`: Address of the user executing the swap
- `block_timestamp`: Time of the swap event

{% enddocs %}