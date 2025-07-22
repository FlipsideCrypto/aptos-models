{% docs defi__fact_bridge_activity %}

## Description
This table aggregates raw on-chain bridge activity for Aptos, including deposits and transfers from protocols such as Celer, LayerZero, Mover, and Wormhole. It captures protocol-level bridge events, including token addresses, amounts, sender/receiver addresses, and chain metadata, without enrichment or price attribution.

## Key Use Cases
- Analyzing cross-chain bridge flows and protocol usage
- Auditing raw bridge events for compliance or research
- Building dashboards for bridge volume, user activity, and token flows
- Supporting downstream enrichment models for DeFi and cross-chain analytics
- Monitoring bridge protocol adoption and trends

## Important Relationships
- Serves as the source for enriched bridge data in `defi.ez_bridge_activity`
- Token metadata is joined from `core.dim_tokens` for symbol/decimals
- Can be linked to price data in `price.ez_prices_hourly` for USD values
- Related to DEX swap data in `defi.fact_dex_swaps` for DeFi flow analysis

## Commonly-used Fields
- `tx_hash`, `event_index`: Unique identifiers for each bridge event
- `platform`: Bridge protocol name
- `token_address`: Token address being bridged
- `amount_unadj`: Raw, non-decimal-adjusted bridge amount
- `sender`, `receiver`: Addresses involved in the bridge event
- `block_timestamp`: Time of the bridge event
- `source_chain_name`, `destination_chain_name`: Names of source and destination chains

{% enddocs %}