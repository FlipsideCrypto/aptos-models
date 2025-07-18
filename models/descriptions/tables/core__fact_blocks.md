{% docs core__fact_blocks %}

## Description
This table contains block-level data for the Aptos blockchain, mapping the fundamental block structure that groups transactions for execution. The Aptos blockchain uses blocks for batching and executing transactions, where each block contains a range of transaction versions. A transaction at height 0 is the first transaction (genesis transaction), and a transaction at height 100 is the 101st transaction in the transaction store. This table provides the essential metadata for each block including timestamps, hash values, and transaction counts.

## Key Use Cases
- Block-level trend analysis and transaction volume monitoring over time
- Network performance analysis and block production rate calculations
- Transaction throughput analysis and network capacity planning
- Block time analysis and network efficiency metrics
- Historical block data for network health monitoring and anomaly detection

## Important Relationships
- Serves as the foundation for transaction-level analysis in `core.fact_transactions`
- Provides block context for transfer events in `core.fact_transfers` and `core.ez_transfers`
- Links to block metadata in `core.fact_transactions_block_metadata` for enhanced block analysis
- Supports event analysis in `core.fact_events` by providing block-level context

## Commonly-used Fields
- `block_number`: Primary identifier for ordering and filtering blocks chronologically
- `block_timestamp`: Essential for time-series analysis and temporal filtering of blockchain activity
- `tx_count`: Critical for measuring transaction throughput and network activity levels
- `block_hash`: Important for block verification and linking to external block explorers
- `first_version` and `last_version`: Key for understanding the transaction range contained within each block

{% enddocs %}
