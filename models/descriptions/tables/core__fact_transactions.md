{% docs core__fact_transactions %}

## Description
This table contains comprehensive transaction-level data for the Aptos blockchain, serving as the primary fact table for all transaction analysis. Each transaction represents a discrete operation on the blockchain, including user transactions, system transactions, and consensus-related operations. The table captures complete transaction metadata including sender information, gas details, execution status, and cryptographic signatures. This table provides the foundation for understanding blockchain activity, user behavior, and network performance across all transaction types.

## Key Use Cases
- Transaction volume analysis and network activity monitoring
- User behavior analysis and transaction pattern recognition
- Gas fee analysis and network economics monitoring
- Transaction success rate analysis and failure investigation
- Sender activity tracking and wallet behavior analysis
- Network performance analysis and transaction throughput monitoring

## Important Relationships
- Serves as the foundation for all transaction-related analysis across core models
- Links to transfer data in `core.fact_transfers` and `core.ez_transfers` via `tx_hash`
- Provides context for event analysis in `core.fact_events` and state changes in `core.fact_changes`
- Supports block-level analysis in `core.fact_blocks` with transaction aggregation
- Enables comprehensive transaction effect analysis across the entire data model

## Commonly-used Fields
- `tx_hash`: Primary identifier for linking to all related transaction data
- `sender`: Essential for user behavior analysis and wallet tracking
- `success`: Critical for transaction success rate analysis and failure investigation
- `gas_used` and `gas_unit_price`: Important for gas fee analysis and network economics
- `block_timestamp`: Primary field for time-series analysis and trend detection
- `tx_type`: Key for categorizing and filtering different transaction types
- `version`: Unique transaction identifier for ordering and version tracking

{% enddocs %}
