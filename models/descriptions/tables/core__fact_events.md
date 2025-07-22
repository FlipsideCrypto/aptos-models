{% docs core__fact_events %}

## Description
This table contains flattened events from Aptos blockchain transactions, providing a comprehensive view of all events emitted during transaction execution. Events are the primary mechanism for smart contracts to communicate state changes and important occurrences to external observers. Each event represents a discrete occurrence within a transaction, such as token transfers, contract interactions, or state modifications, with a unique index within its parent transaction. This table enables detailed analysis of blockchain activity and smart contract behavior.

## Key Use Cases
- Event-driven analytics and smart contract interaction monitoring
- Token transfer event analysis and flow tracking
- DeFi protocol event monitoring and activity analysis
- Contract interaction pattern recognition and behavior analysis
- Event-based alerting and real-time monitoring
- Smart contract debugging and event emission analysis

## Important Relationships
- Provides event context for transaction analysis in `core.fact_transactions`
- Links to transfer data in `core.fact_transfers` and `core.ez_transfers` for comprehensive flow analysis
- Supports state change analysis in `core.fact_changes` with event correlation
- Enables event-driven analytics across all core models
- Provides foundation for DeFi and NFT event analysis

## Commonly-used Fields
- `tx_hash`: Essential for linking events to their parent transactions
- `event_index`: Unique identifier for ordering events within a transaction
- `event_type`: Critical for categorizing and filtering different types of events
- `event_address`: Primary field for identifying the contract that emitted the event
- `event_module` and `event_resource`: Important for understanding the event source
- `event_data`: Essential for analyzing the specific event payload and parameters
- `account_address`: Key for identifying the account associated with the event
- `block_timestamp`: Primary field for time-series analysis of events

{% enddocs %}
