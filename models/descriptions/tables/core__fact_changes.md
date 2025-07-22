{% docs core__fact_changes %}

## Description
This table contains flattened state changes from Aptos blockchain transactions, providing a comprehensive view of all modifications to the blockchain state. Each change represents a discrete modification to the global state, including resource modifications, account balance changes, module deployments, and other state transitions. The table captures the granular details of how transactions affect the blockchain state, with each change having a unique index within its parent transaction. This enables detailed analysis of state transitions and their impact on the network.

## Key Use Cases
- State change analysis and transaction impact assessment
- Resource modification tracking and state transition monitoring
- Account balance change analysis and financial state tracking
- Module deployment and upgrade monitoring
- State key analysis and storage pattern identification
- Transaction effect analysis and state mutation tracking

## Important Relationships
- Provides detailed state change context for transaction analysis in `core.fact_transactions`
- Links to transaction metadata for complete transaction effect analysis
- Supports resource analysis and state modification tracking
- Enables granular state transition analysis across all core models
- Provides foundation for advanced state analysis and debugging

## Commonly-used Fields
- `tx_hash`: Essential for linking changes to their parent transactions
- `change_index`: Unique identifier for ordering changes within a transaction
- `change_type`: Critical for categorizing and filtering different types of state changes
- `address`: Primary field for identifying which account's state was modified
- `change_module` and `change_resource`: Important for understanding what was modified
- `key` and `value`: Essential for analyzing the specific data that was changed
- `block_timestamp`: Primary field for time-series analysis of state changes

{% enddocs %}
