{% docs core__fact_transactions_block_metadata %}

## Description
This table contains BlockMetadata transactions from the Aptos blockchain, which are special system transactions inserted at the beginning of each block to provide essential block-level information. These transactions include consensus metadata such as proposer information, epoch details, round numbers, and validator voting data. BlockMetadata transactions also serve as epoch boundary markers, triggering reward distribution to validators when an epoch ends. The table provides critical infrastructure data for understanding Aptos's consensus mechanism and block production process.

## Key Use Cases
- Consensus mechanism analysis and validator performance tracking
- Epoch boundary detection and reward distribution monitoring
- Block proposer analysis and validator rotation tracking
- Network governance and consensus participation analysis
- Block production rate and network efficiency monitoring
- Validator voting pattern analysis and consensus health assessment

## Important Relationships
- Provides block-level context for transaction analysis in `core.fact_transactions`
- Links to block data in `core.fact_blocks` for comprehensive block analysis
- Supports consensus analysis and validator performance tracking
- Enables epoch-based analysis across all core models
- Provides infrastructure context for network health monitoring

## Commonly-used Fields
- `block_number`: Primary identifier for linking to block-level analysis
- `proposer`: Essential for validator performance and rotation analysis
- `epoch`: Critical for epoch-based analysis and reward distribution tracking
- `round`: Important for consensus round analysis and block production timing
- `vm_status`: Transaction execution status for consensus health monitoring
- `block_timestamp`: Primary field for time-series analysis of consensus activity

{% enddocs %}
