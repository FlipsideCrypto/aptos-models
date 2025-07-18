{% docs core__fact_transactions_state_checkpoint %}

## Description
This table contains StateCheckpoint transactions from the Aptos blockchain, which are special system transactions appended at the end of each block to serve as checkpoint milestones. These transactions provide critical state verification data including state checkpoint hashes, accumulator root hashes, and event root hashes that enable the blockchain to maintain data integrity and support efficient state synchronization. StateCheckpoint transactions are essential for the blockchain's consensus mechanism and enable nodes to verify the integrity of the blockchain state at regular intervals.

## Key Use Cases
- Blockchain state integrity verification and checkpoint analysis
- State synchronization monitoring and node health assessment
- Consensus mechanism analysis and state verification tracking
- Block finality analysis and state checkpoint milestone monitoring
- Network security analysis and state tampering detection
- State recovery and synchronization efficiency analysis

## Important Relationships
- Provides state verification context for transaction analysis in `core.fact_transactions`
- Links to block data in `core.fact_blocks` for comprehensive block analysis
- Supports consensus analysis and state integrity verification
- Enables checkpoint-based analysis across all core models
- Provides infrastructure context for network security monitoring

## Commonly-used Fields
- `block_number`: Primary identifier for linking to block-level analysis
- `state_checkpoint_hash`: Essential for state integrity verification and checkpoint analysis
- `accumulator_root_hash`: Critical for state accumulator verification and data integrity
- `event_root_hash`: Important for event tree verification and event integrity
- `vm_status`: Transaction execution status for checkpoint health monitoring
- `block_timestamp`: Primary field for time-series analysis of checkpoint activity

{% enddocs %}
