{% docs tx_type %}

The type of transaction executed on the Aptos blockchain, categorizing transactions by their purpose and origin.

**Data type:** String
**Example:**
- user_transaction (regular user-initiated transactions)
- block_metadata_transaction (system transactions for block metadata)
- state_checkpoint_transaction (system transactions for state checkpoints)

**Business Context:**
- Essential for filtering and categorizing different types of blockchain activity.
- Critical for separating user activity from system operations in analytics.
- Enables focused analysis on specific transaction categories and use cases.

{% enddocs %}