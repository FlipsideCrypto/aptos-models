{% docs version %}

The version number, also known as the height, represents the sequential position of a transaction in the Aptos blockchain. The first transaction has a version of 0, and each subsequent transaction increments by 1.

**Data type:** Integer
**Example:**
- 0 (genesis transaction)
- 12345678
- 98765432

**Business Context:**
- Unique identifier for ordering transactions chronologically across the entire blockchain.
- Essential for transaction sequencing and version-based analysis.
- Enables precise transaction tracking and blockchain state verification.

{% enddocs %}