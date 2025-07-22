{% docs nft_creator_fee_raw %}

The non-decimal adjusted amount of creator royalty fees paid for this NFT event in the transaction's currency.

**Data type:** Integer
**Example:**
- 100000000 (for 1 unit with 8 decimals)
- 500000000000000000 (for 0.5 unit at 18 decimals)

**Business Context:**
- Used for reconstructing the exact on-chain value of creator fees.
- Enables technical audits, protocol analytics, and downstream decimal adjustment.
- Supports accurate calculation of creator fee volumes and value flows.

{% enddocs %}