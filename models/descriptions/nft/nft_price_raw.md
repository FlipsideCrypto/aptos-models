{% docs nft_price_raw %}

The non-decimal adjusted amount of the NFT event in the currency in which the transaction occurred.

**Data type:** Integer
**Example:**
- 100000000 (for 1 NFT at 1 unit with 8 decimals)
- 500000000000000000 (for 0.5 NFT at 18 decimals)

**Business Context:**
- Used for reconstructing the exact on-chain value of NFT sales or mints.
- Enables technical audits, protocol analytics, and downstream decimal adjustment.
- Supports accurate calculation of NFT volumes and value flows.

{% enddocs %}