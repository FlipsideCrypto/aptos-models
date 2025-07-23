{% docs nft_currency_address %}

The contract address of the currency used for the NFT event (mint or sale).

**Data type:** String
**Example:**
- 0x1::aptos_coin::AptosCoin
- 0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890

**Business Context:**
- Used for identifying the payment currency for NFT events.
- Enables currency-level analytics, filtering, and reporting.
- Supports joining with token metadata and price information.

{% enddocs %}