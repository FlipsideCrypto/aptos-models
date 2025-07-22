{% docs token_address %}

The full address of the token on the Aptos blockchain, containing the account, module, and resource identifiers.

**Data type:** String
**Example:**
- 0x1::coin::AptosCoin (native APT token)
- 0xf22bede237a07e121b56d91a491eb7bcdfd1f5907926a9e58338f964a01b17fa::coin::USDC

**Business Context:**
- Primary identifier for filtering and grouping transactions by specific tokens.
- Essential for DeFi analysis, token flow tracking, and protocol-specific analytics.
- Enables correlation with token metadata for symbol and decimal information.

{% enddocs %}