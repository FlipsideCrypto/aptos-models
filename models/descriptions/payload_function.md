{% docs payload_function %}

The specific function being called within the transaction payload, identifying the smart contract method to be executed.

**Data type:** String
**Example:**
- 0x1::coin::transfer
- 0x1::coin::register
- 0xf22bede237a07e121b56d91a491eb7bcdfd1f5907926a9e58338f964a01b17fa::coin::mint

**Business Context:**
- Essential for categorizing transactions by function type and smart contract interaction.
- Critical for DeFi protocol analysis and function call pattern recognition.
- Enables transaction filtering and specific function usage analytics.

{% enddocs %}