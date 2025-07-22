{% docs payload %}

The data payload carried by a transaction, containing the specific instructions and parameters for the transaction execution.

**Data type:** String (JSON)
**Example:**
- {"function":"0x1::coin::transfer","type_arguments":["0x1::aptos_coin::AptosCoin"],"arguments":["0x123...","1000000"]}

**Business Context:**
- Essential for understanding the specific actions and parameters of transactions.
- Critical for transaction analysis and smart contract interaction tracking.
- Enables detailed transaction parsing and function call analysis.

{% enddocs %}