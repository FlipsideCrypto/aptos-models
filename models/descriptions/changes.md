{% docs changes %}

The state changes that were executed by the transaction, representing modifications to the blockchain's global state.

**Data type:** String (JSON)
**Example:**
- [{"type":"write_resource","address":"0x123...","data":{"coin":{"value":"1000000"}}}]

**Business Context:**
- Essential for understanding the impact and effects of transactions on blockchain state.
- Critical for state transition analysis and transaction effect tracking.
- Enables comprehensive transaction analysis and state modification monitoring.

{% enddocs %}