{% docs sequence_number %}

The sequence number for an account indicates the number of transactions that have been submitted and committed on-chain from that account, incremented with each executed or aborted transaction.

**Data type:** Integer
**Example:**
- 0 (first transaction from account)
- 10 (11th transaction from account)
- 100 (101st transaction from account)

**Business Context:**
- Essential for transaction ordering and account activity tracking.
- Critical for preventing replay attacks and ensuring transaction uniqueness.
- Enables account-based analytics and transaction sequence analysis.

{% enddocs %}