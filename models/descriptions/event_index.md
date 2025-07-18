{% docs event_index %}

Unique identifier for an event within a transaction, representing the sequential order of events emitted during transaction execution.

**Data type:** Integer
**Example:**
- 0 (first event in transaction)
- 1 (second event in transaction)
- 5 (sixth event in transaction)

**Business Context:**
- Essential for determining the chronological order of events within a transaction.
- Critical for event correlation and transaction flow analysis.
- Enables precise event sequencing and debugging of complex transactions.

{% enddocs %}