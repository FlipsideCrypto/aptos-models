{% docs change_index %}

Unique identifier for a state change within a transaction, representing the sequential order of state modifications during transaction execution.

**Data type:** Integer
**Example:**
- 0 (first change in transaction)
- 1 (second change in transaction)
- 3 (fourth change in transaction)

**Business Context:**
- Essential for determining the chronological order of state changes within a transaction.
- Critical for state transition analysis and transaction effect tracking.
- Enables precise debugging and verification of transaction impact on blockchain state.

{% enddocs %}