{% docs change_type %}

The type of state change that occurred during transaction execution, categorizing how the blockchain state was modified.

**Data type:** String
**Example:**
- write_resource (created or updated a resource)
- delete_resource (deleted a resource)
- write_module (deployed or updated a module)
- write_table_item (created or updated a table item)
- delete_table_item (deleted a table item)

**Business Context:**
- Essential for understanding the nature and impact of state modifications.
- Critical for resource lifecycle analysis and state transition tracking.
- Enables debugging and verification of transaction effects on blockchain state.

{% enddocs %}