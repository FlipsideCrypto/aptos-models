{% docs is_fungible %}

Boolean indicating whether the transfer was conducted using the legacy coin transfer mechanism (simpler, original method) or the fungible_asset module (newer, more flexible system for managing fungible assets).

**Data type:** Boolean
**Example:**
- true (uses fungible_asset module)
- false (uses legacy coin transfer mechanism)

**Business Context:**
- Essential for understanding transfer mechanism evolution and compatibility.
- Critical for protocol analysis and transfer method categorization.
- Enables mechanism-based analytics and transfer type reporting.

{% enddocs %} 