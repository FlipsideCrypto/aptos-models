{% docs event_type %}

The full three-part descriptive type of an event, consisting of the event_address, event_module, and event_resource identifiers.

**Data type:** String
**Example:**
- 0x1::coin::DepositEvent
- 0x1::coin::WithdrawEvent
- 0xf22bede237a07e121b56d91a491eb7bcdfd1f5907926a9e58338f964a01b17fa::coin::DepositEvent

**Business Context:**
- Essential for categorizing and filtering events by their type and source.
- Critical for DeFi protocol analysis and event-driven analytics.
- Enables pattern recognition and event correlation across different contracts.

{% enddocs %}