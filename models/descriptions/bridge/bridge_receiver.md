{% docs bridge_receiver %}

The designated address set to receive the bridged tokens on the target chain after the completion of the bridge transaction. For non-evm chains, the hex address is decoded/encoded to match the data format of the destination chain, where possible.

**Data type:** String
**Example:**
- 0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef

**Business Context:**
- Used for tracking bridge recipients and cross-chain asset flows.
- Enables user-level analytics, bridge destination patterns, and receiver behavior analysis.
- Supports linking to address labels and user profiles across chains.

{% enddocs %} 