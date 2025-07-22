{% docs amount_out_unadj %}

The non-decimal adjusted amount of the outbound token for the swap.

**Data type:** Integer
**Example:**
- 1500000 (for 1.5 tokens with 6 decimals)
- 1000000000000000000 (for 1 token with 18 decimals)

**Business Context:**
- Essential for reconstructing the exact on-chain value of swap outputs.
- Used for technical audits, protocol analytics, and downstream decimal adjustment.
- Enables accurate calculation of swap volumes and liquidity flows.

{% enddocs %}