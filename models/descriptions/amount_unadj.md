{% docs amount_unadj %}

The non-decimal adjusted amount of a token, representing the raw on-chain value before decimal precision is applied.

**Data type:** Decimal
**Example:**
- 1500000000000000000 (for 1.5 tokens with 18 decimals)
- 1000000000000000000000 (for 1000 tokens with 18 decimals)

**Business Context:**
- Preserves the exact on-chain representation for precise calculations and verification.
- Essential for blockchain-level accuracy and cross-reference with external data sources.
- Used when decimal precision needs to be maintained for technical analysis.

{% enddocs %}
