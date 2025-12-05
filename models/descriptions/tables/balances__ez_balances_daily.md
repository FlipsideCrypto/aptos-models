{% docs balances__ez_balances_daily %}

## Description
This table provides daily end-of-day token balances for all addresses holding verified fungible assets on the Aptos blockchain. Each row represents a unique address-token combination for a specific date, with balances forward-filled from the last known balance change. The table includes decimal-adjusted balances and USD valuations using end-of-day token prices.

**Data Retention:** Daily records are retained for the most recent 95 days. For data older than 95 days, only weekly snapshots (Sundays) are preserved to optimize storage while maintaining historical trend analysis capabilities.

## Key Use Cases
- Portfolio tracking and historical balance analysis
- Wallet wealth distribution and concentration metrics
- Token holder analysis and whale tracking over time
- DeFi TVL calculations and protocol health monitoring
- Time-series analysis of address holdings

## Important Relationships
- Sources balance data from `silver.bals_daily` which tracks daily balance snapshots
- Joins to `core.dim_tokens` for token metadata (symbol, name, decimals)
- Joins to `price.ez_prices_hourly` for end-of-day USD price valuations
- Can be joined with `core.dim_labels` for address labeling and entity identification

## Commonly-used Fields
- `balance_date`: Primary field for time-series analysis and point-in-time balance queries
- `address`: Core field for wallet-level analysis and filtering
- `token_address`: Essential for token-specific balance queries and aggregations
- `balance`: Decimal-adjusted balance for human-readable amounts
- `balance_usd`: Critical for portfolio valuation and cross-token comparisons
- `balance_changed_on_date`: Useful for identifying active vs. stale balances

{% enddocs %}
