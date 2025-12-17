{% docs balances__ez_balances %}

## Description
This table provides token balances for all addresses holding verified fungible assets on the Aptos blockchain. It combines a historical snapshot with recent balance changes to provide comprehensive balance data. Each row represents a unique address-token combination, with decimal-adjusted balances and USD valuations using end-of-day token prices.

**Data Structure:** The table unions:
- Historical balances from a point-in-time snapshot (configurable via `SNAPSHOT_DATE` variable, default: 2025-09-01)
- Recent balance changes that occurred after the snapshot date

## Key Use Cases
- Portfolio tracking and balance analysis
- Wallet wealth distribution and concentration metrics
- Token holder analysis and whale tracking
- DeFi TVL calculations and protocol health monitoring
- Point-in-time balance queries

## Important Relationships
- Sources historical data from `silver.balances_snapshot` for the snapshot date
- Sources recent data from `silver.balances` for post-snapshot changes
- Joins to `core.dim_tokens` for token metadata (symbol, name, decimals)
- Joins to `price.ez_prices_hourly` for end-of-day USD price valuations
- Can be joined with `core.dim_labels` for address labeling and entity identification

## Commonly-used Fields
- `balance_date`: The date of the balance record (snapshot date or block date)
- `address`: Core field for wallet-level analysis and filtering
- `token_address`: Essential for token-specific balance queries and aggregations
- `balance`: Decimal-adjusted balance for human-readable amounts
- `balance_usd`: Critical for portfolio valuation and cross-token comparisons
- `last_balance_change`: Timestamp of the most recent balance modification

{% enddocs %}
