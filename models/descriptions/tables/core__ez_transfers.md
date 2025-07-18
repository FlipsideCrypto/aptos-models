{% docs core__ez_transfers %}

## Description
This table provides an enhanced, user-friendly version of token transfers on the Aptos blockchain with business logic applied for analytics. It builds upon `core.fact_transfers` by adding decimal conversion for proper token amounts, USD pricing for financial analysis, token symbols for readability, and verification status for data quality assessment. The table automatically handles both legacy coin transfers and newer fungible asset transfers, providing a unified view of all token movements with standardized formatting and pricing information.

## Key Use Cases
- Financial analysis with USD-denominated transfer values and proper decimal handling
- Token flow analysis with human-readable symbols and verified token identification
- DeFi protocol analytics requiring accurate token amounts and pricing data
- Cross-chain bridge monitoring with standardized transfer information
- Whale movement tracking with USD value context for large transfers
- Token distribution studies with proper decimal-adjusted amounts and pricing

## Important Relationships
- Sources data from `core.fact_transfers` and applies business logic transformations
- Enriches token information by joining with `core.dim_tokens` for decimals and symbols
- Adds pricing data through `price.ez_prices_hourly` for USD value calculations
- Supports native transfer analysis in `core.ez_native_transfers` with enhanced data
- Provides foundation for DeFi and NFT analytics that require decimal-adjusted amounts

## Commonly-used Fields
- `amount`: Decimal-adjusted token amount for accurate financial calculations
- `amount_usd`: USD value of transfers for financial analysis and value tracking
- `symbol`: Human-readable token symbol for easy identification and filtering
- `token_is_verified`: Quality indicator for data reliability and token authenticity
- `tx_hash`: Essential for linking to transaction details and verification
- `account_address`: Core field for identifying transfer participants and flow analysis
- `block_timestamp`: Primary field for time-series analysis and trend detection

{% enddocs %}

 