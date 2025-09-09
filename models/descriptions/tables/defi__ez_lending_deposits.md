{% docs defi__ez_lending_deposits %}

## Description
This table tracks all lending deposits across multiple DeFi lending protocols on Aptos, combining data from Echelon and Echo protocols. It captures when users deposit assets into lending markets to earn interest or provide liquidity, including complete transaction context, token metadata, and USD pricing information.

## Key Use Cases
- Lending protocol deposit analysis and TVL tracking
- DeFi user behavior analysis and deposit pattern identification
- Protocol performance metrics and yield farming analysis
- Cross-protocol deposit flow monitoring and comparison
- User journey analysis and deposit-to-withdrawal lifecycle tracking

## Important Relationships
- Utilizes token price data from `price.ez_prices_hourly` for USD calculations
- References token metadata from `core.dim_tokens` for decimal precision
- Complements `defi.ez_lending_withdraws`, `defi.ez_lending_borrows`, and `defi.ez_lending_repayments` for complete lending activity analysis

## Commonly-used Fields
- `tx_hash`: Essential for linking to transaction details and verification
- `depositor`: Core field for user behavior analysis and wallet tracking
- `token_address` and `token_symbol`: Key for filtering by specific tokens and DeFi analysis
- `amount_raw` and `amount_usd`: Critical for value calculations and financial analysis
- `platform`: Important for protocol comparison and multi-protocol analysis
- `block_timestamp`: Primary field for time-series analysis and trend detection

{% enddocs %}
