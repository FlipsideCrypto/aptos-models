{% docs defi__ez_lending_withdraws %}

## Description
This table tracks all lending withdrawals across multiple DeFi lending protocols on Aptos, combining data from Echelon and Echo protocols. It captures when users withdraw their previously deposited collateral or liquidity from lending markets, including complete transaction context, token metadata, and USD pricing information.

## Key Use Cases
- Lending protocol withdrawal analysis and user behavior tracking
- DeFi liquidity flow monitoring and protocol comparison
- Risk assessment and collateral management analysis
- User journey analysis from deposit to withdrawal patterns
- Protocol performance metrics and TVL calculations

## Important Relationships
- Utilizes token price data from `price.ez_prices_hourly` for USD calculations
- References token metadata from `core.dim_tokens` for decimal precision
- Complements `defi.ez_lending_deposits` for complete lending activity analysis

## Commonly-used Fields
- `tx_hash`: Essential for linking to transaction details and verification
- `withdrawer`: Core field for user behavior analysis and wallet tracking
- `token_address` and `token_symbol`: Key for filtering by specific tokens and DeFi analysis
- `amount_raw` and `amount_usd`: Critical for value calculations and financial analysis
- `platform`: Important for protocol comparison and multi-protocol analysis
- `block_timestamp`: Primary field for time-series analysis and trend detection

{% enddocs %}
