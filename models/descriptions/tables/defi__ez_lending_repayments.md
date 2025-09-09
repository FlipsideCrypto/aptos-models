{% docs defi__ez_lending_repayments %}

## Description
This table tracks all lending repayments across multiple DeFi lending protocols on Aptos, combining data from Echelon and Echo protocols. It captures when users repay their borrowed assets to lending markets, including complete transaction context, token metadata, and USD pricing information. This includes both self-repayments and third-party repayments.

## Key Use Cases
- Lending protocol repayment analysis and debt management tracking
- DeFi user behavior analysis and repayment pattern identification
- Protocol health monitoring and bad debt risk assessment
- Cross-protocol repayment efficiency analysis
- Borrower creditworthiness and repayment behavior evaluation

## Important Relationships
- Utilizes token price data from `price.ez_prices_hourly` for USD calculations
- References token metadata from `core.dim_tokens` for decimal precision
- Complements `defi.ez_lending_borrows` and `defi.ez_lending_liquidations` for complete lending activity analysis

## Commonly-used Fields
- `tx_hash`: Essential for linking to transaction details and verification
- `payer` and `borrower`: Core fields for user behavior analysis and wallet tracking
- `token_address` and `token_symbol`: Key for filtering by specific tokens and DeFi analysis
- `amount_raw` and `amount_usd`: Critical for value calculations and financial analysis
- `platform`: Important for protocol comparison and multi-protocol analysis
- `block_timestamp`: Primary field for time-series analysis and trend detection

{% enddocs %}
