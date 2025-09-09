{% docs defi__ez_lending_borrows %}

## Description
This table tracks all lending borrows across multiple DeFi lending protocols on Aptos, combining data from Echelon and Echo protocols. It captures when users borrow assets from lending markets using their deposited collateral, including complete transaction context, token metadata, and USD pricing information.

## Key Use Cases
- Lending protocol borrowing analysis and leverage tracking
- DeFi risk assessment and collateralization ratio monitoring
- User borrowing behavior analysis and credit risk evaluation
- Protocol lending capacity and utilization metrics
- Cross-protocol borrowing pattern analysis

## Important Relationships
- Utilizes token price data from `price.ez_prices_hourly` for USD calculations
- References token metadata from `core.dim_tokens` for decimal precision
- Complements `defi.ez_lending_deposits` and `defi.ez_lending_repayments` for complete lending activity analysis

## Commonly-used Fields
- `tx_hash`: Essential for linking to transaction details and verification
- `borrower`: Core field for user behavior analysis and wallet tracking
- `token_address` and `token_symbol`: Key for filtering by specific tokens and DeFi analysis
- `amount_raw` and `amount_usd`: Critical for value calculations and financial analysis
- `platform`: Important for protocol comparison and multi-protocol analysis
- `block_timestamp`: Primary field for time-series analysis and trend detection

{% enddocs %}
