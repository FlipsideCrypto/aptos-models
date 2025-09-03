{% docs defi__ez_lending_liquidations %}

## Description
This table tracks all lending liquidations across multiple DeFi lending protocols on Aptos, combining data from Echelon and Echo protocols. It captures when borrowers' positions are liquidated due to insufficient collateralization, including complete transaction context, both collateral and debt token information, and USD pricing calculations.

## Key Use Cases
- Lending protocol liquidation analysis and risk monitoring
- DeFi market stress testing and systemic risk assessment
- Liquidator behavior analysis and profit tracking
- Borrower risk profile analysis and collateralization monitoring
- Protocol liquidation efficiency and market impact analysis

## Important Relationships
- Utilizes token price data from `price.ez_prices_hourly` for USD calculations of both collateral and debt tokens
- References token metadata from `core.dim_tokens` for decimal precision
- Complements `defi.ez_lending_borrows` and `defi.ez_lending_repayments` for complete lending risk analysis

## Commonly-used Fields
- `tx_hash`: Essential for linking to transaction details and verification
- `liquidator` and `borrower`: Core fields for user behavior analysis and wallet tracking
- `collateral_token` and `debt_token`: Key for understanding liquidation mechanics and token relationships
- `collateral_token_symbol` and `debt_token_symbol`: Important for token identification and analysis
- `amount_raw` and `amount_usd`: Critical for value calculations and liquidation impact analysis
- `platform`: Important for protocol comparison and multi-protocol risk analysis
- `block_timestamp`: Primary field for time-series analysis and market stress detection

{% enddocs %}
