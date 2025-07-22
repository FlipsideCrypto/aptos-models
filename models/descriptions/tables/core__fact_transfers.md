{% docs core__fact_transfers %}

## Description
This table tracks all token transfers on the Aptos blockchain, capturing movements of both native tokens and fungible assets between accounts. The data combines two transfer mechanisms: the legacy coin transfer system (coin module) and the newer fungible asset module. The table includes Deposit and Withdraw events from both modules, with transfers having zero amounts excluded. Each transfer record contains complete transaction context, event metadata, and token information for comprehensive transfer analysis.

## Key Use Cases
- Token flow analysis and wallet tracking across the Aptos network
- DeFi protocol volume measurements and liquidity flow monitoring
- Cross-chain bridge activity tracking and asset movement analysis
- Whale movement detection and large transfer alerts
- Token distribution analysis and holder behavior studies
- Network activity monitoring and transfer pattern recognition

## Important Relationships
- Serves as the foundation for enhanced transfer analysis in `core.ez_transfers` which adds decimal conversion, USD pricing, and token symbols
- Links to transaction details in `core.fact_transactions` via `tx_hash` for complete transaction context
- Connects to token metadata in `core.dim_tokens` via `token_address` for token information
- Supports native transfer analysis in `core.ez_native_transfers` which applies specific filtering logic
- Provides event context for `core.fact_events` analysis

## Commonly-used Fields
- `tx_hash`: Essential for linking to transaction details and verification
- `account_address`: Core field for identifying transfer participants and flow analysis
- `amount`: Critical for value calculations and financial analysis (raw amount before decimal adjustment)
- `token_address`: Key for filtering by specific tokens and DeFi analysis
- `block_timestamp`: Primary field for time-series analysis and trend detection
- `transfer_event`: Important for understanding the type of transfer (Deposit/Withdraw)
- `is_fungible`: Distinguishes between legacy coin transfers and newer fungible asset transfers

{% enddocs %}
