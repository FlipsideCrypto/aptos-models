{% docs core__ez_native_transfers %}

## Description
This table provides a simplified, flattened view of native token transfers on the Aptos blockchain, specifically focusing on APT token movements between accounts. The table applies specific business logic to identify genuine transfer pairs by requiring withdrawal events to occur immediately before deposit events with matching amounts, with exceptions for intermediate "CoinRegisterEvent" occurrences. This filtering ensures that only actual token transfers are captured, excluding other blockchain events that might appear as transfers but represent different operations.

## Key Use Cases
- Native APT token flow analysis and tracking across the network
- Wallet-to-wallet transfer monitoring and pattern recognition
- Network fee analysis and gas cost tracking for APT transfers
- User behavior analysis for native token movements
- Transfer volume analysis and network activity monitoring
- Simplified transfer analysis without complex event parsing

## Important Relationships
- Complements the broader transfer analysis available in `core.fact_transfers` and `core.ez_transfers`
- Provides simplified transfer data for analytics that don't require complex event parsing
- Supports native token-specific analysis separate from other token types
- Enables straightforward sender-to-receiver transfer tracking

## Commonly-used Fields
- `from_address`: Essential for identifying transfer senders and outflow analysis
- `to_address`: Critical for identifying transfer recipients and inflow analysis
- `amount`: Transfer amount for value calculations and volume analysis
- `tx_hash`: Important for linking to transaction details and verification
- `block_timestamp`: Primary field for time-series analysis and trend detection
- `success`: Transaction success status for filtering valid transfers

{% enddocs %}
