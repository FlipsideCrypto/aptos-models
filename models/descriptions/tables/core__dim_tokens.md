{% docs core__dim_tokens %}

## Description
This table serves as the comprehensive token metadata dimension for the Aptos blockchain, combining information from both legacy coin tokens and newer fungible assets. It provides essential token characteristics including names, symbols, decimal precision, creator addresses, and creation timestamps. The table unifies metadata from two different token standards on Aptos: the original coin module and the newer fungible asset module, ensuring complete coverage of all tokens on the network for analytics and display purposes.

## Key Use Cases
- Token identification and display with human-readable names and symbols
- Decimal conversion for accurate financial calculations in transfer analysis
- Token creation analysis and creator address tracking
- Token verification and metadata validation for data quality
- Token discovery and categorization for DeFi and NFT analytics
- Historical token creation timeline analysis

## Important Relationships
- Enriches transfer data in `core.fact_transfers` and `core.ez_transfers` with token metadata
- Provides decimal information for proper amount calculations in transfer analytics
- Supports token verification processes and quality assessment
- Links to creator addresses for token origin analysis
- Enables token categorization and filtering across all core models

## Commonly-used Fields
- `token_address`: Primary identifier for linking to transfer and transaction data
- `symbol`: Human-readable token symbol for easy identification and filtering
- `decimals`: Critical for proper decimal conversion in financial calculations
- `name`: Full token name for display and identification purposes
- `creator_address`: Important for token origin analysis and creator tracking
- `transaction_created_timestamp`: Essential for token creation timeline analysis

{% enddocs %}
