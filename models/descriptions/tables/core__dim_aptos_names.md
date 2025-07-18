{% docs core__dim_aptos_names %}

## Description
This table contains comprehensive information about Aptos Names, the blockchain's naming service that allows users to register human-readable domain names linked to their wallet addresses. The table tracks all registered names including domains, subdomains, ownership details, registration status, and expiration information. Aptos Names function similarly to ENS on Ethereum, providing a user-friendly way to identify and interact with addresses on the Aptos network through memorable names rather than long hexadecimal addresses.

## Key Use Cases
- Aptos Name registration and ownership analysis
- Domain name tracking and expiration monitoring
- User identity mapping and address resolution
- Subdomain management and hierarchical name analysis
- Primary name identification for user profile analysis
- Name service adoption and usage pattern analysis

## Important Relationships
- Enriches address information across core models by providing human-readable names
- Links to transaction data in `core.fact_transactions` through owner and registered addresses
- Supports transfer analysis in `core.fact_transfers` and `core.ez_transfers` with named addresses
- Enables user-friendly analytics by resolving addresses to readable names
- Provides context for user behavior analysis through named entity identification

## Commonly-used Fields
- `token_name`: Full Aptos Name identifier for display and filtering
- `domain`: Primary domain name for categorization and analysis
- `owner_address`: Address that owns the name for ownership analysis
- `registered_address`: Address the name resolves to for address mapping
- `is_active`: Status indicator for valid and active names
- `is_primary`: Flag for identifying primary names for users
- `expiration_timestamp`: Critical for name renewal and expiration analysis

{% enddocs %}
