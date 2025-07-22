{% docs core__dim_labels %}

## Description
This table provides comprehensive address labeling and classification for the Aptos blockchain, serving as a centralized repository of address identifiers and names. Labels are organized into hierarchical categories with "type" (e.g., cex, dex, dapp, games) and "subtype" (e.g., contract_deployer, hot_wallet, token_contract) to enable systematic address classification. The labeling system combines automatic algorithmic detection with manual curation, including community contributions through tools like the add-a-label platform. Labels can be dynamically updated and removed based on accuracy assessments and relevance changes.

## Key Use Cases
- Address identification and classification for analytics and reporting
- DeFi protocol analysis by identifying protocol-related addresses
- Exchange and institutional wallet tracking and monitoring
- Contract deployment analysis and developer activity tracking
- Security analysis and suspicious address identification
- Network mapping and relationship analysis between labeled entities

## Important Relationships
- Enriches address information across all core models including `core.fact_transfers`, `core.ez_transfers`, and `core.fact_transactions`
- Provides context for transaction analysis by identifying participant types
- Supports DeFi analytics by classifying protocol-related addresses
- Enables cross-chain analysis through consistent labeling standards
- Links to community-contributed labels and external labeling systems

## Commonly-used Fields
- `address`: Primary identifier for linking to transaction and transfer data
- `label_type`: High-level categorization for broad address classification
- `label_subtype`: Detailed classification for specific address roles and functions
- `label`: Human-readable name or identifier for the address
- `creator`: Source of the label for attribution and quality assessment
- `blockchain`: Chain identifier for cross-chain analysis

{% enddocs %}
