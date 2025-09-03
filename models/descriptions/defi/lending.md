{% docs lending_platform %}
The name of the lending platform where the activity occurred (e.g., 'echelon', 'echo'). Used to identify which specific lending protocol handled the transaction.

**Data type:** String
**Example:** 
- echelon
- echo

**Business Context:**
- Enables filtering and analysis by specific lending platforms
- Supports cross-protocol comparisons and platform performance metrics
- Essential for multi-protocol lending analytics and protocol adoption tracking
{% enddocs %}

{% docs lending_protocol %}
The protocol name for the lending platform, typically matching the platform name. Used for protocol identification and categorization in analytics.

**Data type:** String
**Example:**
- echelon
- echo

**Business Context:**
- Provides consistent protocol naming across different data sources
- Enables protocol-specific analysis and reporting
- Supports protocol comparison and market share analysis
{% enddocs %}

{% docs lending_version %}
The version of the lending protocol being used (e.g., 'v1', 'v2'). Indicates which iteration of the protocol's smart contracts handled the transaction.

**Data type:** String
**Example:**
- v1
- v2

**Business Context:**
- Tracks protocol upgrades and version adoption
- Enables analysis of protocol evolution and feature usage
- Important for understanding protocol maturity and stability
{% enddocs %}

{% docs lending_liquidator %}
The address of the account that initiated the liquidation of a borrower's position. Liquidators are incentivized to liquidate undercollateralized positions to maintain protocol health.

**Data type:** String
**Example:**
- 0x1234567890abcdef1234567890abcdef12345678

**Business Context:**
- Identifies who is performing liquidations and earning liquidation rewards
- Enables analysis of liquidator behavior and profitability
- Important for understanding liquidation market dynamics and risk management
{% enddocs %}

{% docs lending_borrower %}
The address of the account that borrowed assets from the lending protocol. Borrowers must provide collateral to secure their loans.

**Data type:** String
**Example:**
- 0x1234567890abcdef1234567890abcdef12345678

**Business Context:**
- Core field for user behavior analysis and risk assessment
- Enables tracking of borrowing patterns and credit risk
- Essential for understanding user engagement and protocol utilization
{% enddocs %}

{% docs lending_depositor %}
The address of the account that deposited assets into the lending protocol to earn interest or provide liquidity.

**Data type:** String
**Example:**
- 0x1234567890abcdef1234567890abcdef12345678

**Business Context:**
- Identifies users providing liquidity to the protocol
- Enables analysis of deposit patterns and user retention
- Important for understanding TVL growth and user engagement
{% enddocs %}

{% docs lending_collateral_token %}
The token address of the collateral asset used in a lending transaction. In liquidations, this represents the asset being liquidated from the borrower's position.

**Data type:** String
**Example:**
- 0x1234567890abcdef1234567890abcdef12345678

**Business Context:**
- Identifies which assets are being used as collateral
- Enables analysis of collateral composition and risk
- Important for understanding asset utilization and market dynamics
{% enddocs %}

{% docs lending_debt_token %}
The token address of the debt asset in a lending transaction. In liquidations, this represents the asset that was borrowed and is being repaid.

**Data type:** String
**Example:**
- 0x1234567890abcdef1234567890abcdef12345678

**Business Context:**
- Identifies which assets are being borrowed
- Enables analysis of borrowing demand and asset utilization
- Important for understanding lending market dynamics and risk exposure
{% enddocs %}

{% docs lending_payer %}
The address of the account that paid for a repayment transaction. This can be the borrower themselves or a third party (e.g., a liquidator or helper service).

**Data type:** String
**Example:**
- 0x1234567890abcdef1234567890abcdef12345678

**Business Context:**
- Identifies who is making repayments (self vs. third-party)
- Enables analysis of repayment patterns and user behavior
- Important for understanding debt management and protocol health
{% enddocs %}

{% docs lending_withdrawer %}
The address of the account that withdrew previously deposited assets from the lending protocol.

**Data type:** String
**Example:**
- 0x1234567890abcdef1234567890abcdef12345678

**Business Context:**
- Identifies users removing liquidity and realizing yield or reallocating collateral
- Enables analysis of withdrawal behavior, retention, and liquidity outflows
- Useful for tracking user lifecycle (deposit → borrow/repay → withdraw)
{% enddocs %}
