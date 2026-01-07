{{ config(
    materialized = 'incremental',
    unique_key = ['token_address_lower', 'hour'],
    incremental_strategy = 'delete+insert',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['HOUR::DATE'],
    tags = ['core']
) }}

{#
  PERFORMANCE OPTIMIZATION: Pre-compute LOWER() values in CTEs to enable index usage
  Original issue: LOWER() on both sides of JOIN prevents index usage (non-sargable)
#}

WITH prices_base AS (
    SELECT
        hour,
        token_address,
        LOWER(token_address) AS token_address_lower,
        price,
        is_imputed,
        is_deprecated,
        provider,
        source,
        is_verified,
        complete_token_prices_id,
        modified_timestamp,
        _inserted_timestamp,
        blockchain
    FROM
        {{ ref('bronze__complete_token_prices') }}
    WHERE
        (
            blockchain = 'aptos'
            OR (
                blockchain = 'ethereum'
                AND token_address IN (
                    '0xdac17f958d2ee523a2206206994597c13d831ec7',
                    '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599',
                    '0xd31a59c85ae9d8edefec411d448f90841571b89c',
                    '0x4e15361fd6b4bb609fa63c81a2be19d873717870',
                    '0x7c9f4c87d911613fe9ca58b579f737911aad2d43'
                )
            )
        )
{% if is_incremental() %}
        AND modified_timestamp >= (
            SELECT
                MAX(modified_timestamp) - INTERVAL '24 hours'
            FROM
                {{ this }}
        )
{% endif %}
),

manual_metadata AS (
    SELECT
        token_address,
        token_address_raw,
        LOWER(token_address_raw) AS token_address_raw_lower
    FROM
        {{ ref('bronze__manual_token_price_metadata') }}
),

asset_metadata AS (
    SELECT
        token_address,
        LOWER(token_address) AS token_address_lower,
        symbol,
        name
    FROM
        {{ ref('silver__asset_metadata_priority') }}
),

coin_info AS (
    SELECT
        coin_type,
        LOWER(coin_type) AS coin_type_lower,
        symbol,
        name,
        decimals
    FROM
        {{ ref('silver__coin_info') }}
)

SELECT
    p.hour,
    LOWER(COALESCE(b.token_address, p.token_address)) AS token_address_lower,
    COALESCE(
        b.token_address,
        p.token_address
    ) AS token_address,
    p.price,
    p.is_imputed,
    p.is_deprecated,
    COALESCE(
        C.symbol,
        m.symbol
    ) AS symbol,
    C.decimals AS decimals,
    p.provider,
    p.source,
    COALESCE(
        C.name,
        m.name
    ) AS NAME,
    p.is_verified,
    p.complete_token_prices_id AS hourly_prices_priority_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    p._inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    prices_base p
    LEFT JOIN manual_metadata b
        ON p.token_address_lower = b.token_address_raw_lower
    LEFT JOIN asset_metadata m
        ON p.token_address_lower = m.token_address_lower
    LEFT JOIN coin_info C
        ON LOWER(COALESCE(b.token_address, p.token_address)) = C.coin_type_lower
QUALIFY (ROW_NUMBER() OVER (
    PARTITION BY p.hour, LOWER(COALESCE(b.token_address, p.token_address))
    ORDER BY HOUR DESC
)) = 1
