{{ config(
    materialized = 'incremental',
    unique_key = 'token_address',
    incremental_strategy = 'delete+insert',
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['silver', 'defi', 'stablecoins', 'curated']
) }}

-- Primary source from crosschain + manual seed file

WITH crosschain_stablecoins AS (
    SELECT
        s.token_address,
        s.id AS crosschain_id,
        s.asset_id AS gecko_id,
        UPPER(s.symbol) AS symbol,
        s.name,
        s.peg_type,
        s.peg_mechanism,
        s.modified_timestamp AS crosschain_modified_timestamp
    FROM {{ source('crosschain_silver', 'tokens_stablecoins') }} s
    WHERE LOWER(s.blockchain) = 'aptos'

    {% if is_incremental() %}
    AND s.modified_timestamp > (
        SELECT MAX(crosschain_modified_timestamp)
        FROM {{ this }}
        WHERE crosschain_id IS NOT NULL
    )
    {% endif %}
),

seed_stablecoins AS (
    SELECT
        token_address,
        NULL AS crosschain_id,
        NULL AS gecko_id,
        UPPER(symbol) AS symbol,
        NULL AS name,
        'peggedUSD' AS peg_type,
        NULL AS peg_mechanism,
        NULL AS crosschain_modified_timestamp
    FROM {{ ref('silver__stablecoins_seed') }}
    WHERE LOWER(blockchain) = 'aptos'
    {% if is_incremental() %}
        AND token_address NOT IN (
            SELECT token_address FROM {{ this }}
        )
    {% endif %}
),

combined_stablecoins AS (
    SELECT * FROM crosschain_stablecoins
    UNION ALL
    SELECT * FROM seed_stablecoins
),

enriched_stablecoins AS (
    SELECT
        c.token_address,
        c.crosschain_id,
        c.gecko_id,
        UPPER(COALESCE(c.symbol, m.symbol, p.symbol, ci.symbol)) AS symbol,
        COALESCE(c.name, m.name, p.name, ci.name) AS name,
        COALESCE(m.decimals, p.decimals, ci.decimals) AS decimals,
        m.icon_uri,
        m.project_uri,
        c.peg_type,
        c.peg_mechanism,
        COALESCE(pr.is_verified, FALSE) AS is_verified,
        NULL AS is_verified_modified_timestamp,
        COALESCE(p.is_deprecated, FALSE) AS is_deprecated,
        CASE
            WHEN c.crosschain_id IS NOT NULL THEN 'crosschain'
            ELSE 'manual_seed'
        END AS source_type,
        c.crosschain_modified_timestamp,
        m.modified_timestamp AS metadata_modified_timestamp,
        p.asset_id
    FROM combined_stablecoins c
    LEFT JOIN {{ ref('silver__fungible_asset_metadata') }} m
        ON c.token_address = m.token_address
    LEFT JOIN {{ ref('price__ez_asset_metadata') }} p
        ON c.token_address = p.token_address
    LEFT JOIN {{ ref('silver__coin_info') }} ci
        ON c.token_address = ci.coin_type
    LEFT JOIN (
        SELECT
            token_address,
            is_verified,
            ROW_NUMBER() OVER (PARTITION BY token_address ORDER BY hour DESC) AS rn
        FROM {{ ref('price__ez_prices_hourly') }}
    ) pr ON c.token_address = pr.token_address AND pr.rn = 1
)

SELECT
    token_address,
    crosschain_id,
    gecko_id,
    asset_id,
    symbol,
    name,
    decimals,
    icon_uri,
    project_uri,
    peg_type,
    peg_mechanism,
    is_verified,
    is_verified_modified_timestamp,
    is_deprecated,
    source_type,
    crosschain_modified_timestamp,
    metadata_modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['token_address']) }} AS stablecoins_metadata_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM enriched_stablecoins

{% if is_incremental() %}
WHERE modified_timestamp >= (
    SELECT MAX(modified_timestamp)
    FROM {{ this }}
)
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY token_address
    ORDER BY COALESCE(crosschain_modified_timestamp, metadata_modified_timestamp) DESC
) = 1
{% endif %}
