{{ config(
    materialized = 'incremental',
    unique_key = 'bridge_address',
    incremental_strategy = 'delete+insert',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['modified_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(bridge_address, bridge_platform);",
    tags = ['silver', 'defi', 'bridge', 'curated']
) }}

WITH seed_bridges AS (
    SELECT
        bridge_address,
        bridge_platform,
        bridge_type,
        'seed_file' AS source_type
    FROM {{ ref('silver__bridge_addresses_seed') }}
),

discovered_bridges AS (
    SELECT DISTINCT
        bridge_address,
        platform AS bridge_platform,
        NULL AS bridge_type,
        'automated_discovery' AS source_type
    FROM {{ ref('silver__bridge_combined') }}
    WHERE bridge_address NOT IN (
        SELECT bridge_address FROM seed_bridges
    )

    {% if is_incremental() %}
    AND modified_timestamp >= (
        SELECT MAX(modified_timestamp)
        FROM {{ this }}
    )
    {% endif %}
),

combined_bridges AS (
    SELECT
        bridge_address,
        bridge_platform,
        bridge_type,
        source_type
    FROM seed_bridges

    UNION ALL

    SELECT
        bridge_address,
        bridge_platform,
        bridge_type,
        source_type
    FROM discovered_bridges
)

SELECT
    bridge_address,
    bridge_platform,
    bridge_type,
    CASE bridge_platform
        WHEN 'layerzero' THEN 'LayerZero'
        WHEN 'layerzero_oft' THEN 'LayerZero OFT'
        WHEN 'wormhole' THEN 'Wormhole'
        WHEN 'celer_cbridge' THEN 'Celer cBridge'
        WHEN 'celer' THEN 'Celer cBridge'
        WHEN 'mover' THEN 'Mover'
        WHEN 'mover_alt' THEN 'Mover'
        ELSE INITCAP(bridge_platform)
    END AS bridge_name,
    source_type,
    {{ dbt_utils.generate_surrogate_key(['bridge_address']) }} AS bridge_metadata_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM combined_bridges

{% if is_incremental() %}
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY bridge_address
    ORDER BY CASE source_type WHEN 'seed_file' THEN 1 ELSE 2 END  -- Prefer seed file over auto-discovery
) = 1
{% endif %}
