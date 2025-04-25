{{ config(
    materialized = 'incremental',
    full_refresh = false,
    tags = ['noncore']
) }}

WITH tokens AS (

    SELECT
        metadata_address
    FROM
        {{ ref('silver__fungiblestore_metadata') }} A

{% if is_incremental() %}
LEFT JOIN {{ this }}
b USING(metadata_address)
{% endif %}

{% if is_incremental() %}
WHERE
    b.metadata_address IS NULL
{% endif %}
GROUP BY
    metadata_address {# HAVING
    COUNT(1) > 10 #}
ORDER BY
    COUNT(1) DESC
LIMIT
    20
), res AS (
    SELECT
        metadata_address,
        {{ target.database }}.live.udf_api(
            'GET',
            '{service}/{Authentication}/v1/accounts/' || metadata_address || '/resource/0x1::fungible_asset::Metadata',
            OBJECT_CONSTRUCT(
                'Content-Type',
                'application/json'
            ),
            OBJECT_CONSTRUCT(),
            'Vault/prod/aptos/node/mainnet'
        ) :data :data AS DATA
    FROM
        tokens
)
SELECT
    metadata_address,
    DATA,
    DATA :symbol :: STRING AS symbol,
    DATA :name :: STRING AS NAME,
    DATA :decimals :: INT AS decimals,
    DATA :icon_uri :: STRING AS icon_uri,
    DATA :project_uri :: STRING AS project_uri,
    SYSDATE() AS _inserted_timestamp
FROM
    res
WHERE
    decimals IS NOT NULL
