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
WHERE
    metadata_address <> '0xa'

{% if is_incremental() %}
AND b.metadata_address IS NULL
{% endif %}
GROUP BY
    metadata_address
HAVING
    COUNT(1) > 10
ORDER BY
    COUNT(1) DESC
LIMIT
    10
), params AS (
    SELECT
        'query MyQuery { fungible_asset_metadata_by_pk( asset_type:"' || metadata_address || '") { asset_type creator_address decimals icon_uri name project_uri symbol token_standard }} ' AS query,
        metadata_address
    FROM
        tokens
),
res AS (
    SELECT
        metadata_address,
        live.udf_api(
            'post',
            'https://indexer.mainnet.aptoslabs.com/v1/graphql',
            OBJECT_CONSTRUCT(
                'Content-Type',
                'application/json'
            ),
            OBJECT_CONSTRUCT(
                'query',
                query,
                'variables',{}
            )
        ) :data :data :fungible_asset_metadata_by_pk AS DATA
    FROM
        params
)
SELECT
    metadata_address,
    DATA,
    DATA :asset_type :: STRING AS asset_type,
    DATA :creator_address :: STRING AS creator_address,
    DATA :symbol :: STRING AS symbol,
    DATA :name :: STRING AS NAME,
    DATA :decimals :: INT AS decimals,
    SYSDATE() AS _inserted_timestamp
FROM
    res
WHERE
    asset_type IS NOT NULL
