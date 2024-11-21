{{ config (
    materialized = "view",
    tags = ['streamline_view']
) }}

  SELECT
    {{ target.database }}.live.udf_api(
        'GET',
        '{service}/{Authentication}/v1',
        OBJECT_CONSTRUCT(
            'Content-Type',
            'application/json',
            'fsc-quantum-state',
            'livequery'
        ),
        OBJECT_CONSTRUCT(),
        'Vault/prod/aptos/node/mainnet'
    )  :data:block_height :: INT AS block_number
