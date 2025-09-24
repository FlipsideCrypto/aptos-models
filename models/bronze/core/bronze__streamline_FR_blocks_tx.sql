{{ config (
    materialized = 'view',
    tags = ['core']
) }}

SELECT
    VALUE,
    partition_key,
    DATA,
    _INSERTED_TIMESTAMP
FROM
    {{ ref('bronze__streamline_FR_blocks_tx_v2') }}