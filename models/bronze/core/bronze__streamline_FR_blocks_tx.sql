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
UNION ALL
SELECT
    VALUE,
    _partition_by_block_id AS partition_key,
    DATA,
    _INSERTED_TIMESTAMP
FROM
    {{ ref('bronze__streamline_FR_blocks_tx_v1') }}