{{ config(
    materialized = 'view',
    tags = ['full_test'],
    enabled = false
) }}

SELECT
    *
FROM
    {{ ref(
        'silver__transfers'
    ) }}
