{{ config(
    materialized = 'table',
    tags = ['balances_snapshot']
) }}

{# Set snapshot date - override with --var 'SNAPSHOT_DATE:2025-09-02' #}
{% set snapshot_date = var('SNAPSHOT_DATE', '2025-09-02') %}

SELECT
    block_number,
    block_timestamp,
    block_date,
    version,
    address,
    token_address,
    balance,
    frozen,
    '{{ snapshot_date }}'::DATE AS snapshot_date,
    {{ dbt_utils.generate_surrogate_key(['address', 'token_address', "'" ~ snapshot_date ~ "'"]) }} AS balances_snapshot_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('silver__balances') }}
WHERE
    block_timestamp < '{{ snapshot_date }}'::TIMESTAMP
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY address, token_address
    ORDER BY version DESC
) = 1
