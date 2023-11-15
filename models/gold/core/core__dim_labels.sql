{{ config(
    materialized = 'view',
    tags = ['noncore']
) }}

SELECT
    blockchain,
    creator,
    address,
    label_type,
    label_subtype,
    project_name AS label,
    address_name AS address_name
FROM
    {{ ref('silver__labels') }}