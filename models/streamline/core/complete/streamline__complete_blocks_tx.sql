-- depends_on: {{ ref('bronze__streamline_blocks_tx') }}
-- depends_on: {{ ref('bronze__streamline_FR_blocks_tx') }}
{{ config (
    materialized = "incremental",
    unique_key = "block_number",
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["block_number"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(block_number)"
) }}

SELECT
    DATA: block_height :: INT AS block_number,
    {{ dbt_utils.generate_surrogate_key(
        ['block_number']
    ) }} AS complete_blocks_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_blocks_tx') }}
{% else %}
    {{ ref('bronze__streamline_FR_blocks_tx') }}
{% endif %}
WHERE
    block_number IS NOT NULL

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        COALESCE(MAX(_INSERTED_TIMESTAMP), '1970-01-01' :: DATE) max_INSERTED_TIMESTAMP
    FROM
        {{ this }})
    {% endif %}

    qualify(ROW_NUMBER() over (PARTITION BY block_number
    ORDER BY
        _inserted_timestamp DESC)) = 1
