{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"blocks_tx_v2",
        "sql_limit" :"1200000",
        "producer_batch_size" :"300000",
        "worker_batch_size" :"50000",
        "sql_source" :"{{this.identifier}}" }
    ),
    tags = ['streamline_core_realtime']
) }}

{% if execute %}
    {% set next_batch_num_query %}
    SELECT
        greatest(
            251452492,
            (SELECT coalesce(max(block_number),0) FROM {{ ref('streamline__complete_blocks_tx_v2') }})
        )+1
    {% endset %}
    {% set next_batch_num = run_query(next_batch_num_query)[0][0] %}
{% endif %}

WITH blocks AS (
    SELECT
        block_number
    FROM
        {{ ref("streamline__blocks") }}
    EXCEPT
    SELECT
        block_number
    FROM
        {{ ref("streamline__complete_blocks_tx") }}
    WHERE
        block_number <= 251452492
    EXCEPT
    SELECT 
        block_number
    FROM
        {{ ref('streamline__complete_blocks_tx_v2') }}
)
SELECT
    ROUND(
        block_number,
        -3
    ) :: INT AS partition_key,
    aptos.live.udf_api(
        'GET',
        '{service}/{Authentication}/v1/blocks/by_height/' || block_number || '?with_transactions=true',
        object_construct(
            'Content-Type',
            'application/json',
            'token',
            '{Authentication}'
        ),
        {},
        'Vault/prod/aptos/node/mainnet'
    ) AS request
FROM
    blocks