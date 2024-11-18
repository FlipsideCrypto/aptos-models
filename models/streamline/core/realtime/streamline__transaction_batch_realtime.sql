{{ config(
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func='streamline.udf_bulk_rest_api_v2',
        target="{{this.schema}}.{{this.identifier}}",
        params={
            "external_table": "transaction_batch_v2",
            "sql_limit": "1200000",
            "producer_batch_size": "300000",
            "worker_batch_size": "50000",
            "sql_source": "{{this.identifier}}" }
    ),
    tags = ['streamline_core_realtime']
) }}
-- depends_on: {{ ref('bronze__streamline_transaction_batch') }}

WITH blocks AS (
    SELECT
        A.block_number,
        tx_count_from_versions - 100 AS tx_count,
        first_version + 100 AS version_start
    FROM
        {{ ref('silver__blocks') }} A
    WHERE
        tx_count_from_versions > 100
        AND block_number >= 252143860
),
numbers AS (
    -- Recursive CTE to generate numbers. We'll use the maximum txcount value to limit our recursion.
    SELECT
        1 AS n
    UNION ALL
    SELECT 
        n + 1
    FROM 
        numbers
    WHERE
        n < (
            SELECT
                CEIL(MAX(tx_count) / 100.0)
            FROM
                blocks)
),
blocks_with_page_numbers AS (
    SELECT
        tt.block_number :: INT AS block_number,
        n.n - 1 AS multiplier,
        version_start,
        tx_count
    FROM
        blocks tt
                JOIN numbers n
                ON n.n <= CASE
        WHEN tt.tx_count % 100 = 0 THEN tt.tx_count / 100
                    ELSE FLOOR(
                        tt.tx_count / 100
                    ) + 1
        END
),
work AS (
    SELECT
        A.block_number,
        version_start + (100 * multiplier) AS tx_version
    FROM
        blocks_with_page_numbers A
    LEFT JOIN {{ ref('streamline__complete_transaction_batch') }} b
    ON A.block_number = b.block_number
    WHERE b.block_number IS NULL
)
SELECT
    ROUND(
        block_number,
        -3
    ) :: INT AS partition_key,
    aptos_dev.live.udf_api(
        'GET',
        '{service}/{Authentication}/v1/transactions?start=' || tx_version || '&limit=100',
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
    work
ORDER BY
    block_number
limit 10
