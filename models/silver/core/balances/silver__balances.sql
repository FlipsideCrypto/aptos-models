{{ config(
    materialized = 'incremental',
    unique_key = ['balances_id'],
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['modified_timestamp'],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::DATE"],
    post_hook = '{{ unverify_tokens() }}',
    tags = ['daily', 'full_test', 'heal']
) }}
-- at most one record per (address, token_address) pair per day - we will get the last transaction of the day
WITH verified_tokens AS (

    SELECT
        DISTINCT token_address
    FROM
        {{ ref('price__ez_prices_hourly') }}
    WHERE
        is_verified
),

{% if is_incremental() and var(
    'HEAL_MODEL',
    false
) %}
newly_verified_tokens AS ({{ get_missing_verified_tokens() }}),
heal_balances AS (
    SELECT
        C.block_number,
        C.block_timestamp,
        C.version,
        C.change_data :metadata :inner :: STRING AS token_address,
        C.change_data :balance :: bigint AS post_balance,
        C.change_data :frozen :: BOOLEAN AS frozen,
        C.address
    FROM
        {{ ref('silver__changes') }} C
    WHERE
        block_timestamp :: DATE >= '2023-07-28'
        AND C.change_module = 'fungible_asset'
        AND C.change_resource = 'FungibleStore'
        AND TRY_CAST(
            C.change_data :balance :: STRING AS bigint
        ) IS NOT NULL
        AND C.address IS NOT NULL
        AND LOWER(
            C.change_data :metadata :inner :: STRING
        ) IN (
            SELECT
                token_address
            FROM
                newly_verified_tokens
        )
),
{% endif %}

fungible_asset_balances AS (
    SELECT
        C.block_number,
        C.block_timestamp,
        C.version,
        C.change_data :metadata :inner :: STRING AS token_address,
        C.change_data :balance :: bigint AS post_balance,
        C.change_data :frozen :: BOOLEAN AS frozen,
        C.address
    FROM
        {{ ref('silver__changes') }} C
    WHERE
        block_timestamp :: DATE >= '2023-07-28'
        AND C.change_module = 'fungible_asset'
        AND C.change_resource = 'FungibleStore'
        AND TRY_CAST(
            C.change_data :balance :: STRING AS bigint
        ) IS NOT NULL
        AND C.address IS NOT NULL
        AND LOWER(token_address) IN (
            SELECT
                LOWER(token_address)
            FROM
                verified_tokens
        )

{% if is_incremental() %}
AND C.modified_timestamp >= (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
all_balances AS (
    SELECT
        block_number,
        block_timestamp,
        version,
        token_address,
        post_balance,
        frozen,
        address
    FROM
        fungible_asset_balances

{% if is_incremental() and var(
    'HEAL_MODEL',
    false
) %}
UNION ALL
SELECT
    block_number,
    block_timestamp,
    version,
    token_address,
    post_balance,
    frozen,
    address
FROM
    heal_balances
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    block_timestamp :: DATE AS block_date,
    version,
    address,
    token_address,
    post_balance AS balance,
    frozen,
    {{ dbt_utils.generate_surrogate_key(['block_date', 'address', 'token_address']) }} AS balances_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    all_balances qualify ROW_NUMBER() over (
        PARTITION BY block_timestamp :: DATE,
        address,
        token_address
        ORDER BY
            block_timestamp DESC
    ) = 1
