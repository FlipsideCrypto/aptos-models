{{ config(
    materialized = 'incremental',
    unique_key = ['block_date', 'address', 'token_address'],
    incremental_strategy = 'delete+insert',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_date', '_inserted_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(address, token_address);",
    tags = ['core', 'full_test']
) }}

WITH fungible_asset_balances AS (
    SELECT
        c.block_number,
        c.block_timestamp,
        c.block_timestamp::DATE AS block_date,
        c.version,
        c.change_data:metadata:inner::STRING AS token_address,
        c.change_data:balance::BIGINT AS post_balance,
        c.change_data:frozen::BOOLEAN AS frozen,
        c.address,
        c.modified_timestamp,
        c._inserted_timestamp
    FROM {{ ref('silver__changes') }} c
    WHERE c.change_module = 'fungible_asset'
      AND c.change_resource = 'FungibleStore'
      AND c.change_data:balance IS NOT NULL
      AND c.address IS NOT NULL
    {% if is_incremental() %}
      AND c.modified_timestamp >= (
          SELECT MAX(modified_timestamp) FROM {{ this }}
      )
    {% endif %}
),

coin_balances AS (
    SELECT
        c.block_number,
        c.block_timestamp,
        c.block_timestamp::DATE AS block_date,
        c.version,
        REPLACE(REPLACE(c.change_resource::STRING, 'CoinStore<', ''), '>', '') AS token_address,
        c.change_data:coin:value::BIGINT AS post_balance,
        FALSE AS frozen,
        COALESCE(
            c.change_data:deposit_events:guid:id:addr,
            c.change_data:withdraw_events:guid:id:addr,
            c.change_data:coin_amount_event:guid:id:addr
        )::STRING AS address,
        c.modified_timestamp,
        c._inserted_timestamp
    FROM {{ ref('silver__changes') }} c
    WHERE c.change_module = 'coin'
      AND c.change_resource LIKE 'CoinStore<%'
      AND c.change_data:coin:value IS NOT NULL
      AND COALESCE(
          c.change_data:deposit_events:guid:id:addr,
          c.change_data:withdraw_events:guid:id:addr,
          c.change_data:coin_amount_event:guid:id:addr
      ) IS NOT NULL
    {% if is_incremental() %}
      AND c.modified_timestamp >= (
          SELECT MAX(modified_timestamp) FROM {{ this }}
      )
    {% endif %}
),

all_balances AS (
    SELECT * FROM fungible_asset_balances
    UNION ALL
    SELECT * FROM coin_balances
),

address_token_pairs AS (
    SELECT
        address,
        token_address,
        MIN(block_date) AS min_date
    FROM all_balances
    GROUP BY address, token_address
),

date_spine AS (
    SELECT date_day AS block_date
    FROM {{ source('crosschain', 'dim_dates') }}
    WHERE date_day >= '2022-10-12'
      AND date_day <= CURRENT_DATE
),

address_token_date_spine AS (
    SELECT
        d.block_date,
        p.address,
        p.token_address
    FROM address_token_pairs p
    CROSS JOIN date_spine d
    WHERE d.block_date >= p.min_date
),

daily_balances AS (
    SELECT
        block_date,
        address,
        token_address,
        post_balance,
        frozen,
        modified_timestamp,
        _inserted_timestamp
    FROM all_balances
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY block_date, address, token_address
        ORDER BY block_number DESC, version DESC
    ) = 1
),

forward_filled_values AS (
    SELECT
        s.block_date,
        s.address,
        s.token_address,
        LAST_VALUE(b.post_balance IGNORE NULLS) OVER (
            PARTITION BY s.address, s.token_address
            ORDER BY s.block_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS post_balance,
        LAST_VALUE(b.frozen IGNORE NULLS) OVER (
            PARTITION BY s.address, s.token_address
            ORDER BY s.block_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS frozen,
        LAST_VALUE(b.modified_timestamp IGNORE NULLS) OVER (
            PARTITION BY s.address, s.token_address
            ORDER BY s.block_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS modified_timestamp,
        LAST_VALUE(b._inserted_timestamp IGNORE NULLS) OVER (
            PARTITION BY s.address, s.token_address
            ORDER BY s.block_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS _inserted_timestamp
    FROM address_token_date_spine s
    LEFT JOIN daily_balances b
        ON s.block_date = b.block_date
        AND s.address = b.address
        AND s.token_address = b.token_address
),

forward_filled_balances AS (
    SELECT
        block_date,
        address,
        token_address,
        post_balance,
        frozen,
        modified_timestamp,
        _inserted_timestamp,
        LAST_VALUE(CASE WHEN post_balance > 0 THEN block_date END IGNORE NULLS) OVER (
            PARTITION BY address, token_address
            ORDER BY block_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS last_positive_date
    FROM forward_filled_values
),

filtered_balances AS (
    SELECT
        block_date,
        address,
        token_address,
        post_balance,
        frozen,
        modified_timestamp,
        _inserted_timestamp
    FROM forward_filled_balances
    WHERE post_balance IS NOT NULL
      AND (
          post_balance > 0
          OR (post_balance = 0
              AND last_positive_date IS NOT NULL
              AND DATEDIFF('day', last_positive_date, block_date) <= 3)
      )
)

SELECT
    block_date,
    address,
    token_address,
    post_balance,
    frozen,
    {{ dbt_utils.generate_surrogate_key(['block_date', 'address', 'token_address']) }} AS balances_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM filtered_balances
