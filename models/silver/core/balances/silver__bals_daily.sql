{{ config(
    materialized = 'incremental',
    unique_key = ['balances_daily_id'],
    incremental_predicates = ["dynamic_range_predicate", "balance_date"],
    cluster_by = ['balance_date'],
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = [
        "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(address, token_address);",
        "DELETE FROM {{ this }} WHERE balance_date < CURRENT_DATE - 95 AND DAYOFWEEK(balance_date) != 0;"
    ],
    tags = ['daily_balances'],
    full_refresh = false
) }}

WITH date_spine AS (
    SELECT
        date_day AS balance_date
    FROM
        {{ source(
            'crosschain',
            'dim_dates'
        ) }}
    WHERE
        date_day >= '2023-07-28'
        AND date_day < SYSDATE() :: DATE

{% if is_incremental() %}
AND date_day > (
    SELECT
        MAX(balance_date)
    FROM
        {{ this }}
)
 
{% endif %}
),

{% if is_incremental() %}
latest_balances_from_table AS (
    SELECT
        address,
        token_address,
        balance,
        frozen,
        last_balance_change,
        balance_date
    FROM {{ this }}
    WHERE balance_date = (
        SELECT MAX(balance_date)
        FROM {{ this }}
    )
),
{% endif %}

todays_balance_changes AS (
    -- Get balance changes for dates in the date spine
    SELECT
        block_date AS balance_date,
        address,
        token_address,
        balance,
        frozen,
        block_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY block_date, address, token_address
            ORDER BY block_timestamp DESC, block_number DESC, version DESC
        ) AS daily_rank
    FROM {{ ref('silver__bals') }} tb
    WHERE EXISTS (
            SELECT 1 FROM date_spine ds
            WHERE ds.balance_date = tb.block_date
        )
),

todays_final_balances AS (
    -- Get the last balance change per address-token_address for today
    SELECT
        balance_date,
        address,
        token_address,
        balance,
        frozen,
        block_timestamp AS last_balance_change_timestamp,
        TRUE AS balance_changed_on_date
    FROM todays_balance_changes
    WHERE daily_rank = 1
),

address_token_combinations AS (
    -- Get all unique address-token_address combinations that have ever had a balance
    SELECT DISTINCT
        address,
        token_address
    FROM todays_final_balances
),

source_data AS (
    {% if is_incremental() %}
    -- Check if processing multiple days (batch mode)
    {% if execute %}
        {% set max_date_query %}
            SELECT MAX(balance_date) as max_date FROM {{ this }}
        {% endset %}
        {% set max_date = run_query(max_date_query).columns[0].values()[0] %}
        {% set days_to_process = (modules.datetime.date.today() - max_date).days %}
        {% set batch_size = days_to_process if days_to_process <= 60 else 60 %}
    {% else %}
        {% set batch_size = 1 %}
    {% endif %}

    {% if batch_size > 1 %}
    -- Multi-day batch: Use window functions for proper forward-filling
    SELECT
        d.balance_date,
        COALESCE(c.address, y.address) AS address,
        COALESCE(c.token_address, y.token_address) AS token_address,
        -- For balance, use the most recent change within batch, or carry forward from yesterday
        COALESCE(
            LAST_VALUE(t.balance IGNORE NULLS) OVER (
                PARTITION BY COALESCE(c.address, y.address), COALESCE(c.token_address, y.token_address)
                ORDER BY d.balance_date
                ROWS UNBOUNDED PRECEDING
            ),
            y.balance
        ) AS balance,
        -- For frozen, use the most recent change within batch, or carry forward from yesterday
        COALESCE(
            LAST_VALUE(t.frozen IGNORE NULLS) OVER (
                PARTITION BY COALESCE(c.address, y.address), COALESCE(c.token_address, y.token_address)
                ORDER BY d.balance_date
                ROWS UNBOUNDED PRECEDING
            ),
            y.frozen
        ) AS frozen,
        -- For last_balance_change, we need to track the most recent change date within the batch
        CASE
            WHEN MAX(CASE WHEN t.balance_date IS NOT NULL THEN d.balance_date END) OVER (
                PARTITION BY COALESCE(c.address, y.address), COALESCE(c.token_address, y.token_address)
                ORDER BY d.balance_date
                ROWS UNBOUNDED PRECEDING
            ) IS NOT NULL THEN
                MAX(CASE WHEN t.balance_date IS NOT NULL THEN d.balance_date END) OVER (
                    PARTITION BY COALESCE(c.address, y.address), COALESCE(c.token_address, y.token_address)
                    ORDER BY d.balance_date
                    ROWS UNBOUNDED PRECEDING
                )::TIMESTAMP
            ELSE y.last_balance_change::TIMESTAMP
        END AS last_balance_change_timestamp,
        CASE WHEN t.balance_date IS NOT NULL THEN TRUE ELSE FALSE END AS balance_changed_on_date
    FROM date_spine d
    CROSS JOIN (
        -- All addresses that should exist (previous + new)
        SELECT address, token_address FROM latest_balances_from_table
        UNION
        SELECT address, token_address FROM address_token_combinations
    ) c
    LEFT JOIN todays_final_balances t
        ON d.balance_date = t.balance_date
        AND c.address = t.address
        AND c.token_address = t.token_address
    LEFT JOIN latest_balances_from_table y
        ON c.address = y.address
        AND c.token_address = y.token_address

    {% else %}
    -- Single day: Use original efficient logic
    SELECT
        balance_date,
        address,
        token_address,
        balance,
        frozen,
        last_balance_change_timestamp,
        balance_changed_on_date
    FROM todays_final_balances

    UNION ALL

    -- Carry forward yesterday's balances for addresses that didn't change today
    SELECT
        d.balance_date,
        y.address,
        y.token_address,
        y.balance,
        y.frozen,
        y.last_balance_change::TIMESTAMP AS last_balance_change_timestamp,
        FALSE AS balance_changed_on_date
    FROM date_spine d
    CROSS JOIN latest_balances_from_table y
    LEFT JOIN todays_final_balances t
        ON y.address = t.address
        AND y.token_address = t.token_address
        AND d.balance_date = t.balance_date
    WHERE t.address IS NULL  -- Only addresses with no changes today
    {% endif %}

    {% else %}
    -- Full refresh: Create complete time series with forward-filling
    SELECT
        d.balance_date,
        c.address,
        c.token_address,
        LAST_VALUE(t.balance IGNORE NULLS) OVER (
            PARTITION BY c.address, c.token_address
            ORDER BY d.balance_date
            ROWS UNBOUNDED PRECEDING
        ) AS balance,
        LAST_VALUE(t.frozen IGNORE NULLS) OVER (
            PARTITION BY c.address, c.token_address
            ORDER BY d.balance_date
            ROWS UNBOUNDED PRECEDING
        ) AS frozen,
        LAST_VALUE(t.last_balance_change_timestamp IGNORE NULLS) OVER (
            PARTITION BY c.address, c.token_address
            ORDER BY d.balance_date
            ROWS UNBOUNDED PRECEDING
        ) AS last_balance_change_timestamp,
        CASE WHEN t.balance_date IS NOT NULL THEN TRUE ELSE FALSE END AS balance_changed_on_date
    FROM date_spine d
    CROSS JOIN address_token_combinations c
    LEFT JOIN todays_final_balances t
        ON d.balance_date = t.balance_date
        AND c.address = t.address
        AND c.token_address = t.token_address
    {% endif %}
)

SELECT
    balance_date,
    address,
    token_address,
    balance,
    frozen,
    last_balance_change_timestamp::DATE AS last_balance_change,
    balance_changed_on_date,
    {{ dbt_utils.generate_surrogate_key(['balance_date', 'address', 'token_address']) }} AS balances_daily_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM source_data
WHERE balance IS NOT NULL  -- Only include addresses that have had at least one balance
    AND balance > 0  -- Only include addresses with positive balances
