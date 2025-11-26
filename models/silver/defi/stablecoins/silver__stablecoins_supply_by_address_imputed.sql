{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ["stablecoins_supply_by_address_imputed_id"],
    cluster_by = ['block_date'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(address, contract_address);",
    tags = ['silver','defi','stablecoins','curated_daily']
) }}

-- Forward-fill supply_by_address

WITH base_supply AS (
    SELECT
        block_date,
        address,
        contract_address,
        symbol,
        decimals,
        name,
        balance,
        modified_timestamp
    FROM {{ ref('silver__stablecoins_supply_by_address') }}

    {% if is_incremental() %}
    WHERE modified_timestamp > (
        SELECT MAX(modified_timestamp)
        FROM {{ this }}
    )
    {% endif %}
),

{% if is_incremental() %}
min_base_supply AS (
    SELECT
        MIN(block_date) AS min_base_supply_date,
        address,
        contract_address
    FROM base_supply
    GROUP BY address, contract_address
),

incremental_supply AS (
    SELECT
        s.block_date,
        s.address,
        s.contract_address,
        s.symbol,
        s.decimals,
        s.name,
        s.balance,
        s.modified_timestamp,
        FALSE AS is_imputed
    FROM {{ ref('silver__stablecoins_supply_by_address') }} s
    INNER JOIN min_base_supply m
        ON s.address = m.address
        AND s.contract_address = m.contract_address
        AND s.block_date >= m.min_base_supply_date
),

base_supply_list AS (
    SELECT
        address,
        contract_address,
        COUNT(1) AS pair_count
    FROM base_supply
    GROUP BY address, contract_address
),

existing_supply AS (
    SELECT
        t.block_date,
        t.address,
        t.contract_address,
        t.symbol,
        t.decimals,
        t.name,
        t.balance,
        t.modified_timestamp,
        t.is_imputed
    FROM {{ this }} t
    LEFT JOIN base_supply_list b
        ON t.address = b.address
        AND t.contract_address = b.contract_address
    WHERE block_date = (SELECT MAX(block_date) FROM {{ this }})
      AND b.address IS NULL
),
{% endif %}

all_supply AS (
    {% if is_incremental() %}
    SELECT * FROM incremental_supply
    UNION ALL
    SELECT * FROM existing_supply
    {% else %}
    SELECT block_date, address, contract_address, symbol, decimals, name,
           balance, modified_timestamp, FALSE AS is_imputed
    FROM base_supply
    {% endif %}
),

address_contract_pairs AS (
    SELECT
        address,
        contract_address,
        symbol,
        decimals,
        name,
        MIN(block_date) AS min_balance_date
    FROM all_supply
    GROUP BY address, contract_address, symbol, decimals, name
),

date_spine AS (
    SELECT date_day
    FROM {{ source('crosschain', 'dim_dates') }}
    WHERE date_day < SYSDATE()::DATE
      AND date_day >= (SELECT MIN(block_date) FROM all_supply)
),

date_address_contract_spine AS (
    SELECT
        d.date_day AS block_date,
        p.address,
        p.contract_address,
        p.symbol,
        p.decimals,
        p.name
    FROM date_spine d
    INNER JOIN address_contract_pairs p
        ON d.date_day >= p.min_balance_date
),

filled_balances AS (
    SELECT
        s.block_date,
        s.address,
        s.contract_address,
        s.symbol,
        s.decimals,
        s.name,
        COALESCE(
            a.balance,
            LAST_VALUE(a.balance IGNORE NULLS) OVER (
                PARTITION BY s.address, s.contract_address
                ORDER BY s.block_date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
        ) AS balance,
        {% if is_incremental() %}
        COALESCE(a.is_imputed, TRUE) AS is_imputed,
        {% else %}
        CASE WHEN a.balance IS NULL THEN TRUE ELSE FALSE END AS is_imputed,
        {% endif %}
        a.modified_timestamp
    FROM date_address_contract_spine s
    LEFT JOIN all_supply a
        ON s.block_date = a.block_date
        AND s.address = a.address
        AND s.contract_address = a.contract_address
)

SELECT
    s.block_date,
    s.address,
    s.contract_address,
    s.symbol,
    s.decimals,
    s.name,
    s.balance,
    s.is_imputed,
    {{ dbt_utils.generate_surrogate_key(
        ['block_date','address','contract_address']
    ) }} AS stablecoins_supply_by_address_imputed_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM filled_balances s
WHERE s.balance > 0