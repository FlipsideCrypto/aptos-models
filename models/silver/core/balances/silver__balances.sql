{{ config(
    materialized = 'incremental',
    unique_key = ['block_date','address','token_address'],
    incremental_strategy = 'delete+insert',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_date','_inserted_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(address, token_address);",
    tags = ['core','balances','daily'],
    enabled = true
) }}

WITH
fungible_asset_balances AS (
    SELECT
        c.block_number,
        c.block_timestamp,
        c.block_timestamp::DATE AS block_date,
        c.version,
        c.tx_hash,
        c.change_data:metadata:inner::STRING AS token_address,
        c.change_data:balance::BIGINT AS post_balance,
        c.change_data:frozen::BOOLEAN AS frozen,
        c.address,
        c.modified_timestamp,
        c._inserted_timestamp
    FROM {{ ref('silver__changes') }} c
    INNER JOIN {{ ref('silver__fungible_asset_metadata') }} m
        ON c.change_data:metadata:inner::STRING = m.token_address
    WHERE c.change_module = 'fungible_asset'
      AND c.change_resource = 'FungibleStore'
      AND c.change_data:balance IS NOT NULL
      AND c.address IS NOT NULL
      AND c.change_data:balance::BIGINT > 0

    {% if is_incremental() %}
    AND c.modified_timestamp >= (
        SELECT MAX(modified_timestamp)
        FROM {{ this }}
    )
    {% endif %}
),

coin_balances AS (
    SELECT
        c.block_number,
        c.block_timestamp,
        c.block_timestamp::DATE AS block_date,
        c.version,
        c.tx_hash,
        REPLACE(REPLACE(c.change_resource::STRING, 'CoinStore<'), '>') AS token_address,
        c.change_data:coin:value::BIGINT AS post_balance,
        FALSE AS frozen, -- Coin shouldn't be frozen
        COALESCE(
            c.change_data:deposit_events:guid:id:addr,
            c.change_data:withdraw_events:guid:id:addr,
            c.change_data:coin_amount_event:guid:id:addr
        )::STRING AS address,
        c.modified_timestamp,
        c._inserted_timestamp
    FROM {{ ref('silver__changes') }} c
    INNER JOIN {{ ref('core__dim_tokens') }} t
        ON REPLACE(REPLACE(c.change_resource::STRING, 'CoinStore<'), '>') = t.token_address
    WHERE c.change_module = 'coin'
      AND c.change_resource LIKE 'CoinStore<%'
      AND c.change_data:coin:value IS NOT NULL
      AND COALESCE(
          c.change_data:deposit_events:guid:id:addr,
          c.change_data:withdraw_events:guid:id:addr,
          c.change_data:coin_amount_event:guid:id:addr
      ) IS NOT NULL
      AND c.change_data:coin:value::BIGINT > 0

    {% if is_incremental() %}
    AND c.modified_timestamp >= (
        SELECT MAX(modified_timestamp)
        FROM {{ this }}
    )
    {% endif %}
),

all_balances AS (
    SELECT * FROM fungible_asset_balances
    UNION ALL
    SELECT * FROM coin_balances
),

token_metadata AS (
    SELECT
        token_address,
        symbol,
        decimals,
        name
    FROM {{ ref('silver__fungible_asset_metadata') }}

    UNION ALL

    SELECT
        token_address,
        symbol,
        decimals,
        name
    FROM {{ ref('core__dim_tokens') }}
),

-- Decimal adjustment
balances_with_metadata AS (
    SELECT
        b.block_date,
        b.block_number,
        b.version,
        b.tx_hash,
        b.address,
        b.token_address,
        m.symbol,
        m.decimals,
        m.name,
        b.post_balance,
        b.frozen,
        CASE
            WHEN m.decimals IS NOT NULL
            THEN b.post_balance / POW(10, m.decimals)
            ELSE NULL
        END AS balance,
        b.modified_timestamp,
        b._inserted_timestamp
    FROM all_balances b
    INNER JOIN token_metadata m
        ON b.token_address = m.token_address
    WHERE b.post_balance > 0
),

daily_balances AS (
    SELECT
        block_date,
        address,
        token_address,
        symbol,
        decimals,
        name,
        post_balance,
        balance,
        frozen,
        modified_timestamp,
        _inserted_timestamp
    FROM balances_with_metadata
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY block_date, address, token_address
        ORDER BY block_number DESC, version DESC
    ) = 1
)

SELECT
    block_date,
    address,
    token_address,
    symbol,
    decimals,
    name,
    post_balance,
    balance,
    frozen,
    {{ dbt_utils.generate_surrogate_key(
        ['block_date','address','token_address']
    ) }} AS balances_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM daily_balances
WHERE balance > 0
