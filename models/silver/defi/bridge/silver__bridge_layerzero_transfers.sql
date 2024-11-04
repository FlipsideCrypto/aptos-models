{{ config(
    materialized = 'incremental',
    unique_key = "bridge_layerzero_transfers_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash, version, sender, receiver);",
    tags = ['noncore']
) }}

WITH evnts AS (

    SELECT
        block_number,
        block_timestamp,
        version,
        tx_hash,
        event_index,
        payload_function,
        event_address,
        event_resource,
        event_data,
        _inserted_timestamp
    FROM
        {{ ref('silver__events') }}
    WHERE
        --bridge events & oft
        event_address IN (
            '0xf22bede237a07e121b56d91a491eb7bcdfd1f5907926a9e58338f964a01b17fa',
            '0x43d8cad89263e6936921a0adb8d5d49f0e236c229460f01b14dca073114df2b9'
        )
        AND event_module IN(
            'coin_bridge',
            'oft'
        )
        AND event_resource IN (
            'SendEvent',
            'ReceiveEvent'
        )
        AND success

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% else %}
    AND block_timestamp :: DATE >= '2022-10-19'
{% endif %}
),
txs AS (
    SELECT
        block_timestamp,
        tx_hash,
        sender,
        payload :type_arguments [0] :: STRING AS token_address,
        payload :arguments [1] :: STRING AS src_sender
    FROM
        {{ ref('silver__transactions') }}
    WHERE
        success

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% else %}
    AND block_timestamp :: DATE >= '2022-10-19'
{% endif %}
),
chngs AS (
    SELECT
        block_timestamp,
        tx_hash,
        change_module,
        CASE
            WHEN change_module = 'coin' THEN change_data :coin :value
            WHEN change_module = 'oft' THEN change_data :locked_coin :value
        END :: INT AS amount,
        change_resource :: STRING AS token_address,
        change_index
    FROM
        {{ ref('silver__changes') }}
    WHERE
        success
        AND change_module IN (
            'coin',
            'oft'
        ) {# AND amount IS NOT NULL #}

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% else %}
    AND block_timestamp :: DATE >= '2022-10-19'
{% endif %}
),
chngs_2 AS (
    SELECT
        block_timestamp,
        tx_hash,
        token_address
    FROM
        chngs
    WHERE
        change_module = 'coin'
        AND token_address LIKE 'CoinInfo%' qualify(ROW_NUMBER() over(PARTITION BY tx_hash
    ORDER BY
        change_index DESC) = 1)
)
SELECT
    A.block_number,
    A.block_timestamp,
    A.version,
    A.tx_hash,
    'layerzero' AS platform,
    A.event_address AS bridge_address,
    A.event_resource AS event_name,
    CASE
        WHEN event_resource = 'SendEvent' THEN 'outbound'
        ELSE 'inbound'
    END AS direction,
    b.sender AS tx_sender,
    CASE
        WHEN event_resource = 'SendEvent' THEN b.sender
        ELSE b.src_sender
    END AS sender,
    COALESCE(
        event_data :receiver,
        REPLACE(
            event_data :dst_receiver,
            '000000000000000000000000'
        )
    ) :: STRING AS receiver,
    CASE
        WHEN direction = 'outbound' THEN 108
        ELSE event_data :src_chain_id :: INT
    END AS source_chain_id,
    src.chain_name AS source_chain_name,
    CASE
        WHEN direction = 'inbound' THEN 108
        ELSE event_data :dst_chain_id :: INT
    END AS destination_chain_id,
    dst.chain_name AS destination_chain_name,
    REPLACE(
        REPLACE(
            REPLACE(
                COALESCE(
                    event_data :coin_type :account_address || '::' || HEX_DECODE_STRING(REPLACE(event_data :coin_type :module_name, '0x')) || '::' || HEX_DECODE_STRING(REPLACE(event_data :coin_type :struct_name, '0x')),
                    b.token_address,
                    C.token_address,
                    d.token_address,
                    e.token_address
                ),
                'CoinStore<'
            ),
            'CoinInfo<'
        ),
        '>'
    ) AS token_address,
    COALESCE(
        event_data :amount,
        event_data :amount_ld
    ) :: INT AS amount_unadj,
    A.event_index,
    {{ dbt_utils.generate_surrogate_key(
        ['a.tx_hash','a.event_index']
    ) }} AS bridge_layerzero_transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    A._inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    evnts A
    JOIN txs b
    ON A.tx_hash = b.tx_hash
    AND A.block_timestamp :: DATE = b.block_timestamp :: DATE
    LEFT JOIN chngs C
    ON A.tx_hash = C.tx_hash
    AND A.block_timestamp :: DATE = C.block_timestamp :: DATE
    AND amount_unadj = C.amount
    AND C.change_module = 'coin'
    LEFT JOIN chngs d
    ON A.tx_hash = d.tx_hash
    AND A.block_timestamp :: DATE = d.block_timestamp :: DATE
    AND d.change_module = 'oft'
    AND d.amount IS NOT NULL
    LEFT JOIN chngs_2 e
    ON A.tx_hash = e.tx_hash
    AND A.block_timestamp :: DATE = e.block_timestamp :: DATE
    AND C.tx_hash IS NULL
    AND d.tx_hash IS NULL
    LEFT JOIN {{ ref('silver__bridge_layerzero_chain_id_seed') }}
    src
    ON source_chain_id = src.chain_id
    LEFT JOIN {{ ref('silver__bridge_layerzero_chain_id_seed') }}
    dst
    ON destination_chain_id = dst.chain_id
