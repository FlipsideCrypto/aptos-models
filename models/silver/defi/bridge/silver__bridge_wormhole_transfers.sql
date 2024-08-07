{{ config(
    materialized = 'incremental',
    unique_key = "bridge_wormhole_transfers_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash, version, sender, receiver);",
    tags = ['noncore']
) }}

WITH txs AS (

    SELECT
        block_timestamp,
        tx_hash,
        sender,
        payload_function,
        payload
    FROM
        {{ ref('silver__transactions') }}
    WHERE
        payload_function IN (
            '0x576410486a2da45eee6c949c995670112ddf2fbeedab20350d506328eefc9d4f::transfer_tokens::transfer_tokens_entry',
            '0x576410486a2da45eee6c949c995670112ddf2fbeedab20350d506328eefc9d4f::transfer_tokens::transfer_tokens_with_payload_entry',
            '0x576410486a2da45eee6c949c995670112ddf2fbeedab20350d506328eefc9d4f::complete_transfer::submit_vaa_and_register_entry'
        )

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
events AS (
    SELECT
        block_number,
        block_timestamp,
        version,
        tx_hash,
        event_data,
        event_index,
        event_resource,
        _inserted_timestamp
    FROM
        {{ ref('silver__events') }}
    WHERE
        payload_function IN (
            '0x576410486a2da45eee6c949c995670112ddf2fbeedab20350d506328eefc9d4f::transfer_tokens::transfer_tokens_entry',
            '0x576410486a2da45eee6c949c995670112ddf2fbeedab20350d506328eefc9d4f::transfer_tokens::transfer_tokens_with_payload_entry',
            '0x576410486a2da45eee6c949c995670112ddf2fbeedab20350d506328eefc9d4f::complete_transfer::submit_vaa_and_register_entry'
        )
        AND event_type IN (
            '0x1::coin::DepositEvent',
            '0x1::coin::WithdrawEvent'
        )

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
wormhole_transfers AS (
    --wormhole in
    SELECT
        A.block_number,
        A.block_timestamp,
        A.version,
        A.tx_hash,
        'wormhole' AS platform,
        '0x576410486a2da45eee6c949c995670112ddf2fbeedab20350d506328eefc9d4f' AS bridge_address,
        A.event_resource AS event_name,
        'inbound' AS direction,
        b.sender AS tx_sender,
        NULL AS sender,
        b.sender AS receiver,
        NULL AS source_chain_id,
        NULL AS source_chain_name,
        21 AS destination_chain_id,
        'aptos' AS destination_chain_name,
        payload :type_arguments [0] :: STRING AS token_address,
        event_data :amount :: INT AS amount_unadj,
        A.event_index,
        A._inserted_timestamp
    FROM
        events A
        LEFT JOIN txs b
        ON A.tx_hash = b.tx_hash
        AND A.block_timestamp :: DATE = b.block_timestamp :: DATE
    WHERE
        A.event_resource = 'DepositEvent'
        AND b.payload_function = '0x576410486a2da45eee6c949c995670112ddf2fbeedab20350d506328eefc9d4f::complete_transfer::submit_vaa_and_register_entry'
        AND event_data :amount :: INT <> 0
    UNION ALL
        --wormhole out
    SELECT
        A.block_number,
        A.block_timestamp,
        A.version,
        A.tx_hash,
        'wormhole' AS platform,
        '0x576410486a2da45eee6c949c995670112ddf2fbeedab20350d506328eefc9d4f' AS bridge_address,
        A.event_resource AS event_name,
        'outbound' AS direction,
        b.sender AS tx_sender,
        b.sender AS sender,
        (
            CASE
                WHEN LEFT(
                    payload :arguments [2],
                    26
                ) = '0x000000000000000000000000' THEN CONCAT('0x', RIGHT(payload :arguments [2], 40))
                ELSE payload :arguments [2]
            END
        ) AS receiver,
        22 AS source_chain_id,
        'aptos' AS source_chain_name,
        CASE
            WHEN TRY_CAST(
                payload :arguments [1] :: STRING AS INT
            ) IS NOT NULL THEN payload :arguments [1]
            ELSE utils.udf_hex_to_int(RTRIM(payload :arguments [1] :: STRING, '0'))
        END :: INT AS destination_chain_id,
        chain_name AS destination_chain_name,
        payload :type_arguments [0] :: STRING AS token_address,
        CASE
            WHEN A.block_number < 165050375
            AND A.block_timestamp :: DATE <= '2024-04-04' THEN payload :arguments [0]
            ELSE event_data :amount
        END :: INT AS amount_unadj,
        A.event_index,
        A._inserted_timestamp
    FROM
        events A
        LEFT JOIN txs b
        ON A.tx_hash = b.tx_hash
        AND A.block_timestamp :: DATE = b.block_timestamp :: DATE
        LEFT JOIN {{ ref('silver__bridge_wormhole_chain_id_seed') }}
        ON chain_id = destination_chain_id
    WHERE
        A.event_resource = 'WithdrawEvent'
        AND b.payload_function IN (
            '0x576410486a2da45eee6c949c995670112ddf2fbeedab20350d506328eefc9d4f::transfer_tokens::transfer_tokens_entry',
            '0x576410486a2da45eee6c949c995670112ddf2fbeedab20350d506328eefc9d4f::transfer_tokens::transfer_tokens_with_payload_entry'
        )
        AND event_data :amount :: INT <> 0
),
near_addresses AS (
    SELECT
        near_address,
        addr_encoded
    FROM
        crosschain.silver.near_address_encoded
)
SELECT
    t.block_number,
    t.block_timestamp,
    t.version,
    t.tx_hash,
    t.platform,
    t.bridge_address,
    t.event_name,
    t.direction,
    t.tx_sender,
    t.sender,
    CASE
        WHEN destination_chain_name = 'solana' THEN ethereum.utils.udf_hex_to_base58(receiver)
        WHEN destination_chain_name IN (
            'injective',
            'sei'
        ) THEN ethereum.utils.udf_hex_to_bech32(
            receiver,
            SUBSTR(
                destination_chain_name,
                1,
                3
            )
        )
        WHEN destination_chain_name IN (
            'osmosis',
            'xpla'
        ) THEN ethereum.utils.udf_hex_to_bech32(
            receiver,
            SUBSTR(
                destination_chain_name,
                1,
                4
            )
        )
        WHEN destination_chain_name IN (
            'terra',
            'terra2',
            'evmos'
        ) THEN ethereum.utils.udf_hex_to_bech32(
            receiver,
            SUBSTR(
                destination_chain_name,
                1,
                5
            )
        )
        WHEN destination_chain_name IN (
            'cosmoshub',
            'kujira'
        ) THEN ethereum.utils.udf_hex_to_bech32(
            receiver,
            SUBSTR(
                destination_chain_name,
                1,
                6
            )
        )
        WHEN destination_chain_name IN ('near') THEN near_address
        WHEN destination_chain_name IN ('algorand') THEN ethereum.utils.udf_hex_to_algorand(receiver)
        WHEN destination_chain_name IN ('polygon') THEN SUBSTR(
            receiver,
            1,
            42
        )
        ELSE receiver
    END AS receiver,
    t.source_chain_id,
    t.source_chain_name,
    t.destination_chain_id,
    t.destination_chain_name,
    t.token_address,
    t.amount_unadj,
    t.event_index,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','event_index']
    ) }} AS bridge_wormhole_transfers_id,
    -- tx_id is unique but is it enough?
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    t._inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    wormhole_transfers t
    LEFT JOIN near_addresses n
    ON t.receiver = n.addr_encoded
