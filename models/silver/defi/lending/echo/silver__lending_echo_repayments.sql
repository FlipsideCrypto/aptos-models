{{ config(
    materialized = 'incremental',
    unique_key = "lending_echo_repayments_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE', '_inserted_timestamp::DATE'],
    tags = ['noncore']
) }}

WITH events AS (
    SELECT 
        block_number,
        block_timestamp,
        version,
        tx_hash,
        success,
        payload_function,
        event_index,
        event_type,
        event_address,
        event_module,
        event_resource,
        event_data,
        event_data:repayer::string AS payer,
        event_data:user::string AS borrower,
        event_data:amount::number AS amount,
        event_data:reserve::string AS lending_market,  -- reserve is the market
        _inserted_timestamp
    FROM aptos.silver.events
    WHERE event_address = '0xeab7ea4d635b6b6add79d5045c4a45d8148d88287b1cfa1c3b6a4b56f46839ed'
        AND event_module = 'supply_logic' 
        AND event_resource = 'Repay'
{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% else %}
    AND block_timestamp :: DATE >= '2024-08-26'
{% endif %}
),
reserve_token_mapping AS (
    SELECT * FROM (VALUES
        ('0xf8d3506b42a6879680974fc745526c9cbd48b0b0816079aa59f65fc865bdfbf6', '0x1::aptos_coin::aptosCoin'),
        ('0xc2315bdb8f7789e1817ac423e80748075fa68b7e33949bde8c2769a46fdb212', '0xf22bede237a07e121b56d91a491eb7bcdfd1f5907926a9e58338f964a01b17fa::asset::USDC'),
        ('0x66cca033c185547a88494a9c71baa0dd2185212cfe5e79ec2ec0c4040b5c35c0', '0xf22bede237a07e121b56d91a491eb7bcdfd1f5907926a9e58338f964a01b17fa::asset::USDT'),
        ('0x2760ca0d7ed74fec37f1c539fd752b5c684fa011c539a79def7a6c67717ebace', '0x4e1854f6d332c9525e258fb6e66f84b6af8aba687bbcb832a24768c4e175feec::abtc::ABTC'),
        ('0xd03ef5ba3d8742597f3d2ad2707e8800eb6888517ae5563de22b7822e3cf6603', '0x81214a80d82035a190fcb76b6ff3c0145161c3a9f33d137f2bbaee4cfec8a387'),
        ('0x77d97d473f0be79c6b55c7577f92fff6e27cde3e39c473247b76d411dd63a344', '0x8a7403ae3d95f181761cf36742680442c698b49e047350b77a8906ec5168bdae'),
        ('0xb993e4c6f53e1e40d4bd9ae6033fe0c07fcd7e385e9cd9ab6fb54ae4f8eb55b1', '0x357b0b74bc833e95a115ad22604854d6b0fca151cecd94111770e5d6ffc9dc2b'),
        ('0xc9699e980dace9ecd74ef411db855cd532ea7be73ac84c47aaccbb6f82f47a57', '0xbae207659db88bea0cbead6da0ed00aac12edcdda169e591cd41c94180b46f3b'),
        ('0x633786a0316f24a3d79a56571c7970e3465f673962fae42b2e55b95482470efb', '0xaef6a8c3182e076db72d64324617114cacf9a52f28325edc10b483f7f05da0e7')
    ) AS t(reserve, token)
)

SELECT 
    A.block_number,
    A.block_timestamp,
    A.version,
    A.tx_hash,
    A.event_index,
    A.event_address,
    A.event_resource,
    A.payer,
    A.borrower,
    A.amount,
    A.lending_market,  -- The reserve/market identifier
    B.token AS token_address,
    A._inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['A.tx_hash', 'A.event_index']) }} AS lending_echo_repayments_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM events A
LEFT JOIN reserve_token_mapping b
    ON A.lending_market = b.reserve