{{ config(
    materialized = 'incremental',
    unique_key = "dex_swaps_tsunami_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['modified_timestamp::DATE'],
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
        event_type
    FROM
        {{ ref(
            'silver__events'
        ) }}
    WHERE
        event_address = '0x1786191d0ce793debfdef9890868abdcdc7053f982ccdd102a72732b3082f31d'
        AND event_resource ILIKE 'SwapEvent%'
        AND success

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    version,
    tx_hash,
    event_index,
    event_address,
    A.event_data :user :: STRING AS swapper,
    A.event_data :deposit_coin_type_info :account_address || '::' || utils.udf_hex_to_string(
        SUBSTRING(
            A.event_data :deposit_coin_type_info :module_name,
            3
        )
    ) || '::' || utils.udf_hex_to_string(
        SUBSTRING(
            A.event_data :deposit_coin_type_info :struct_name,
            3
        )
    ) AS token_in,
    A.event_data :withdraw_coin_type_info :account_address || '::' || utils.udf_hex_to_string(
        SUBSTRING(
            A.event_data :withdraw_coin_type_info :module_name,
            3
        )
    ) || '::' || utils.udf_hex_to_string(
        SUBSTRING(
            A.event_data :withdraw_coin_type_info :struct_name,
            3
        )
    ) AS token_out,
    A.event_data :deposit_amount :: INT AS amount_in_unadj,
    A.event_data :withdraw_amount :: INT AS amount_out_unadj,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','event_index']
    ) }} AS dex_swaps_tsunami_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    evnts A
