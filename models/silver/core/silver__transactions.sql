{{ config(
  materialized = 'incremental',
  unique_key = ['tx_hash','block_timestamp::DATE'],
  incremental_strategy = 'merge',
  incremental_predicates = ["dynamic_range_predicate", "block_timestamp::DATE"],
  merge_exclude_columns = ["inserted_timestamp"],
  cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE','tx_type'],
  post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash,version,sender);",
  tags = ['core','full_test']
) }}
-- depends_on: {{ ref('bronze__streamline_blocks_tx') }}
-- depends_on: {{ ref('bronze__streamline_transactions') }}
-- depends_on: {{ ref('bronze__streamline_transaction_batch') }}
{% if execute %}
  {% set max_inserted_query %}

  SELECT
    DATEADD('minute', -5, MAX(_inserted_timestamp))
  FROM
    {{ this }}

    {% endset %}
    {% set max_ins = run_query(max_inserted_query) [0] [0] %}
    {% if not max_ins or max_ins == 'None' %}
      {% set max_ins = '2099-01-01' %}
    {% endif %}

    {% set query_blocks %}
    CREATE
    OR REPLACE temporary TABLE silver.transactions__block_intermediate_tmp AS
  SELECT
    DATA,
    partition_key,
    _inserted_timestamp
  FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_blocks_tx') }}
{% else %}
  {{ ref('bronze__streamline_FR_blocks_tx') }}
{% endif %}

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= '{{max_ins}}'
{% endif %}

{% endset %}
{% do run_query(
  query_blocks
) %}
{% set query_tx_batch %}
CREATE
OR REPLACE temporary TABLE silver.transactions_tx_batch_intermediate_tmp AS
SELECT
  DATA,
  VALUE,
  partition_key,
  _inserted_timestamp
FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_transaction_batch') }}
{% else %}
  {{ ref('bronze__streamline_FR_transaction_batch') }}
{% endif %}

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= '{{max_ins}}'
{% endif %}

{% endset %}
{% do run_query(
  query_tx_batch
) %}
{% endif %}

WITH from_blocks AS (
  SELECT
    a.data :block_height :: INT AS block_number,
    TO_TIMESTAMP(
      DATA :block_timestamp :: STRING
    ) AS block_timestamp,
    b.value :hash :: STRING AS tx_hash,
    b.value :version :: INT AS version,
    b.value :success :: BOOLEAN AS success,
    b.value :type :: STRING AS tx_type,
    b.value :accumulator_root_hash :: STRING AS accumulator_root_hash,
    b.value :changes AS changes,
    b.value :epoch :: INT AS epoch,
    b.value :event_root_hash :: STRING AS event_root_hash,
    b.value :events AS events,
    b.value :expiration_timestamp_secs :: bigint AS expiration_timestamp_secs,
    b.value: failed_proposer_indices AS failed_proposer_indices,
    b.value: gas_unit_price :: bigint AS gas_unit_price,
    b.value :gas_used :: INT AS gas_used,
    b.value :id :: STRING AS id,
    b.value :max_gas_amount :: bigint AS max_gas_amount,
    b.value :payload AS payload,
    b.value :payload :function :: STRING AS payload_function,
    b.value :previous_block_votes_bitvec AS previous_block_votes_bitvec,
    b.value :proposer :: STRING AS proposer,
    b.value :round :: INT AS ROUND,
    b.value :sender :: STRING AS sender,
    b.value :signature :: STRING AS signature,
    b.value :state_change_hash :: STRING AS state_change_hash,
    b.value :state_checkpoint_hash :: STRING AS state_checkpoint_hash,
    COALESCE(
      b.value :timestamp,
      DATA :block_timestamp
    ) :: bigint AS TIMESTAMP,
    b.value :vm_status :: STRING AS vm_status,
    {{ dbt_utils.generate_surrogate_key(
      ['tx_hash']
    ) }} AS transactions_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
  FROM
    silver.transactions__block_intermediate_tmp A,
    LATERAL FLATTEN (
      DATA :transactions
    ) b
),
from_transaction_batch AS (
  SELECT
    {# b.block_number, #}
    TO_TIMESTAMP(
      b.value :timestamp :: STRING
    ) AS block_timestamp,
    b.value :hash :: STRING AS tx_hash,
    b.value :version :: INT AS version,
    b.value :success :: BOOLEAN AS success,
    b.value :type :: STRING AS tx_type,
    b.value :accumulator_root_hash :: STRING AS accumulator_root_hash,
    b.value :changes AS changes,
    b.value :epoch :: INT AS epoch,
    b.value :event_root_hash :: STRING AS event_root_hash,
    b.value :events AS events,
    b.value :expiration_timestamp_secs :: bigint AS expiration_timestamp_secs,
    b.value: failed_proposer_indices AS failed_proposer_indices,
    b.value: gas_unit_price :: bigint AS gas_unit_price,
    b.value :gas_used :: INT AS gas_used,
    b.value :id :: STRING AS id,
    b.value :max_gas_amount :: bigint AS max_gas_amount,
    b.value :payload AS payload,
    b.value :payload :function :: STRING AS payload_function,
    b.value :previous_block_votes_bitvec AS previous_block_votes_bitvec,
    b.value :proposer :: STRING AS proposer,
    b.value :round :: INT AS ROUND,
    b.value :sender :: STRING AS sender,
    b.value :signature :: STRING AS signature,
    b.value :state_change_hash :: STRING AS state_change_hash,
    b.value :state_checkpoint_hash :: STRING AS state_checkpoint_hash,
    b.value :timestamp :: bigint AS TIMESTAMP,
    b.value :vm_status :: STRING AS vm_status,
    {{ dbt_utils.generate_surrogate_key(
      ['tx_hash']
    ) }} AS transactions_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    A._inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
  FROM
    silver.transactions_tx_batch_intermediate_tmp A,
    LATERAL FLATTEN(
      A.data
    ) b
),
combo AS (
  SELECT
    *
  FROM
    from_blocks
  UNION ALL
  SELECT
    b.block_number,
    A.*
  FROM
    from_transaction_batch A
    JOIN {{ ref('silver__blocks') }}
    b
    ON A.version BETWEEN b.first_version
    AND b.last_version
)
SELECT
  *
FROM
  combo qualify(ROW_NUMBER() over (PARTITION BY tx_hash
ORDER BY
  _inserted_timestamp DESC)) = 1
