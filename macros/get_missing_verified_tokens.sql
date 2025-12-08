{% macro get_missing_verified_tokens(column_name='token_address') %}
  SELECT DISTINCT LOWER(p.token_address) AS token_address
  FROM {{ ref('price__ez_prices_hourly') }} p
  WHERE p.is_verified = TRUE
    AND p.token_address IS NOT NULL
    AND NOT EXISTS (
        SELECT 1
        FROM {{ this }} t
        WHERE LOWER(t.{{ column_name }}) = LOWER(p.token_address)
    )
{% endmacro %}
