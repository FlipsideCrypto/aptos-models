{% macro unverify_tokens() %}
  {% if var('HEAL_MODEL', false) and is_incremental() %}
    DELETE FROM {{ this }}
    WHERE LOWER(token_address) NOT IN (
        SELECT DISTINCT LOWER(token_address)
        FROM {{ ref('price__ez_prices_hourly') }}
        WHERE 
          is_verified = TRUE
          AND token_address IS NOT NULL
    );
  {% endif %}
{% endmacro %}
