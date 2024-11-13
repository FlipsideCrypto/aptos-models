{% macro create_aws_aptos_api() %}
    {{ log(
        "Creating integration for target:" ~ target
    ) }}

    {% if target.name == "prod" %}
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_aptos_api api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::490041342817:role/aptos-api-prod-rolesnowflakeudfsAF733095-wEyotLQyFEIl' api_allowed_prefixes = (
            'https://dedvhh9fi1.execute-api.us-east-1.amazonaws.com/prod/'
        ) enabled = TRUE;
{% endset %}
        {% do run_query(sql) %}
        {% elif target.name == "dev" %}
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_aptos_api_dev api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::704693948482:role/aptos-api-stg-rolesnowflakeudfsAF733095-k23uBmqxZRsN' api_allowed_prefixes = (
            'https://9v6g64rv1e.execute-api.us-east-1.amazonaws.com/stg/'
        ) enabled = TRUE;
{% endset %}
        {% do run_query(sql) %}
    {% endif %}
{% endmacro %}
