
{% macro create_udf_bulk_json_rpc() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_bulk_json_rpc(
        json variant
    ) returns text api_integration = {% if target.name == "prod" %}
        aws_aptos_api AS 'https://dedvhh9fi1.execute-api.us-east-1.amazonaws.com/prod/udf_bulk_json_rpc'
    {% else %}
        aws_aptos_api_dev AS 'https://jx15f84555.execute-api.us-east-1.amazonaws.com/dev/udf_bulk_json_rpc'
    {%- endif %};
{% endmacro %}

{% macro create_udf_bulk_rest_api() %}
    {% if target.name == "prod" %}
        CREATE OR REPLACE EXTERNAL FUNCTION streamline.udf_bulk_rest_api(
            json OBJECT
        ) returns ARRAY api_integration = aws_aptos_api AS
            'https://dedvhh9fi1.execute-api.us-east-1.amazonaws.com/prod/udf_bulk_rest_api'
    {% else %}
        CREATE OR REPLACE EXTERNAL FUNCTION streamline.udf_bulk_rest_api(
            json OBJECT
        ) returns ARRAY api_integration = aws_aptos_api_dev AS
            'https://jx15f84555.execute-api.us-east-1.amazonaws.com/dev/udf_bulk_rest_api'
    {%- endif %};
{% endmacro %}

{% macro create_udf_bulk_rest_api_v2() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_bulk_rest_api_v2(
        json OBJECT
    ) returns ARRAY api_integration = {% if target.name == "prod" %}
        aws_aptos_api_prod_v2 AS ''
    {% else %}
        aws_aptos_api_stg_v2 AS 'https://9v6g64rv1e.execute-api.us-east-1.amazonaws.com/stg/udf_bulk_rest_api'
    {%- endif %};
{% endmacro %}