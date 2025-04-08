{% test recency_where(model, field, datepart, interval, where, ignore_time_component=False, group_by_columns = []) %}
  {#
    This is a custom implementation that doesn't depend on other macros.
    It's duplicated from dbt_utils.recency, but modified to include a where clause.
  #}

{% set threshold = 'cast(' ~ dbt.dateadd(datepart, interval * -1, dbt.current_timestamp()) ~ ' as ' ~ ('date' if ignore_time_component else dbt.type_timestamp()) ~ ')'  %}

{% if group_by_columns|length() > 0 %}
  {% set select_gb_cols = group_by_columns|join(' ,') + ', ' %}
  {% set groupby_gb_cols = 'group by ' + group_by_columns|join(',') %}
{% else %}
  {% set select_gb_cols = '' %}
  {% set groupby_gb_cols = '' %}
{% endif %}

with recency as (

    select
      {{ select_gb_cols }}
      {% if ignore_time_component %}
        cast(max({{ field }}) as date) as most_recent
      {%- else %}
        max({{ field }}) as most_recent
      {%- endif %}

    from {{ model }}
    {% if where %}
    where {{ where }}
    {% endif %}

    {{ groupby_gb_cols }}

)

select
    {{ select_gb_cols }}
    most_recent,
    {{ threshold }} as threshold

from recency
where most_recent < {{ threshold }}

{% endtest %}
