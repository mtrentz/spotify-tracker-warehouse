{% macro map_superset_time_grain_to_postgres_unit(time_grain) %}
    {% set time_grain_upper = time_grain.upper() %}
    {% if time_grain_upper == "P1D" %} {% set postgres_unit = "day" %}
    {% elif time_grain_upper == "P1W" %} {% set postgres_unit = "week" %}
    {% elif time_grain_upper == "P1M" %} {% set postgres_unit = "month" %}
    {% elif time_grain_upper == "P3M" %} {% set postgres_unit = "quarter" %}
    {% elif time_grain_upper == "P1Y" %} {% set postgres_unit = "year" %}
    {% elif time_grain_upper == "PT1S" %} {% set postgres_unit = "second" %}
    {% elif time_grain_upper == "PT1M" %} {% set postgres_unit = "minute" %}
    {% elif time_grain_upper == "PT1H" %} {% set postgres_unit = "hour" %}
    {% else %} {% set postgres_unit = time_grain %}
    {% endif %}
    {{ return(postgres_unit) }}
{% endmacro %}
