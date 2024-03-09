{% macro map_superset_time_grain_to_postgres_unit(time_grain) %}
    {% if time_grain == "P1D" %} {% set postgres_unit = "day" %}
    {% elif time_grain == "P1W" %} {% set postgres_unit = "week" %}
    {% elif time_grain == "P1M" %} {% set postgres_unit = "month" %}
    {% elif time_grain == "P3M" %} {% set postgres_unit = "quarter" %}
    {% elif time_grain == "P1Y" %} {% set postgres_unit = "year" %}
    {% elif time_grain == "PT1S" %} {% set postgres_unit = "second" %}
    {% elif time_grain == "PT1M" %} {% set postgres_unit = "minute" %}
    {% elif time_grain == "PT1H" %} {% set postgres_unit = "hour" %}
    {% else %} {% set postgres_unit = time_grain %}
    {% endif %}
    {{ return(postgres_unit) }}
{% endmacro %}
