{% macro map_superset_time_grain_to_postgres_unit(time_grain) %}
    {% if time_grain == "p1d" %} {% set postgres_unit = "day" %}
    {% elif time_grain == "p1w" %} {% set postgres_unit = "week" %}
    {% elif time_grain == "p1m" %} {% set postgres_unit = "month" %}
    {% elif time_grain == "p3m" %} {% set postgres_unit = "quarter" %}
    {% elif time_grain == "p1y" %} {% set postgres_unit = "year" %}
    {% elif time_grain == "pt1s" %} {% set postgres_unit = "second" %}
    {% elif time_grain == "pt1m" %} {% set postgres_unit = "minute" %}
    {% elif time_grain == "pt1h" %} {% set postgres_unit = "hour" %}
    {% else %} {% set postgres_unit = time_grain %}
    {% endif %}
    {{ return(postgres_unit) }}
{% endmacro %}
