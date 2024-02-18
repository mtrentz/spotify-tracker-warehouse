{% macro generate_superset_html_artist_card(artist_name, artist_image) %}

    '<div style="display: flex; align-items: center;">'
    || '<img src="'
    || {{ artist_image }}
    || '" style="width: 50px; height: 50px; margin-right: 10px;">'
    || '<div>'
    || {{ artist_name }}
    || '</div>'

{% endmacro %}
