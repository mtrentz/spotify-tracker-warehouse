{% macro generate_superset_html_album_card(album_name, album_image) %}

    '<div style="display: flex; align-items: center;">'
    || '<img src="'
    || {{ album_image }}
    || '" style="width: 50px; height: 50px; margin-right: 10px;">'
    || '<div>'
    || {{ album_name }}
    || '</div>'

{% endmacro %}
