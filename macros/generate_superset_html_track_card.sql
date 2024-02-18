{% macro generate_superset_html_track_card(
    track_name, all_track_artists, album_image
) %}

    '<div style="display: flex; align-items: center;">'
    || '<img src="'
    || {{ album_image }}
    || '" style="width: 50px; height: 50px; margin-right: 10px;">'
    || '<div>'
    || {{ track_name }}
    || '<br><span style="color: #4b5563;">'
    || {{ all_track_artists }}
    || '</span></div>'
    || '</div>'

{% endmacro %}
