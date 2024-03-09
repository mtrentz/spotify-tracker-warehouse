select * from {{ target.schema }}.aggregate_top_n_tracks_per_timeframe(10, 'week')
