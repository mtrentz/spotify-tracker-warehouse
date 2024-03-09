select * from {{ target.schema }}.aggregate_top_n_artists_per_timeframe(10, 'week')
