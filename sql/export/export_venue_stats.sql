COPY INTO @MEETUP_DB.GOLD.S3_GOLD_STAGE/venue_stats/
FROM (
    SELECT
        venue_id,
        venue_name,
        address,
        city,
        state_code,
        country_code,
        latitude,
        longitude,
        venue_rating,
        venue_rating_count,
        normalised_rating,
        total_events_hosted,
        distinct_groups,
        distinct_categories,
        avg_attendees,
        max_attendees,
        total_attendees,
        avg_event_rating,
        paid_events_hosted,
        CAST(_gold_loaded_at AS TIMESTAMP_NTZ) AS _gold_loaded_at
    FROM MEETUP_DB.GOLD.GLD_AGG_VENUE_STATS
)
FILE_FORMAT      = (TYPE = PARQUET SNAPPY_COMPRESSION = TRUE)
OVERWRITE        = TRUE
HEADER           = TRUE
MAX_FILE_SIZE    = 104857600;
