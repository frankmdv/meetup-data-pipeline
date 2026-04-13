COPY INTO @MEETUP_DB.GOLD.S3_GOLD_STAGE/city_stats/
FROM (
    SELECT
        city_id,
        city_name,
        state_code,
        country_code,
        country_name,
        latitude,
        longitude,
        city_registered_members,
        popularity_ranking,
        total_groups,
        distinct_categories,
        avg_members_per_group,
        largest_group_size,
        total_group_memberships,
        avg_group_rating,
        total_events,
        upcoming_events,
        avg_event_attendees,
        total_attendees,
        paid_events,
        distinct_venues,
        CAST(_gold_loaded_at AS TIMESTAMP_NTZ) AS _gold_loaded_at
    FROM MEETUP_DB.GOLD.GLD_AGG_CITY_STATS
)
FILE_FORMAT      = (TYPE = PARQUET SNAPPY_COMPRESSION = TRUE)
OVERWRITE        = TRUE
HEADER           = TRUE
MAX_FILE_SIZE    = 104857600;
