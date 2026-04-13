COPY INTO @MEETUP_DB.GOLD.S3_GOLD_STAGE/group_performance/
FROM (
    SELECT
        group_id,
        group_name,
        city,
        state_code,
        country_code,
        category_id,
        category_name,
        member_count,
        group_rating,
        join_mode,
        CAST(group_created_at AS TIMESTAMP_NTZ) AS group_created_at,
        total_events,
        upcoming_events,
        past_events,
        avg_attendees_per_event,
        max_attendees_event,
        total_confirmed_attendees,
        avg_rsvp_fill_rate_pct,
        paid_events_count,
        avg_event_fee,
        avg_event_rating,
        distinct_venues_used,
        group_topics,
        total_topics,
        CAST(_gold_loaded_at AS TIMESTAMP_NTZ) AS _gold_loaded_at
    FROM MEETUP_DB.GOLD.GLD_AGG_GROUP_PERFORMANCE
)
FILE_FORMAT      = (TYPE = PARQUET SNAPPY_COMPRESSION = TRUE)
OVERWRITE        = TRUE
HEADER           = TRUE
MAX_FILE_SIZE    = 104857600;
