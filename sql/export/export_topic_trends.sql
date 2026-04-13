COPY INTO @MEETUP_DB.GOLD.S3_GOLD_STAGE/topic_trends/
FROM (
    SELECT
        topic_id,
        topic_name,
        url_key,
        parent_topic_id,
        topic_global_members,
        total_groups,
        cities_covered,
        countries_covered,
        avg_group_size,
        total_group_members,
        total_events,
        avg_event_attendees,
        total_attendees,
        members_interested,
        CAST(_gold_loaded_at AS TIMESTAMP_NTZ) AS _gold_loaded_at
    FROM MEETUP_DB.GOLD.GLD_AGG_TOPIC_TRENDS
)
FILE_FORMAT      = (TYPE = PARQUET SNAPPY_COMPRESSION = TRUE)
OVERWRITE        = TRUE
HEADER           = TRUE
MAX_FILE_SIZE    = 104857600;
