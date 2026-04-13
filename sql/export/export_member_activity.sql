COPY INTO @MEETUP_DB.GOLD.S3_GOLD_STAGE/member_activity/
FROM (
    SELECT
        member_id,
        member_name,
        city,
        country_code,
        member_status,
        CAST(joined_at       AS TIMESTAMP_NTZ) AS joined_at,
        CAST(last_visited_at AS TIMESTAMP_NTZ) AS last_visited_at,
        days_since_last_visit,
        activity_segment,
        total_groups,
        total_topics,
        topics_of_interest,
        CAST(_gold_loaded_at AS TIMESTAMP_NTZ) AS _gold_loaded_at
    FROM MEETUP_DB.GOLD.GLD_AGG_MEMBER_ACTIVITY
)
FILE_FORMAT      = (TYPE = PARQUET SNAPPY_COMPRESSION = TRUE)
OVERWRITE        = TRUE
HEADER           = TRUE
MAX_FILE_SIZE    = 104857600;
