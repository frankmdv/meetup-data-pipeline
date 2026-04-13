COPY INTO @MEETUP_DB.GOLD.S3_GOLD_STAGE/category_summary/
FROM (
    SELECT
        category_id,
        category_name,
        short_name,
        total_groups,
        cities_with_groups,
        countries_with_groups,
        avg_group_size,
        total_memberships,
        avg_group_rating,
        total_events,
        upcoming_events,
        avg_attendees_per_event,
        total_attendees,
        paid_events,
        avg_fee_amount,
        CAST(_gold_loaded_at AS TIMESTAMP_NTZ) AS _gold_loaded_at
    FROM MEETUP_DB.GOLD.GLD_AGG_CATEGORY_SUMMARY
)
FILE_FORMAT      = (TYPE = PARQUET SNAPPY_COMPRESSION = TRUE)
OVERWRITE        = TRUE
HEADER           = TRUE
MAX_FILE_SIZE    = 104857600;
