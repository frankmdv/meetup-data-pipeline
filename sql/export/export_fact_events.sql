COPY INTO @MEETUP_DB.GOLD.S3_GOLD_STAGE/fact_events/
FROM (
    SELECT
        event_id,
        event_name,
        event_status,
        visibility,
        CAST(event_at    AS TIMESTAMP_NTZ) AS event_at,
        CAST(event_month AS TIMESTAMP_NTZ) AS event_month,
        CAST(event_week  AS TIMESTAMP_NTZ) AS event_week,
        event_day_name,
        duration_minutes,
        has_fee,
        fee_amount,
        fee_currency,
        confirmed_attendees,
        maybe_attendees,
        waitlist_count,
        rsvp_limit,
        rsvp_fill_rate_pct,
        rating_average,
        rating_count,
        group_id,
        group_name,
        category_id,
        category_name,
        group_member_count,
        group_rating,
        join_mode,
        city_id,
        city,
        state_code,
        country_code,
        timezone,
        venue_id,
        venue_name,
        venue_city,
        venue_address,
        venue_latitude,
        venue_longitude,
        venue_normalised_rating,
        event_url,
        CAST(_gold_loaded_at AS TIMESTAMP_NTZ) AS _gold_loaded_at
    FROM MEETUP_DB.GOLD.GLD_FACT_EVENTS
)
FILE_FORMAT      = (TYPE = PARQUET SNAPPY_COMPRESSION = TRUE)
OVERWRITE        = TRUE
HEADER           = TRUE
MAX_FILE_SIZE    = 104857600;
