MERGE INTO MEETUP_DB.SILVER.SLV_EVENTS AS target
USING (
    WITH transformed AS (
        SELECT
            TRIM(event_id)                                              AS event_id,
            TRIM(event_name)                                            AS event_name,
            NULLIF(TRIM(description), '')                               AS description,
            TRY_TO_NUMBER(group_id)                                     AS group_id,
            TRIM(group_name)                                            AS group_name,
            NULLIF(TRY_TO_NUMBER(venue_id), -1)                         AS venue_id,
            NULLIF(TRIM(venue_name), '')                                AS venue_name,
            NULLIF(TRIM(venue_city), '')                                AS venue_city,
            UPPER(NULLIF(TRIM(venue_country), ''))                      AS venue_country_code,
            NULLIF(TRY_TO_DOUBLE(venue_lat), -1)                        AS venue_latitude,
            NULLIF(TRY_TO_DOUBLE(venue_lon), -1)                        AS venue_longitude,
            LOWER(TRIM(event_status))                                   AS event_status,
            LOWER(TRIM(visibility))                                     AS visibility,
            CASE WHEN TRY_TO_NUMBER(fee_required) = 1 THEN TRUE ELSE FALSE END AS has_fee,
            NULLIF(TRY_TO_DOUBLE(fee_amount), -1)                       AS fee_amount,
            NULLIF(TRIM(fee_currency), 'not_found')                     AS fee_currency,
            NULLIF(TRY_TO_NUMBER(yes_rsvp_count), -1)                   AS yes_rsvp_count,
            NULLIF(TRY_TO_NUMBER(maybe_rsvp_count), -1)                 AS maybe_rsvp_count,
            NULLIF(TRY_TO_NUMBER(waitlist_count), -1)                   AS waitlist_count,
            NULLIF(TRY_TO_NUMBER(rsvp_limit), -1)                       AS rsvp_limit,
            NULLIF(TRY_TO_NUMBER(headcount), -1)                        AS headcount,
            NULLIF(TRY_TO_DOUBLE(rating_average), -1)                   AS rating_average,
            NULLIF(TRY_TO_NUMBER(rating_count), -1)                     AS rating_count,
            NULLIF(TRIM(event_url), '')                                 AS event_url,
            NULLIF(TRIM(how_to_find_us), 'not_found')                   AS how_to_find_us,
            NULLIF(ROUND(TRY_TO_NUMBER(duration) / 60), -1)             AS duration_minutes,
            NULLIF(TRY_TO_NUMBER(utc_offset), -99999)                   AS utc_offset_seconds,
            TRY_TO_TIMESTAMP(event_time, 'YYYY-MM-DD HH24:MI:SS')      AS event_at,
            TRY_TO_TIMESTAMP(created, 'YYYY-MM-DD HH24:MI:SS')         AS created_at
        FROM MEETUP_DB.BRONZE.BRZ_EVENTS
        WHERE TRIM(event_id) IS NOT NULL
            AND TRIM(event_name) IS NOT NULL
            AND TRY_TO_NUMBER(group_id) IS NOT NULL
            AND _loaded_at = (SELECT MAX(_loaded_at) FROM MEETUP_DB.BRONZE.BRZ_EVENTS)
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY TRIM(event_id)
            ORDER BY COALESCE(TRY_TO_NUMBER(yes_rsvp_count), 0) DESC
        ) = 1
    )
    SELECT
        *,
        SHA2(CONCAT_WS('|',
            COALESCE(event_name,                           ''),
            COALESCE(description,                          ''),
            COALESCE(event_status,                         ''),
            COALESCE(CAST(venue_id AS VARCHAR),            ''),
            COALESCE(venue_name,                           ''),
            COALESCE(venue_city,                           ''),
            COALESCE(venue_country_code,                   ''),
            COALESCE(CAST(venue_latitude AS VARCHAR),      ''),
            COALESCE(CAST(venue_longitude AS VARCHAR),     ''),
            COALESCE(CAST(event_at AS VARCHAR),            ''),
            COALESCE(CAST(has_fee AS VARCHAR),             ''),
            COALESCE(CAST(fee_amount AS VARCHAR),          ''),
            COALESCE(fee_currency,                         ''),
            COALESCE(CAST(yes_rsvp_count AS VARCHAR),      ''),
            COALESCE(CAST(maybe_rsvp_count AS VARCHAR),    ''),
            COALESCE(CAST(waitlist_count AS VARCHAR),      ''),
            COALESCE(CAST(rsvp_limit AS VARCHAR),          ''),
            COALESCE(CAST(headcount AS VARCHAR),           ''),
            COALESCE(CAST(rating_average AS VARCHAR),      ''),
            COALESCE(CAST(rating_count AS VARCHAR),        ''),
            COALESCE(event_url,                            ''),
            COALESCE(how_to_find_us,                       '')
        ), 256) AS row_hash
    FROM transformed
) AS source
ON target.event_id = source.event_id
WHEN MATCHED AND target.row_hash IS DISTINCT FROM source.row_hash THEN UPDATE SET
    event_name         = source.event_name,
    description        = source.description,
    event_status       = source.event_status,
    venue_id           = source.venue_id,
    venue_name         = source.venue_name,
    venue_city         = source.venue_city,
    venue_country_code = source.venue_country_code,
    venue_latitude     = source.venue_latitude,
    venue_longitude    = source.venue_longitude,
    event_at           = source.event_at,
    has_fee            = source.has_fee,
    fee_amount         = source.fee_amount,
    fee_currency       = source.fee_currency,
    yes_rsvp_count     = source.yes_rsvp_count,
    maybe_rsvp_count   = source.maybe_rsvp_count,
    waitlist_count     = source.waitlist_count,
    rsvp_limit         = source.rsvp_limit,
    headcount          = source.headcount,
    rating_average     = source.rating_average,
    rating_count       = source.rating_count,
    event_url          = source.event_url,
    how_to_find_us     = source.how_to_find_us,
    row_hash           = source.row_hash,
    updated_at         = CURRENT_TIMESTAMP(),
    _merge_action      = 'UPDATE'
WHEN NOT MATCHED THEN INSERT (
    event_id, event_name, description, group_id, group_name, venue_id, venue_name,
    venue_city, venue_country_code, venue_latitude, venue_longitude, event_status,
    visibility, has_fee, fee_amount, fee_currency, yes_rsvp_count, maybe_rsvp_count,
    waitlist_count, rsvp_limit, headcount, rating_average, rating_count,
    event_url, how_to_find_us, duration_minutes, utc_offset_seconds,
    event_at, created_at,
    row_hash, loaded_at, updated_at, _merge_action
) VALUES (
    source.event_id, source.event_name, source.description, source.group_id,
    source.group_name, source.venue_id, source.venue_name, source.venue_city,
    source.venue_country_code, source.venue_latitude, source.venue_longitude,
    source.event_status, source.visibility, source.has_fee, source.fee_amount,
    source.fee_currency, source.yes_rsvp_count, source.maybe_rsvp_count,
    source.waitlist_count, source.rsvp_limit, source.headcount,
    source.rating_average, source.rating_count, source.event_url,
    source.how_to_find_us, source.duration_minutes, source.utc_offset_seconds,
    source.event_at, source.created_at,
    source.row_hash, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 'INSERT'
);
