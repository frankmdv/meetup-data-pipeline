MERGE INTO MEETUP_DB.GOLD.GLD_FACT_EVENTS AS target
USING (
    SELECT
        e.event_id,
        e.event_name,
        e.event_status,
        e.visibility,
        e.event_at,
        DATE_TRUNC('month', e.event_at)             AS event_month,
        DATE_TRUNC('week', e.event_at)              AS event_week,
        DAYNAME(e.event_at)                         AS event_day_name,
        e.duration_minutes,
        e.has_fee,
        e.fee_amount,
        e.fee_currency,
        COALESCE(e.yes_rsvp_count, 0)               AS confirmed_attendees,
        COALESCE(e.maybe_rsvp_count, 0)             AS maybe_attendees,
        COALESCE(e.waitlist_count, 0)               AS waitlist_count,
        COALESCE(e.rsvp_limit, 0)                   AS rsvp_limit,
        CASE
            WHEN NULLIF(e.rsvp_limit, 0) IS NOT NULL
            THEN ROUND(e.yes_rsvp_count / NULLIF(e.rsvp_limit, 0) * 100, 1)
            ELSE NULL
        END                                         AS rsvp_fill_rate_pct,
        e.rating_average,
        e.rating_count,
        e.group_id,
        g.group_name,
        g.category_id,
        g.category_name,
        g.member_count                              AS group_member_count,
        g.rating                                    AS group_rating,
        g.join_mode,
        g.city_id,
        g.city,
        g.state_code,
        g.country_code,
        g.timezone,
        e.venue_id,
        e.venue_name,
        e.venue_city,
        v.address                                   AS venue_address,
        v.latitude                                  AS venue_latitude,
        v.longitude                                 AS venue_longitude,
        v.normalised_rating                         AS venue_normalised_rating,
        e.event_url,
        CURRENT_TIMESTAMP()                         AS _gold_loaded_at
    FROM MEETUP_DB.SILVER.SLV_EVENTS e
    LEFT JOIN MEETUP_DB.SILVER.SLV_GROUPS  g ON e.group_id = g.group_id
    LEFT JOIN MEETUP_DB.SILVER.SLV_VENUES  v ON e.venue_id = v.venue_id
) AS source
ON target.event_id = source.event_id
WHEN MATCHED AND (
    target.event_status        IS DISTINCT FROM source.event_status        OR
    target.confirmed_attendees IS DISTINCT FROM source.confirmed_attendees OR
    target.maybe_attendees     IS DISTINCT FROM source.maybe_attendees     OR
    target.waitlist_count      IS DISTINCT FROM source.waitlist_count      OR
    target.rsvp_fill_rate_pct  IS DISTINCT FROM source.rsvp_fill_rate_pct  OR
    target.rating_average      IS DISTINCT FROM source.rating_average      OR
    target.rating_count        IS DISTINCT FROM source.rating_count        OR
    target.group_member_count  IS DISTINCT FROM source.group_member_count
) THEN UPDATE SET
    event_status        = source.event_status,
    confirmed_attendees = source.confirmed_attendees,
    maybe_attendees     = source.maybe_attendees,
    waitlist_count      = source.waitlist_count,
    rsvp_fill_rate_pct  = source.rsvp_fill_rate_pct,
    rating_average      = source.rating_average,
    rating_count        = source.rating_count,
    group_member_count  = source.group_member_count,
    _gold_loaded_at     = source._gold_loaded_at
WHEN NOT MATCHED THEN INSERT (
    event_id, event_name, event_status, visibility, event_at,
    event_month, event_week, event_day_name, duration_minutes,
    has_fee, fee_amount, fee_currency, confirmed_attendees, maybe_attendees,
    waitlist_count, rsvp_limit, rsvp_fill_rate_pct, rating_average, rating_count,
    group_id, group_name, category_id, category_name, group_member_count,
    group_rating, join_mode, city_id, city, state_code, country_code, timezone,
    venue_id, venue_name, venue_city, venue_address, venue_latitude,
    venue_longitude, venue_normalised_rating, event_url,
    _gold_loaded_at
) VALUES (
    source.event_id, source.event_name, source.event_status, source.visibility,
    source.event_at, source.event_month, source.event_week, source.event_day_name,
    source.duration_minutes, source.has_fee, source.fee_amount, source.fee_currency,
    source.confirmed_attendees, source.maybe_attendees, source.waitlist_count,
    source.rsvp_limit, source.rsvp_fill_rate_pct, source.rating_average,
    source.rating_count, source.group_id, source.group_name, source.category_id,
    source.category_name, source.group_member_count, source.group_rating,
    source.join_mode, source.city_id, source.city, source.state_code,
    source.country_code, source.timezone, source.venue_id, source.venue_name,
    source.venue_city, source.venue_address, source.venue_latitude,
    source.venue_longitude, source.venue_normalised_rating, source.event_url,
    source._gold_loaded_at
);
