MERGE INTO MEETUP_DB.GOLD.GLD_AGG_VENUE_STATS AS target
USING (
    SELECT
        v.venue_id,
        v.venue_name,
        v.address,
        v.city,
        v.state_code,
        v.country_code,
        v.latitude,
        v.longitude,
        v.rating                                    AS venue_rating,
        v.rating_count                              AS venue_rating_count,
        v.normalised_rating,
        COUNT(DISTINCT e.event_id)                  AS total_events_hosted,
        COUNT(DISTINCT e.group_id)                  AS distinct_groups,
        COUNT(DISTINCT e.category_id)               AS distinct_categories,
        AVG(e.confirmed_attendees)                  AS avg_attendees,
        MAX(e.confirmed_attendees)                  AS max_attendees,
        SUM(e.confirmed_attendees)                  AS total_attendees,
        AVG(e.rating_average)                       AS avg_event_rating,
        COUNT(DISTINCT CASE WHEN e.has_fee THEN e.event_id END) AS paid_events_hosted,
        CURRENT_TIMESTAMP()                         AS _gold_loaded_at
    FROM MEETUP_DB.SILVER.SLV_VENUES v
    LEFT JOIN MEETUP_DB.GOLD.GLD_FACT_EVENTS e ON v.venue_id = e.venue_id
    GROUP BY
        v.venue_id, v.venue_name, v.address, v.city, v.state_code, v.country_code,
        v.latitude, v.longitude, v.rating, v.rating_count, v.normalised_rating
) AS source
ON target.venue_id = source.venue_id
WHEN MATCHED AND (
    target.total_events_hosted IS DISTINCT FROM source.total_events_hosted OR
    target.total_attendees     IS DISTINCT FROM source.total_attendees     OR
    target.avg_event_rating    IS DISTINCT FROM source.avg_event_rating    OR
    target.distinct_groups     IS DISTINCT FROM source.distinct_groups
) THEN UPDATE SET
    total_events_hosted = source.total_events_hosted,
    distinct_groups     = source.distinct_groups,
    distinct_categories = source.distinct_categories,
    avg_attendees       = source.avg_attendees,
    max_attendees       = source.max_attendees,
    total_attendees     = source.total_attendees,
    avg_event_rating    = source.avg_event_rating,
    paid_events_hosted  = source.paid_events_hosted,
    _gold_loaded_at     = source._gold_loaded_at
WHEN NOT MATCHED THEN INSERT (
    venue_id, venue_name, address, city, state_code, country_code,
    latitude, longitude, venue_rating, venue_rating_count, normalised_rating,
    total_events_hosted, distinct_groups, distinct_categories,
    avg_attendees, max_attendees, total_attendees, avg_event_rating,
    paid_events_hosted, _gold_loaded_at
) VALUES (
    source.venue_id, source.venue_name, source.address, source.city,
    source.state_code, source.country_code, source.latitude, source.longitude,
    source.venue_rating, source.venue_rating_count, source.normalised_rating,
    source.total_events_hosted, source.distinct_groups, source.distinct_categories,
    source.avg_attendees, source.max_attendees, source.total_attendees,
    source.avg_event_rating, source.paid_events_hosted, source._gold_loaded_at
);
