MERGE INTO MEETUP_DB.GOLD.GLD_AGG_GROUP_PERFORMANCE AS target
USING (
    SELECT
        g.group_id,
        g.group_name,
        g.city,
        g.state_code,
        g.country_code,
        g.category_id,
        g.category_name,
        g.member_count,
        g.rating                                        AS group_rating,
        g.join_mode,
        g.created_at                                    AS group_created_at,
        COUNT(DISTINCT e.event_id)                      AS total_events,
        COUNT(DISTINCT CASE WHEN e.event_status = 'upcoming' THEN e.event_id END) AS upcoming_events,
        COUNT(DISTINCT CASE WHEN e.event_status = 'past'     THEN e.event_id END) AS past_events,
        AVG(e.confirmed_attendees)                      AS avg_attendees_per_event,
        MAX(e.confirmed_attendees)                      AS max_attendees_event,
        SUM(e.confirmed_attendees)                      AS total_confirmed_attendees,
        AVG(e.rsvp_fill_rate_pct)                       AS avg_rsvp_fill_rate_pct,
        COUNT(DISTINCT CASE WHEN e.has_fee THEN e.event_id END) AS paid_events_count,
        AVG(CASE WHEN e.has_fee THEN e.fee_amount END)  AS avg_event_fee,
        AVG(e.rating_average)                           AS avg_event_rating,
        COUNT(DISTINCT e.venue_id)                      AS distinct_venues_used,
        LISTAGG(DISTINCT gt.topic_name, ', ')
            WITHIN GROUP (ORDER BY gt.topic_name)       AS group_topics,
        COUNT(DISTINCT gt.topic_id)                     AS total_topics,
        CURRENT_TIMESTAMP()                             AS _gold_loaded_at
    FROM MEETUP_DB.SILVER.SLV_GROUPS g
    LEFT JOIN MEETUP_DB.GOLD.GLD_FACT_EVENTS        e  ON g.group_id = e.group_id
    LEFT JOIN MEETUP_DB.SILVER.SLV_GROUPS_TOPICS    gt ON g.group_id = gt.group_id
    GROUP BY
        g.group_id, g.group_name, g.city, g.state_code, g.country_code,
        g.category_id, g.category_name, g.member_count, g.rating,
        g.join_mode, g.created_at
) AS source
ON target.group_id = source.group_id
WHEN MATCHED AND (
    target.total_events              IS DISTINCT FROM source.total_events              OR
    target.total_confirmed_attendees IS DISTINCT FROM source.total_confirmed_attendees OR
    target.member_count              IS DISTINCT FROM source.member_count              OR
    target.avg_rsvp_fill_rate_pct    IS DISTINCT FROM source.avg_rsvp_fill_rate_pct
) THEN UPDATE SET
    member_count              = source.member_count,
    total_events              = source.total_events,
    upcoming_events           = source.upcoming_events,
    past_events               = source.past_events,
    avg_attendees_per_event   = source.avg_attendees_per_event,
    max_attendees_event       = source.max_attendees_event,
    total_confirmed_attendees = source.total_confirmed_attendees,
    avg_rsvp_fill_rate_pct    = source.avg_rsvp_fill_rate_pct,
    paid_events_count         = source.paid_events_count,
    avg_event_fee             = source.avg_event_fee,
    avg_event_rating          = source.avg_event_rating,
    distinct_venues_used      = source.distinct_venues_used,
    group_topics              = source.group_topics,
    total_topics              = source.total_topics,
    _gold_loaded_at           = source._gold_loaded_at
WHEN NOT MATCHED THEN INSERT (
    group_id, group_name, city, state_code, country_code, category_id,
    category_name, member_count, group_rating, join_mode, group_created_at,
    total_events, upcoming_events, past_events, avg_attendees_per_event,
    max_attendees_event, total_confirmed_attendees, avg_rsvp_fill_rate_pct,
    paid_events_count, avg_event_fee, avg_event_rating, distinct_venues_used,
    group_topics, total_topics, _gold_loaded_at
) VALUES (
    source.group_id, source.group_name, source.city, source.state_code,
    source.country_code, source.category_id, source.category_name,
    source.member_count, source.group_rating, source.join_mode,
    source.group_created_at, source.total_events, source.upcoming_events,
    source.past_events, source.avg_attendees_per_event, source.max_attendees_event,
    source.total_confirmed_attendees, source.avg_rsvp_fill_rate_pct,
    source.paid_events_count, source.avg_event_fee, source.avg_event_rating,
    source.distinct_venues_used, source.group_topics, source.total_topics,
    source._gold_loaded_at
);
