CREATE OR REPLACE TABLE MEETUP_DB.GOLD.GLD_AGG_CITY_STATS_TMP
CLUSTER BY (country_code)
AS
SELECT
    c.city_id,
    c.city_name,
    c.state_code,
    c.country_code,
    c.country_name,
    c.latitude,
    c.longitude,
    c.total_members                             AS city_registered_members,
    c.popularity_ranking,
    COUNT(DISTINCT g.group_id)                  AS total_groups,
    COUNT(DISTINCT g.category_id)               AS distinct_categories,
    AVG(g.member_count)                         AS avg_members_per_group,
    MAX(g.member_count)                         AS largest_group_size,
    SUM(g.member_count)                         AS total_group_memberships,
    AVG(g.rating)                               AS avg_group_rating,
    COUNT(DISTINCT e.event_id)                  AS total_events,
    COUNT(DISTINCT CASE WHEN e.event_status = 'upcoming' THEN e.event_id END) AS upcoming_events,
    AVG(e.confirmed_attendees)                  AS avg_event_attendees,
    SUM(e.confirmed_attendees)                  AS total_attendees,
    COUNT(DISTINCT CASE WHEN e.has_fee THEN e.event_id END) AS paid_events,
    COUNT(DISTINCT e.venue_id)                  AS distinct_venues,
    CURRENT_TIMESTAMP()                         AS _gold_loaded_at
FROM MEETUP_DB.SILVER.SLV_CITIES c
LEFT JOIN MEETUP_DB.SILVER.SLV_GROUPS     g ON c.city_id = g.city_id
LEFT JOIN MEETUP_DB.GOLD.GLD_FACT_EVENTS  e ON g.group_id = e.group_id
GROUP BY
    c.city_id, c.city_name, c.state_code, c.country_code, c.country_name,
    c.latitude, c.longitude, c.total_members, c.popularity_ranking;

ALTER TABLE MEETUP_DB.GOLD.GLD_AGG_CITY_STATS_TMP
    SWAP WITH MEETUP_DB.GOLD.GLD_AGG_CITY_STATS;

DROP TABLE MEETUP_DB.GOLD.GLD_AGG_CITY_STATS_TMP;
