CREATE OR REPLACE TABLE MEETUP_DB.GOLD.GLD_AGG_CATEGORY_SUMMARY_TMP
CLUSTER BY (category_id)
AS
SELECT
    cat.category_id,
    cat.category_name,
    cat.short_name,
    COUNT(DISTINCT g.group_id)                  AS total_groups,
    COUNT(DISTINCT g.city_id)                   AS cities_with_groups,
    COUNT(DISTINCT g.country_code)              AS countries_with_groups,
    AVG(g.member_count)                         AS avg_group_size,
    SUM(g.member_count)                         AS total_memberships,
    AVG(g.rating)                               AS avg_group_rating,
    COUNT(DISTINCT e.event_id)                  AS total_events,
    COUNT(DISTINCT CASE WHEN e.event_status = 'upcoming' THEN e.event_id END) AS upcoming_events,
    AVG(e.confirmed_attendees)                  AS avg_attendees_per_event,
    SUM(e.confirmed_attendees)                  AS total_attendees,
    COUNT(DISTINCT CASE WHEN e.has_fee THEN e.event_id END) AS paid_events,
    AVG(CASE WHEN e.has_fee THEN e.fee_amount END) AS avg_fee_amount,
    CURRENT_TIMESTAMP()                         AS _gold_loaded_at
FROM MEETUP_DB.SILVER.SLV_CATEGORIES cat
LEFT JOIN MEETUP_DB.SILVER.SLV_GROUPS     g ON cat.category_id = g.category_id
LEFT JOIN MEETUP_DB.GOLD.GLD_FACT_EVENTS  e ON g.group_id = e.group_id
GROUP BY
    cat.category_id, cat.category_name, cat.short_name;

ALTER TABLE MEETUP_DB.GOLD.GLD_AGG_CATEGORY_SUMMARY_TMP
    SWAP WITH MEETUP_DB.GOLD.GLD_AGG_CATEGORY_SUMMARY;

DROP TABLE MEETUP_DB.GOLD.GLD_AGG_CATEGORY_SUMMARY_TMP;
