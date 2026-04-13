CREATE OR REPLACE TABLE MEETUP_DB.GOLD.GLD_AGG_TOPIC_TRENDS_TMP
CLUSTER BY (parent_topic_id)
AS
SELECT
    t.topic_id,
    t.topic_name,
    t.url_key,
    t.parent_topic_id,
    t.total_members                             AS topic_global_members,
    COUNT(DISTINCT gt.group_id)                 AS total_groups,
    COUNT(DISTINCT g.city_id)                   AS cities_covered,
    COUNT(DISTINCT g.country_code)              AS countries_covered,
    AVG(g.member_count)                         AS avg_group_size,
    SUM(g.member_count)                         AS total_group_members,
    COUNT(DISTINCT e.event_id)                  AS total_events,
    AVG(e.confirmed_attendees)                  AS avg_event_attendees,
    SUM(e.confirmed_attendees)                  AS total_attendees,
    COUNT(DISTINCT mt.member_id)                AS members_interested,
    CURRENT_TIMESTAMP()                         AS _gold_loaded_at
FROM MEETUP_DB.SILVER.SLV_TOPICS t
LEFT JOIN MEETUP_DB.SILVER.SLV_GROUPS_TOPICS  gt ON t.topic_id = gt.topic_id
LEFT JOIN MEETUP_DB.SILVER.SLV_GROUPS          g ON gt.group_id = g.group_id
LEFT JOIN MEETUP_DB.GOLD.GLD_FACT_EVENTS       e ON g.group_id = e.group_id
LEFT JOIN MEETUP_DB.SILVER.SLV_MEMBERS_TOPICS mt ON t.topic_id = mt.topic_id
GROUP BY
    t.topic_id, t.topic_name, t.url_key, t.parent_topic_id, t.total_members;

ALTER TABLE MEETUP_DB.GOLD.GLD_AGG_TOPIC_TRENDS_TMP
    SWAP WITH MEETUP_DB.GOLD.GLD_AGG_TOPIC_TRENDS;

DROP TABLE MEETUP_DB.GOLD.GLD_AGG_TOPIC_TRENDS_TMP;
