CREATE OR REPLACE TABLE MEETUP_DB.GOLD.GLD_AGG_MEMBER_ACTIVITY_TMP
CLUSTER BY (country_code, member_status)
AS
SELECT
    m.member_id,
    m.member_name,
    m.city,
    m.country_code,
    m.member_status,
    m.joined_at,
    m.last_visited_at,
    DATEDIFF('day', m.last_visited_at, CURRENT_DATE())  AS days_since_last_visit,
    CASE
        WHEN DATEDIFF('day', m.last_visited_at, CURRENT_DATE()) <= 30  THEN 'active'
        WHEN DATEDIFF('day', m.last_visited_at, CURRENT_DATE()) <= 90  THEN 'at_risk'
        WHEN DATEDIFF('day', m.last_visited_at, CURRENT_DATE()) <= 365 THEN 'dormant'
        ELSE 'churned'
    END                                                 AS activity_segment,
    COUNT(DISTINCT m.group_id)                          AS total_groups,
    COUNT(DISTINCT mt.topic_id)                         AS total_topics,
    LISTAGG(DISTINCT mt.topic_name, ', ')
        WITHIN GROUP (ORDER BY mt.topic_name)           AS topics_of_interest,
    CURRENT_TIMESTAMP()                                 AS _gold_loaded_at
FROM MEETUP_DB.SILVER.SLV_MEMBERS m
LEFT JOIN MEETUP_DB.SILVER.SLV_MEMBERS_TOPICS mt ON m.member_id = mt.member_id
GROUP BY
    m.member_id, m.member_name, m.city, m.country_code,
    m.member_status, m.joined_at, m.last_visited_at;

ALTER TABLE MEETUP_DB.GOLD.GLD_AGG_MEMBER_ACTIVITY_TMP
    SWAP WITH MEETUP_DB.GOLD.GLD_AGG_MEMBER_ACTIVITY;

DROP TABLE MEETUP_DB.GOLD.GLD_AGG_MEMBER_ACTIVITY_TMP;
