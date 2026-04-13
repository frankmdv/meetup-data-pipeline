MERGE INTO MEETUP_DB.SILVER.SLV_MEMBERS_TOPICS AS target
USING (
    SELECT
        TRY_TO_NUMBER(member_id)    AS member_id,
        TRY_TO_NUMBER(topic_id)     AS topic_id,
        TRIM(topic_name)            AS topic_name,
        TRIM(topic_key)             AS url_key
    FROM MEETUP_DB.BRONZE.BRZ_MEMBERS_TOPICS
    WHERE TRY_TO_NUMBER(member_id) IS NOT NULL
        AND TRY_TO_NUMBER(topic_id) IS NOT NULL
        AND _loaded_at = (SELECT MAX(_loaded_at) FROM MEETUP_DB.BRONZE.BRZ_MEMBERS_TOPICS)
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY TRY_TO_NUMBER(member_id), TRY_TO_NUMBER(topic_id)
        ORDER BY TRIM(topic_name) ASC
    ) = 1
) AS source
ON target.member_id = source.member_id AND target.topic_id = source.topic_id
WHEN NOT MATCHED THEN INSERT (
    member_id, topic_id, topic_name, url_key,
    loaded_at, _merge_action
) VALUES (
    source.member_id, source.topic_id, source.topic_name, source.url_key,
    CURRENT_TIMESTAMP(), 'INSERT'
);
