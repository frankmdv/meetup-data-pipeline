MERGE INTO MEETUP_DB.SILVER.SLV_TOPICS AS target
USING (
    WITH transformed AS (
        SELECT
            TRY_TO_NUMBER(topic_id)                     AS topic_id,
            TRIM(topic_name)                            AS topic_name,
            TRIM(urlkey)                                AS url_key,
            NULLIF(TRIM(description), '')               AS description,
            NULLIF(TRIM(link), '')                      AS topic_url,
            NULLIF(TRY_TO_NUMBER(members), -1)          AS total_members,
            NULLIF(TRY_TO_NUMBER(main_topic_id), -1)    AS parent_topic_id
        FROM MEETUP_DB.BRONZE.BRZ_TOPICS
        WHERE TRY_TO_NUMBER(topic_id) IS NOT NULL
            AND TRIM(topic_name) IS NOT NULL
            AND _loaded_at = (SELECT MAX(_loaded_at) FROM MEETUP_DB.BRONZE.BRZ_TOPICS)
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY TRY_TO_NUMBER(topic_id)
            ORDER BY COALESCE(TRY_TO_NUMBER(members), 0) DESC
        ) = 1
    )
    SELECT
        *,
        SHA2(CONCAT_WS('|',
            COALESCE(topic_name,                       ''),
            COALESCE(description,                      ''),
            COALESCE(topic_url,                        ''),
            COALESCE(CAST(total_members AS VARCHAR),   ''),
            COALESCE(CAST(parent_topic_id AS VARCHAR), '')
        ), 256) AS row_hash
    FROM transformed
) AS source
ON target.topic_id = source.topic_id
WHEN MATCHED AND target.row_hash IS DISTINCT FROM source.row_hash THEN UPDATE SET
    topic_name      = source.topic_name,
    description     = source.description,
    topic_url       = source.topic_url,
    total_members   = source.total_members,
    parent_topic_id = source.parent_topic_id,
    row_hash        = source.row_hash,
    updated_at      = CURRENT_TIMESTAMP(),
    _merge_action   = 'UPDATE'
WHEN NOT MATCHED THEN INSERT (
    topic_id, topic_name, url_key, description, topic_url,
    total_members, parent_topic_id,
    row_hash, loaded_at, updated_at, _merge_action
) VALUES (
    source.topic_id, source.topic_name, source.url_key, source.description,
    source.topic_url, source.total_members, source.parent_topic_id,
    source.row_hash, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 'INSERT'
);
