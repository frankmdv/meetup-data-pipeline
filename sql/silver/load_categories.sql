MERGE INTO MEETUP_DB.SILVER.SLV_CATEGORIES AS target
USING (
    WITH transformed AS (
        SELECT
            TRY_TO_NUMBER(category_id)  AS category_id,
            TRIM(category_name)         AS category_name,
            TRIM(shortname)             AS short_name,
            TRIM(sort_name)             AS sort_name
        FROM MEETUP_DB.BRONZE.BRZ_CATEGORIES
        WHERE TRY_TO_NUMBER(category_id) IS NOT NULL
            AND TRIM(category_name) IS NOT NULL
            AND _loaded_at = (SELECT MAX(_loaded_at) FROM MEETUP_DB.BRONZE.BRZ_CATEGORIES)
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY TRY_TO_NUMBER(category_id)
            ORDER BY TRIM(category_name) ASC
        ) = 1
    )
    SELECT
        *,
        SHA2(CONCAT_WS('|',
            COALESCE(category_name, ''),
            COALESCE(short_name,    ''),
            COALESCE(sort_name,     '')
        ), 256) AS row_hash
    FROM transformed
) AS source
ON target.category_id = source.category_id
WHEN MATCHED AND target.row_hash IS DISTINCT FROM source.row_hash THEN UPDATE SET
    category_name = source.category_name,
    short_name    = source.short_name,
    sort_name     = source.sort_name,
    row_hash      = source.row_hash,
    updated_at    = CURRENT_TIMESTAMP(),
    _merge_action = 'UPDATE'
WHEN NOT MATCHED THEN INSERT (
    category_id, category_name, short_name, sort_name,
    row_hash, loaded_at, updated_at, _merge_action
) VALUES (
    source.category_id, source.category_name, source.short_name, source.sort_name,
    source.row_hash, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 'INSERT'
);
