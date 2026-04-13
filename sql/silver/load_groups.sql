MERGE INTO MEETUP_DB.SILVER.SLV_GROUPS AS target
USING (
    WITH transformed AS (
        SELECT
            TRY_TO_NUMBER(group_id)                             AS group_id,
            TRIM(group_name)                                    AS group_name,
            NULLIF(TRIM(description), '')                       AS description,
            NULLIF(TRY_TO_NUMBER(city_id), -1)                  AS city_id,
            TRIM(city)                                          AS city,
            NULLIF(TRIM(state), '')                             AS state_code,
            UPPER(TRIM(country))                                AS country_code,
            NULLIF(TRY_TO_NUMBER(category_id), -1)              AS category_id,
            TRIM(category_name)                                 AS category_name,
            LOWER(TRIM(join_mode))                              AS join_mode,
            LOWER(TRIM(visibility))                             AS visibility,
            TRY_TO_DOUBLE(lat)                                  AS latitude,
            TRY_TO_DOUBLE(lon)                                  AS longitude,
            NULLIF(TRY_TO_NUMBER(members), -1)                  AS member_count,
            NULLIF(TRY_TO_DOUBLE(rating), -1)                   AS rating,
            NULLIF(TRIM(timezone), '')                          AS timezone,
            NULLIF(TRY_TO_NUMBER(utc_offset), -99999)           AS utc_offset_seconds,
            TRIM(urlname)                                       AS url_name,
            TRIM(link)                                          AS group_url,
            NULLIF(TRY_TO_NUMBER(organizer_member_id), -1)      AS organizer_id,
            NULLIF(TRIM(organizer_name), '')                    AS organizer_name,
            NULLIF(TRIM(who), '')                               AS who,
            TRY_TO_TIMESTAMP(created, 'YYYY-MM-DD HH24:MI:SS') AS created_at
        FROM MEETUP_DB.BRONZE.BRZ_GROUPS
        WHERE TRY_TO_NUMBER(group_id) IS NOT NULL
            AND TRIM(group_name) IS NOT NULL
            AND _loaded_at = (SELECT MAX(_loaded_at) FROM MEETUP_DB.BRONZE.BRZ_GROUPS)
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY TRY_TO_NUMBER(group_id)
            ORDER BY created_at DESC NULLS LAST
        ) = 1
    )
    SELECT
        *,
        SHA2(CONCAT_WS('|',
            COALESCE(group_name,                          ''),
            COALESCE(description,                         ''),
            COALESCE(CAST(city_id AS VARCHAR),            ''),
            COALESCE(city,                                ''),
            COALESCE(state_code,                          ''),
            COALESCE(country_code,                        ''),
            COALESCE(CAST(category_id AS VARCHAR),        ''),
            COALESCE(category_name,                       ''),
            COALESCE(join_mode,                           ''),
            COALESCE(visibility,                          ''),
            COALESCE(CAST(latitude AS VARCHAR),           ''),
            COALESCE(CAST(longitude AS VARCHAR),          ''),
            COALESCE(CAST(member_count AS VARCHAR),       ''),
            COALESCE(CAST(rating AS VARCHAR),             ''),
            COALESCE(timezone,                            ''),
            COALESCE(CAST(utc_offset_seconds AS VARCHAR), ''),
            COALESCE(url_name,                            ''),
            COALESCE(group_url,                           ''),
            COALESCE(CAST(organizer_id AS VARCHAR),       ''),
            COALESCE(organizer_name,                      ''),
            COALESCE(who,                                 '')
        ), 256) AS row_hash
    FROM transformed
) AS source
ON target.group_id = source.group_id
WHEN MATCHED AND target.row_hash IS DISTINCT FROM source.row_hash THEN UPDATE SET
    group_name         = source.group_name,
    description        = source.description,
    city_id            = source.city_id,
    city               = source.city,
    state_code         = source.state_code,
    country_code       = source.country_code,
    category_id        = source.category_id,
    category_name      = source.category_name,
    join_mode          = source.join_mode,
    visibility         = source.visibility,
    latitude           = source.latitude,
    longitude          = source.longitude,
    member_count       = source.member_count,
    rating             = source.rating,
    timezone           = source.timezone,
    utc_offset_seconds = source.utc_offset_seconds,
    url_name           = source.url_name,
    group_url          = source.group_url,
    organizer_id       = source.organizer_id,
    organizer_name     = source.organizer_name,
    who                = source.who,
    row_hash           = source.row_hash,
    updated_at         = CURRENT_TIMESTAMP(),
    _merge_action      = 'UPDATE'
WHEN NOT MATCHED THEN INSERT (
    group_id, group_name, description, city_id, city, state_code, country_code,
    category_id, category_name, join_mode, visibility, latitude, longitude,
    member_count, rating, timezone, utc_offset_seconds, url_name, group_url,
    organizer_id, organizer_name, who, created_at,
    row_hash, loaded_at, updated_at, _merge_action
) VALUES (
    source.group_id, source.group_name, source.description, source.city_id,
    source.city, source.state_code, source.country_code, source.category_id,
    source.category_name, source.join_mode, source.visibility, source.latitude,
    source.longitude, source.member_count, source.rating, source.timezone,
    source.utc_offset_seconds, source.url_name, source.group_url,
    source.organizer_id, source.organizer_name, source.who, source.created_at,
    source.row_hash, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 'INSERT'
);
