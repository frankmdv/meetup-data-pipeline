MERGE INTO MEETUP_DB.SILVER.SLV_MEMBERS AS target
USING (
    WITH transformed AS (
        SELECT
            TRY_TO_NUMBER(member_id)                        AS member_id,
            TRY_TO_NUMBER(group_id)                         AS group_id,
            TRIM(member_name)                               AS member_name,
            NULLIF(NULLIF(TRIM(bio), ''), 'not_found')      AS bio,
            TRIM(city)                                      AS city,
            NULLIF(TRIM(state), '')                         AS state_code,
            UPPER(TRIM(country))                            AS country_code,
            NULLIF(NULLIF(TRIM(hometown), ''), 'not_found') AS hometown,
            NULLIF(TRIM(link), '')                          AS profile_url,
            TRY_TO_DOUBLE(lat)  / 100000000.0               AS latitude,
            TRY_TO_DOUBLE(lon)  / 100000000.0               AS longitude,
            LOWER(TRIM(member_status))                      AS member_status,
            TRY_TO_TIMESTAMP(joined,  'DD/MM/YYYY, HH12:MI:SS p.m.') AS joined_at,
            TRY_TO_TIMESTAMP(visited, 'DD/MM/YYYY, HH12:MI:SS p.m.') AS last_visited_at
        FROM MEETUP_DB.BRONZE.BRZ_MEMBERS
        WHERE TRY_TO_NUMBER(member_id) IS NOT NULL
            AND TRY_TO_NUMBER(group_id) IS NOT NULL
            AND TRIM(member_name) IS NOT NULL
            AND TRIM(member_name) <> 'not_found'
            AND _loaded_at = (SELECT MAX(_loaded_at) FROM MEETUP_DB.BRONZE.BRZ_MEMBERS)
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY TRY_TO_NUMBER(member_id), TRY_TO_NUMBER(group_id)
            ORDER BY TRY_TO_TIMESTAMP(visited, 'DD/MM/YYYY, HH12:MI:SS p.m.') DESC
                     NULLS LAST
        ) = 1
    )
    SELECT
        *,
        SHA2(CONCAT_WS('|',
            COALESCE(member_name,                      ''),
            COALESCE(bio,                              ''),
            COALESCE(city,                             ''),
            COALESCE(state_code,                       ''),
            COALESCE(country_code,                     ''),
            COALESCE(hometown,                         ''),
            COALESCE(profile_url,                      ''),
            COALESCE(CAST(latitude AS VARCHAR),        ''),
            COALESCE(CAST(longitude AS VARCHAR),       ''),
            COALESCE(member_status,                    ''),
            COALESCE(CAST(last_visited_at AS VARCHAR), '')
        ), 256) AS row_hash
    FROM transformed
) AS source
ON target.member_id = source.member_id
   AND target.group_id = source.group_id
WHEN MATCHED AND target.row_hash IS DISTINCT FROM source.row_hash THEN UPDATE SET
    member_name     = source.member_name,
    bio             = source.bio,
    city            = source.city,
    state_code      = source.state_code,
    country_code    = source.country_code,
    hometown        = source.hometown,
    profile_url     = source.profile_url,
    latitude        = source.latitude,
    longitude       = source.longitude,
    member_status   = source.member_status,
    last_visited_at = source.last_visited_at,
    row_hash        = source.row_hash,
    updated_at      = CURRENT_TIMESTAMP(),
    _merge_action   = 'UPDATE'
WHEN NOT MATCHED THEN INSERT (
    member_id, group_id, member_name, bio, city, state_code, country_code,
    hometown, profile_url, latitude, longitude, member_status,
    joined_at, last_visited_at,
    row_hash, loaded_at, updated_at, _merge_action
) VALUES (
    source.member_id, source.group_id, source.member_name, source.bio, source.city,
    source.state_code, source.country_code, source.hometown,
    source.profile_url, source.latitude, source.longitude, source.member_status,
    source.joined_at, source.last_visited_at,
    source.row_hash, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 'INSERT'
);
