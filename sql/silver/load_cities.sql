MERGE INTO MEETUP_DB.SILVER.SLV_CITIES AS target
USING (
    WITH transformed AS (
        SELECT
            TRY_TO_NUMBER(city_id)                  AS city_id,
            TRIM(city)                              AS city_name,
            NULLIF(TRIM(state), '')                 AS state_code,
            UPPER(TRIM(country))                    AS country_code,
            TRIM(localized_country_name)            AS country_name,
            TRY_TO_DOUBLE(latitude)                 AS latitude,
            TRY_TO_DOUBLE(longitude)                AS longitude,
            NULLIF(TRIM(zip), '')                   AS zip_code,
            NULLIF(TRY_TO_NUMBER(member_count), -1) AS total_members,
            NULLIF(TRY_TO_NUMBER(ranking), -1)      AS popularity_ranking
        FROM MEETUP_DB.BRONZE.BRZ_CITIES
        WHERE TRY_TO_NUMBER(city_id) IS NOT NULL
            AND TRIM(city) IS NOT NULL
            AND TRIM(country) IS NOT NULL
            AND _loaded_at = (SELECT MAX(_loaded_at) FROM MEETUP_DB.BRONZE.BRZ_CITIES)
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY TRY_TO_NUMBER(city_id)
            ORDER BY COALESCE(TRY_TO_NUMBER(member_count), 0) DESC
        ) = 1
    )
    SELECT
        *,
        SHA2(CONCAT_WS('|',
            COALESCE(city_name,                           ''),
            COALESCE(state_code,                          ''),
            COALESCE(zip_code,                            ''),
            COALESCE(CAST(total_members AS VARCHAR),      ''),
            COALESCE(CAST(popularity_ranking AS VARCHAR), '')
        ), 256) AS row_hash
    FROM transformed
) AS source
ON target.city_id = source.city_id
WHEN MATCHED AND target.row_hash IS DISTINCT FROM source.row_hash THEN UPDATE SET
    city_name          = source.city_name,
    state_code         = source.state_code,
    zip_code           = source.zip_code,
    total_members      = source.total_members,
    popularity_ranking = source.popularity_ranking,
    row_hash           = source.row_hash,
    updated_at         = CURRENT_TIMESTAMP(),
    _merge_action      = 'UPDATE'
WHEN NOT MATCHED THEN INSERT (
    city_id, city_name, state_code, country_code, country_name,
    latitude, longitude, zip_code, total_members, popularity_ranking,
    row_hash, loaded_at, updated_at, _merge_action
) VALUES (
    source.city_id, source.city_name, source.state_code, source.country_code,
    source.country_name, source.latitude, source.longitude, source.zip_code,
    source.total_members, source.popularity_ranking,
    source.row_hash, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 'INSERT'
);
