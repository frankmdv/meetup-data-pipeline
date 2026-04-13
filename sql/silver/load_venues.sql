MERGE INTO MEETUP_DB.SILVER.SLV_VENUES AS target
USING (
    WITH transformed AS (
        SELECT
            TRY_TO_NUMBER(venue_id)                         AS venue_id,
            TRIM(venue_name)                                AS venue_name,
            NULLIF(TRIM(address_1), '')                     AS address,
            TRIM(city)                                      AS city,
            NULLIF(TRIM(state), '')                         AS state_code,
            UPPER(TRIM(country))                            AS country_code,
            TRIM(localized_country_name)                    AS country_name,
            NULLIF(TRIM(zip), '')                           AS zip_code,
            TRY_TO_DOUBLE(lat)                              AS latitude,
            TRY_TO_DOUBLE(lon)                              AS longitude,
            NULLIF(TRY_TO_DOUBLE(rating), -1)               AS rating,
            NULLIF(TRY_TO_NUMBER(rating_count), -1)         AS rating_count,
            NULLIF(TRY_TO_DOUBLE(normalised_rating), -1)    AS normalised_rating
        FROM MEETUP_DB.BRONZE.BRZ_VENUES
        WHERE TRY_TO_NUMBER(venue_id) IS NOT NULL
            AND TRIM(venue_name) IS NOT NULL
            AND _loaded_at = (SELECT MAX(_loaded_at) FROM MEETUP_DB.BRONZE.BRZ_VENUES)
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY TRY_TO_NUMBER(venue_id)
            ORDER BY COALESCE(TRY_TO_DOUBLE(rating), 0) DESC
        ) = 1
    )
    SELECT
        *,
        SHA2(CONCAT_WS('|',
            COALESCE(venue_name,                         ''),
            COALESCE(address,                            ''),
            COALESCE(city,                               ''),
            COALESCE(state_code,                         ''),
            COALESCE(country_code,                       ''),
            COALESCE(country_name,                       ''),
            COALESCE(zip_code,                           ''),
            COALESCE(CAST(latitude AS VARCHAR),          ''),
            COALESCE(CAST(longitude AS VARCHAR),         ''),
            COALESCE(CAST(rating AS VARCHAR),            ''),
            COALESCE(CAST(rating_count AS VARCHAR),      ''),
            COALESCE(CAST(normalised_rating AS VARCHAR), '')
        ), 256) AS row_hash
    FROM transformed
) AS source
ON target.venue_id = source.venue_id
WHEN MATCHED AND target.row_hash IS DISTINCT FROM source.row_hash THEN UPDATE SET
    venue_name        = source.venue_name,
    address           = source.address,
    city              = source.city,
    state_code        = source.state_code,
    country_code      = source.country_code,
    country_name      = source.country_name,
    zip_code          = source.zip_code,
    latitude          = source.latitude,
    longitude         = source.longitude,
    rating            = source.rating,
    rating_count      = source.rating_count,
    normalised_rating = source.normalised_rating,
    row_hash          = source.row_hash,
    updated_at        = CURRENT_TIMESTAMP(),
    _merge_action     = 'UPDATE'
WHEN NOT MATCHED THEN INSERT (
    venue_id, venue_name, address, city, state_code, country_code,
    country_name, zip_code, latitude, longitude,
    rating, rating_count, normalised_rating,
    row_hash, loaded_at, updated_at, _merge_action
) VALUES (
    source.venue_id, source.venue_name, source.address, source.city,
    source.state_code, source.country_code, source.country_name, source.zip_code,
    source.latitude, source.longitude, source.rating, source.rating_count,
    source.normalised_rating,
    source.row_hash, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 'INSERT'
);
