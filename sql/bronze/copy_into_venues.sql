COPY INTO MEETUP_DB.BRONZE.BRZ_VENUES (
    venue_id,
    address_1,
    city,
    country,
    distance,
    lat,
    localized_country_name,
    lon,
    venue_name,
    rating,
    rating_count,
    state,
    zip,
    normalised_rating,
    _source_file
)
FROM (
    SELECT
        $1,   -- venue_id
        $2,   -- address_1
        $3,   -- city
        $4,   -- country
        $5,   -- distance
        $6,   -- lat
        $7,   -- localized_country_name
        $8,   -- lon
        $9,   -- venue_name
        $10,  -- rating
        $11,  -- rating_count
        $12,  -- state
        $13,  -- zip
        $14,  -- normalised_rating
        METADATA$FILENAME
    FROM @MEETUP_DB.BRONZE.S3_STAGE
    (PATTERN => '.*venues.*\\.csv')
)
FILE_FORMAT = (
    TYPE                         = CSV
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER                  = 1
    NULL_IF                      = ('NULL', 'null', '')
    EMPTY_FIELD_AS_NULL          = TRUE
)
ON_ERROR = CONTINUE;