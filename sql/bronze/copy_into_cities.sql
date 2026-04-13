COPY INTO MEETUP_DB.BRONZE.BRZ_CITIES (
    city,
    city_id,
    country,
    distance,
    latitude,
    localized_country_name,
    longitude,
    member_count,
    ranking,
    state,
    zip,
    _source_file
)
FROM (
    SELECT
        $1,   -- city
        $2,   -- city_id
        $3,   -- country
        $4,   -- distance
        $5,   -- latitude
        $6,   -- localized_country_name
        $7,   -- longitude
        $8,   -- member_count
        $9,   -- ranking
        $10,  -- state
        $11,  -- zip
        METADATA$FILENAME
    FROM @MEETUP_DB.BRONZE.S3_STAGE
    (PATTERN => '.*cities.*\\.csv')
)
FILE_FORMAT = (
    TYPE                         = CSV
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER                  = 1
    NULL_IF                      = ('NULL', 'null', '')
    EMPTY_FIELD_AS_NULL          = TRUE
)
ON_ERROR = CONTINUE;