COPY INTO MEETUP_DB.BRONZE.BRZ_CATEGORIES (
    category_id,
    category_name,
    shortname,
    sort_name,
    _source_file
)
FROM (
    SELECT
        $1,   -- category_id
        $2,   -- category_name
        $3,   -- shortname
        $4,   -- sort_name
        METADATA$FILENAME
    FROM @MEETUP_DB.BRONZE.S3_STAGE
    (PATTERN => '.*categories.*\\.csv')
)
FILE_FORMAT = (
    TYPE                         = CSV
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER                  = 1
    NULL_IF                      = ('NULL', 'null', '')
    EMPTY_FIELD_AS_NULL          = TRUE
)
ON_ERROR = CONTINUE;