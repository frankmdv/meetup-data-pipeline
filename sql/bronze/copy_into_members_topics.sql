COPY INTO MEETUP_DB.BRONZE.BRZ_MEMBERS_TOPICS (
    topic_id,
    topic_key,
    topic_name,
    member_id,
    _source_file
)
FROM (
    SELECT
        $1,   -- topic_id
        $2,   -- topic_key
        $3,   -- topic_name
        $4,   -- member_id
        METADATA$FILENAME
    FROM @MEETUP_DB.BRONZE.S3_STAGE
    (PATTERN => '.*members_topics.*\\.csv')
)
FILE_FORMAT = (
    TYPE                         = CSV
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER                  = 1
    NULL_IF                      = ('NULL', 'null', '')
    EMPTY_FIELD_AS_NULL          = TRUE
)
ON_ERROR = CONTINUE;