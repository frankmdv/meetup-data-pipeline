COPY INTO MEETUP_DB.BRONZE.BRZ_TOPICS (
    topic_id,
    description,
    link,
    members,
    topic_name,
    urlkey,
    main_topic_id,
    _source_file
)
FROM (
    SELECT
        $1,   -- topic_id
        $2,   -- description
        $3,   -- link
        $4,   -- members
        $5,   -- topic_name
        $6,   -- urlkey
        $7,   -- main_topic_id
        METADATA$FILENAME
    FROM @MEETUP_DB.BRONZE.S3_STAGE
    (PATTERN => '.*topics\\.csv')
)
FILE_FORMAT = (
    TYPE                         = CSV
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER                  = 1
    NULL_IF                      = ('NULL', 'null', '')
    EMPTY_FIELD_AS_NULL          = TRUE
    ENCODING                     = 'ISO-8859-1'
)
ON_ERROR = CONTINUE;