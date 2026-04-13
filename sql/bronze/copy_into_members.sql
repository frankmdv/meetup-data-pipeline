COPY INTO MEETUP_DB.BRONZE.BRZ_MEMBERS (
    member_id,
    bio,
    city,
    country,
    hometown,
    joined,
    lat,
    link,
    lon,
    member_name,
    state,
    member_status,
    visited,
    group_id,
    _source_file
)
FROM (
    SELECT
        $1,   -- member_id
        $2,   -- bio
        $3,   -- city
        $4,   -- country
        $5,   -- hometown
        $6,   -- joined  (formato: DD/MM/YYYY, HH12:MI:SS a.m./p.m.)
        $7,   -- lat     (escalada x100,000,000 — se normaliza en Silver)
        $8,   -- link
        $9,   -- lon     (escalada x100,000,000 — se normaliza en Silver)
        $10,  -- member_name
        $11,  -- state
        $12,  -- member_status
        $13,  -- visited (formato: DD/MM/YYYY, HH12:MI:SS a.m./p.m.)
        $14,  -- group_id
        METADATA$FILENAME
    FROM @MEETUP_DB.BRONZE.S3_STAGE
    (PATTERN => '.*members.*\\.csv')
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