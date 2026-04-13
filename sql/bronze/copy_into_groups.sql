COPY INTO MEETUP_DB.BRONZE.BRZ_GROUPS (
    group_id,
    category_id,
    category_name,
    category_shortname,
    city_id,
    city,
    country,
    created,
    description,
    group_photo_base_url,
    group_photo_highres_link,
    group_photo_id,
    group_photo_link,
    group_photo_thumb_link,
    group_photo_type,
    join_mode,
    lat,
    link,
    lon,
    members,
    group_name,
    organizer_member_id,
    organizer_name,
    organizer_photo_base_url,
    organizer_photo_highres,
    organizer_photo_id,
    organizer_photo_link,
    organizer_photo_thumb,
    organizer_photo_type,
    rating,
    state,
    timezone,
    urlname,
    utc_offset,
    visibility,
    who,
    _source_file
)
FROM (
    SELECT
        $1,   -- group_id
        $2,   -- category_id
        $3,   -- category.name
        $4,   -- category.shortname
        $5,   -- city_id
        $6,   -- city
        $7,   -- country
        $8,   -- created
        $9,   -- description
        $10,  -- group_photo.base_url
        $11,  -- group_photo.highres_link
        $12,  -- group_photo.photo_id
        $13,  -- group_photo.photo_link
        $14,  -- group_photo.thumb_link
        $15,  -- group_photo.type
        $16,  -- join_mode
        $17,  -- lat
        $18,  -- link
        $19,  -- lon
        $20,  -- members
        $21,  -- group_name
        $22,  -- organizer.member_id
        $23,  -- organizer.name
        $24,  -- organizer.photo.base_url
        $25,  -- organizer.photo.highres_link
        $26,  -- organizer.photo.photo_id
        $27,  -- organizer.photo.photo_link
        $28,  -- organizer.photo.thumb_link
        $29,  -- organizer.photo.type
        $30,  -- rating
        $31,  -- state
        $32,  -- timezone
        $33,  -- urlname
        $34,  -- utc_offset
        $35,  -- visibility
        $36,  -- who
        METADATA$FILENAME
    FROM @MEETUP_DB.BRONZE.S3_STAGE
    (PATTERN => '.*groups\\.csv')
)
FILE_FORMAT = (
    TYPE                         = CSV
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER                  = 1
    NULL_IF                      = ('NULL', 'null', '')
    EMPTY_FIELD_AS_NULL          = TRUE
)
ON_ERROR = CONTINUE;