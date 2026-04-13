COPY INTO MEETUP_DB.BRONZE.BRZ_EVENTS (
    event_id,
    created,
    description,
    duration,
    event_url,
    fee_accepts,
    fee_amount,
    fee_currency,
    fee_description,
    fee_label,
    fee_required,
    group_created,
    group_lat,
    group_lon,
    group_id,
    group_join_mode,
    group_name,
    group_urlname,
    group_who,
    headcount,
    how_to_find_us,
    maybe_rsvp_count,
    event_name,
    photo_url,
    rating_average,
    rating_count,
    rsvp_limit,
    event_status,
    event_time,
    updated,
    utc_offset,
    venue_address_1,
    venue_address_2,
    venue_city,
    venue_country,
    venue_id,
    venue_lat,
    venue_localized_country,
    venue_lon,
    venue_name,
    venue_phone,
    venue_repinned,
    venue_state,
    venue_zip,
    visibility,
    waitlist_count,
    why,
    yes_rsvp_count,
    _source_file
)
FROM (
    SELECT
        $1,   -- event_id
        $2,   -- created
        $3,   -- description
        $4,   -- duration
        $5,   -- event_url
        $6,   -- fee.accepts      → fee_accepts
        $7,   -- fee.amount       → fee_amount
        $8,   -- fee.currency     → fee_currency
        $9,   -- fee.description  → fee_description
        $10,  -- fee.label        → fee_label
        $11,  -- fee.required     → fee_required
        $12,  -- group.created    → group_created
        $13,  -- group.group_lat  → group_lat
        $14,  -- group.group_lon  → group_lon
        $15,  -- group_id
        $16,  -- group.join_mode  → group_join_mode
        $17,  -- group.name       → group_name
        $18,  -- group.urlname    → group_urlname
        $19,  -- group.who        → group_who
        $20,  -- headcount
        $21,  -- how_to_find_us
        $22,  -- maybe_rsvp_count
        $23,  -- event_name
        $24,  -- photo_url
        $25,  -- rating.average   → rating_average
        $26,  -- rating.count     → rating_count
        $27,  -- rsvp_limit
        $28,  -- event_status
        $29,  -- event_time
        $30,  -- updated
        $31,  -- utc_offset
        $32,  -- venue.address_1  → venue_address_1
        $33,  -- venue.address_2  → venue_address_2
        $34,  -- venue.city       → venue_city
        $35,  -- venue.country    → venue_country
        $36,  -- venue_id
        $37,  -- venue.lat        → venue_lat
        $38,  -- venue.localized_country_name → venue_localized_country
        $39,  -- venue.lon        → venue_lon
        $40,  -- venue.name       → venue_name
        $41,  -- venue.phone      → venue_phone
        $42,  -- venue.repinned   → venue_repinned
        $43,  -- venue.state      → venue_state
        $44,  -- venue.zip        → venue_zip
        $45,  -- visibility
        $46,  -- waitlist_count
        $47,  -- why
        $48,  -- yes_rsvp_count
        METADATA$FILENAME
    FROM @MEETUP_DB.BRONZE.S3_STAGE
    (PATTERN => '.*events.*\\.csv')
)
FILE_FORMAT = (
    TYPE                         = CSV
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER                  = 1
    NULL_IF                      = ('NULL', 'null', '')
    EMPTY_FIELD_AS_NULL          = TRUE
)
ON_ERROR = CONTINUE;