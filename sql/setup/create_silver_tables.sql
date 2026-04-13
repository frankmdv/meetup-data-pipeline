CREATE OR REPLACE TABLE MEETUP_DB.SILVER.SLV_CATEGORIES (
    category_id     INTEGER         NOT NULL PRIMARY KEY,
    category_name   STRING          NOT NULL,
    short_name      STRING,
    sort_name       STRING,
    row_hash        STRING,
    loaded_at       TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    updated_at      TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    _merge_action   STRING
)
CLUSTER BY (category_id);

CREATE OR REPLACE TABLE MEETUP_DB.SILVER.SLV_CITIES (
    city_id             INTEGER         NOT NULL PRIMARY KEY,
    city_name           STRING          NOT NULL,
    state_code          STRING,
    country_code        STRING          NOT NULL,
    country_name        STRING,
    latitude            FLOAT,
    longitude           FLOAT,
    zip_code            STRING,
    total_members       INTEGER,
    popularity_ranking  INTEGER         COMMENT '0 = ciudad principal; sin criterio claro fuera de EE.UU.',
    row_hash            STRING,
    loaded_at           TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    updated_at          TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    _merge_action       STRING
)
CLUSTER BY (city_id, country_code);

CREATE OR REPLACE TABLE MEETUP_DB.SILVER.SLV_TOPICS (
    topic_id        INTEGER         NOT NULL PRIMARY KEY,
    topic_name      STRING          NOT NULL,
    url_key         STRING          COMMENT 'Slug del tópico en URLs. Ej: sportsfans',
    description     STRING,
    topic_url       STRING,
    total_members   INTEGER,
    parent_topic_id INTEGER         COMMENT 'ID del tópico padre. NULL si es tópico raíz',
    row_hash        STRING,
    loaded_at       TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    updated_at      TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    _merge_action   STRING
)
CLUSTER BY (topic_id);

CREATE OR REPLACE TABLE MEETUP_DB.SILVER.SLV_VENUES (
    venue_id            INTEGER         NOT NULL PRIMARY KEY,
    venue_name          STRING          NOT NULL,
    address             STRING,
    city                STRING,
    state_code          STRING,
    country_code        STRING,
    country_name        STRING,
    zip_code            STRING,
    latitude            FLOAT,
    longitude           FLOAT,
    rating              FLOAT,
    rating_count        INTEGER,
    normalised_rating   FLOAT           COMMENT 'Rating con corrección bayesiana para venues con pocas valoraciones',
    row_hash            STRING,
    loaded_at           TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    updated_at          TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    _merge_action       STRING
)
CLUSTER BY (venue_id, city);

CREATE OR REPLACE TABLE MEETUP_DB.SILVER.SLV_GROUPS (
    group_id            INTEGER         NOT NULL PRIMARY KEY,
    group_name          STRING          NOT NULL,
    description         STRING,
    city_id             INTEGER,
    city                STRING,
    state_code          STRING,
    country_code        STRING,
    category_id         INTEGER,
    category_name       STRING,
    join_mode           STRING          COMMENT 'open o closed',
    visibility          STRING,
    latitude            FLOAT,
    longitude           FLOAT,
    member_count        INTEGER,
    rating              FLOAT,
    timezone            STRING,
    utc_offset_seconds  INTEGER         COMMENT 'Desfase UTC en segundos. Ej: -14400 = UTC-4',
    url_name            STRING          COMMENT 'Slug del grupo en su URL de Meetup',
    group_url           STRING,
    organizer_id        INTEGER,
    organizer_name      STRING,
    who                 STRING          COMMENT 'Nombre que el grupo da a sus miembros. Ej: Members, Explorers',
    created_at          TIMESTAMP_NTZ,
    row_hash            STRING,
    loaded_at           TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    updated_at          TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    _merge_action       STRING
)
CLUSTER BY (city_id, category_id);

CREATE OR REPLACE TABLE MEETUP_DB.SILVER.SLV_EVENTS (
    event_id            STRING          NOT NULL PRIMARY KEY,
    event_name          STRING          NOT NULL,
    description         STRING,
    group_id            INTEGER         NOT NULL,
    group_name          STRING,
    venue_id            INTEGER,
    venue_name          STRING,
    venue_city          STRING,
    venue_country_code  STRING,
    venue_latitude      FLOAT,
    venue_longitude     FLOAT,
    event_status        STRING,
    visibility          STRING,
    has_fee             BOOLEAN,
    fee_amount          FLOAT,
    fee_currency        STRING,
    yes_rsvp_count      INTEGER         COMMENT 'RSVPs confirmados. No equivale necesariamente a asistencia real',
    maybe_rsvp_count    INTEGER,
    waitlist_count      INTEGER         COMMENT 'Personas en espera por haber alcanzado el rsvp_limit',
    rsvp_limit          INTEGER         COMMENT 'Máximo de asistentes. -1 si no hay límite',
    headcount           INTEGER         COMMENT 'Personas contadas físicamente. Distinto de yes_rsvp_count',
    rating_average      FLOAT,
    rating_count        INTEGER,
    event_url           STRING,
    how_to_find_us      STRING          COMMENT 'Instrucciones de acceso escritas por el organizador',
    duration_minutes    INTEGER,
    utc_offset_seconds  INTEGER         COMMENT 'Desfase UTC en segundos. Ej: -14400 = UTC-4',
    event_at            TIMESTAMP_NTZ,
    created_at          TIMESTAMP_NTZ,
    row_hash            STRING,
    loaded_at           TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    updated_at          TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    _merge_action       STRING
)
CLUSTER BY (group_id, event_at);

CREATE OR REPLACE TABLE MEETUP_DB.SILVER.SLV_GROUPS_TOPICS (
    group_id        INTEGER         NOT NULL,
    topic_id        INTEGER         NOT NULL,
    topic_name      STRING,
    url_key         STRING          COMMENT 'Slug del tópico en URLs. Ej: sportsfans',
    loaded_at       TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    _merge_action   STRING,
    PRIMARY KEY (group_id, topic_id)
)
CLUSTER BY (group_id, topic_id);

CREATE OR REPLACE TABLE MEETUP_DB.SILVER.SLV_MEMBERS_TOPICS (
    member_id       INTEGER         NOT NULL,
    topic_id        INTEGER         NOT NULL,
    topic_name      STRING,
    url_key         STRING          COMMENT 'Slug del tópico en URLs. Ej: sportsfans',
    loaded_at       TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    _merge_action   STRING,
    PRIMARY KEY (member_id, topic_id)
)
CLUSTER BY (member_id, topic_id);

CREATE OR REPLACE TABLE MEETUP_DB.SILVER.SLV_MEMBERS (
    member_id       INTEGER         NOT NULL,
    group_id        INTEGER         NOT NULL,
    member_name     STRING          NOT NULL,
    bio             STRING,
    city            STRING,
    state_code      STRING,
    country_code    STRING,
    hometown        STRING,
    profile_url     STRING,
    latitude        FLOAT           COMMENT 'Calculada dividiendo el valor crudo de Bronze entre 100,000,000',
    longitude       FLOAT           COMMENT 'Calculada dividiendo el valor crudo de Bronze entre 100,000,000',
    member_status   STRING,
    joined_at       TIMESTAMP_NTZ   COMMENT 'Fecha en que el miembro se unió a este grupo',
    last_visited_at TIMESTAMP_NTZ   COMMENT 'Última visita del miembro a este grupo',
    row_hash        STRING,
    loaded_at       TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    updated_at      TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    _merge_action   STRING,
    PRIMARY KEY (member_id, group_id)
)
CLUSTER BY (member_id, group_id);
