CREATE OR REPLACE TABLE MEETUP_DB.BRONZE.BRZ_CATEGORIES (
    category_id         STRING,
    category_name       STRING,
    shortname           STRING,
    sort_name           STRING,
    _loaded_at          TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    _source_file        STRING          NOT NULL
)
CLUSTER BY (category_id);

CREATE OR REPLACE TABLE MEETUP_DB.BRONZE.BRZ_CITIES (
    city                    STRING,
    city_id                 STRING  COMMENT 'En ciudades de EE.UU. coincide con el código ZIP',
    country                 STRING,
    distance                STRING  COMMENT 'Millas desde el punto de referencia de la búsqueda original del dataset',
    latitude                STRING,
    localized_country_name  STRING,
    longitude               STRING,
    member_count            STRING,
    ranking                 STRING  COMMENT '0 indica ciudad principal; sin ordenamiento claro fuera de EE.UU.',
    state                   STRING,
    zip                     STRING,
    _loaded_at              TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    _source_file            STRING          NOT NULL
)
CLUSTER BY (city_id);

CREATE OR REPLACE TABLE MEETUP_DB.BRONZE.BRZ_TOPICS (
    topic_id            STRING,
    description         STRING,
    link                STRING,
    members             STRING,
    topic_name          STRING,
    urlkey              STRING  COMMENT 'Slug del tópico en URLs. Ej: sportsfans',
    main_topic_id       STRING  COMMENT 'ID del tópico padre. NULL si es tópico raíz',
    _loaded_at          TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    _source_file        STRING          NOT NULL
)
CLUSTER BY (topic_id);

CREATE OR REPLACE TABLE MEETUP_DB.BRONZE.BRZ_GROUPS (
    group_id                    STRING,
    category_id                 STRING,
    category_name               STRING,
    category_shortname          STRING,
    city_id                     STRING,
    city                        STRING,
    country                     STRING,
    created                     STRING,
    description                 STRING,
    group_photo_base_url        STRING,
    group_photo_highres_link    STRING,
    group_photo_id              STRING,
    group_photo_link            STRING,
    group_photo_thumb_link      STRING,
    group_photo_type            STRING  COMMENT 'Origen de la foto: event o member',
    join_mode                   STRING  COMMENT 'open = cualquiera puede unirse, closed = requiere aprobación',
    lat                         STRING,
    link                        STRING,
    lon                         STRING,
    members                     STRING,
    group_name                  STRING,
    organizer_member_id         STRING,
    organizer_name              STRING,
    organizer_photo_base_url    STRING,
    organizer_photo_highres     STRING,
    organizer_photo_id          STRING,
    organizer_photo_link        STRING,
    organizer_photo_thumb       STRING,
    organizer_photo_type        STRING,
    rating                      STRING,
    state                       STRING,
    timezone                    STRING,
    urlname                     STRING  COMMENT 'Slug único del grupo en su URL de Meetup',
    utc_offset                  STRING  COMMENT 'Desfase UTC en milisegundos. Ej: -14400 = UTC-4',
    visibility                  STRING,
    who                         STRING  COMMENT 'Nombre que el grupo le da a sus miembros. Ej: Members, Explorers',
    _loaded_at                  TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    _source_file                STRING          NOT NULL
)
CLUSTER BY (group_id, city_id);

CREATE OR REPLACE TABLE MEETUP_DB.BRONZE.BRZ_GROUPS_TOPICS (
    topic_id            STRING,
    topic_key           STRING,
    topic_name          STRING,
    group_id            STRING,
    _loaded_at          TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    _source_file        STRING          NOT NULL
)
CLUSTER BY (group_id, topic_id);

CREATE OR REPLACE TABLE MEETUP_DB.BRONZE.BRZ_MEMBERS_TOPICS (
    topic_id            STRING,
    topic_key           STRING,
    topic_name          STRING,
    member_id           STRING,
    _loaded_at          TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    _source_file        STRING          NOT NULL
)
CLUSTER BY (member_id, topic_id);

CREATE OR REPLACE TABLE MEETUP_DB.BRONZE.BRZ_VENUES (
    venue_id                    STRING,
    address_1                   STRING,
    city                        STRING,
    country                     STRING,
    distance                    STRING,
    lat                         STRING,
    localized_country_name      STRING,
    lon                         STRING,
    venue_name                  STRING,
    rating                      STRING,
    rating_count                STRING,
    state                       STRING,
    zip                         STRING,
    normalised_rating           STRING  COMMENT 'Rating con corrección bayesiana para venues con pocas valoraciones',
    _loaded_at                  TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    _source_file                STRING          NOT NULL
)
CLUSTER BY (venue_id, city);

CREATE OR REPLACE TABLE MEETUP_DB.BRONZE.BRZ_EVENTS (
    event_id                    STRING,
    created                     STRING,
    description                 STRING,
    duration                    STRING  COMMENT 'Duración en milisegundos. Dividir entre 60000 para minutos',
    event_url                   STRING,
    fee_accepts                 STRING,
    fee_amount                  STRING,
    fee_currency                STRING,
    fee_description             STRING,
    fee_label                   STRING,
    fee_required                STRING  COMMENT '1 = pago obligatorio, 0 = opcional, -1 = no aplica',
    group_created               STRING,
    group_lat                   STRING,
    group_lon                   STRING,
    group_id                    STRING,
    group_join_mode             STRING,
    group_name                  STRING,
    group_urlname               STRING,
    group_who                   STRING  COMMENT 'Nombre que el grupo da a sus miembros',
    headcount                   STRING  COMMENT 'Personas contadas físicamente en el evento. Distinto de yes_rsvp_count',
    how_to_find_us              STRING  COMMENT 'Instrucciones de acceso escritas por el organizador',
    maybe_rsvp_count            STRING,
    event_name                  STRING,
    photo_url                   STRING,
    rating_average              STRING,
    rating_count                STRING,
    rsvp_limit                  STRING  COMMENT 'Máximo de asistentes. -1 si no hay límite',
    event_status                STRING,
    event_time                  STRING,
    updated                     STRING,
    utc_offset                  STRING  COMMENT 'Desfase UTC en milisegundos. Ej: -14400 = UTC-4',
    venue_address_1             STRING,
    venue_address_2             STRING,
    venue_city                  STRING,
    venue_country               STRING,
    venue_id                    STRING,
    venue_lat                   STRING,
    venue_localized_country     STRING,
    venue_lon                   STRING,
    venue_name                  STRING,
    venue_phone                 STRING,
    venue_repinned              STRING  COMMENT '1 si el venue fue reubicado manualmente en el mapa',
    venue_state                 STRING,
    venue_zip                   STRING,
    visibility                  STRING,
    waitlist_count              STRING  COMMENT 'Personas en espera por haber alcanzado el rsvp_limit',
    why                         STRING  COMMENT 'Texto de invitación escrito por el organizador',
    yes_rsvp_count              STRING  COMMENT 'RSVPs confirmados. No equivale necesariamente a asistencia real',
    _loaded_at                  TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    _source_file                STRING          NOT NULL
)
CLUSTER BY (group_id, venue_id);

CREATE OR REPLACE TABLE MEETUP_DB.BRONZE.BRZ_MEMBERS (
    member_id           STRING,
    bio                 STRING,
    city                STRING,
    country             STRING,
    hometown            STRING,
    joined              STRING  COMMENT 'Fecha en que el miembro se unió a este grupo (no a Meetup.com)',
    lat                 STRING  COMMENT 'Escalada x100,000,000. Ej: 4072000000 = 40.72°',
    link                STRING,
    lon                 STRING  COMMENT 'Escalada x100,000,000. Ej: -7400000000 = -74.00°',
    member_name         STRING,
    state               STRING,
    member_status       STRING,
    visited             STRING  COMMENT 'Última visita del miembro a este grupo. Formato: DD/MM/YYYY, HH12:MI:SS a.m./p.m.',
    group_id            STRING,
    _loaded_at          TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    _source_file        STRING          NOT NULL
)
CLUSTER BY (member_id, group_id);
