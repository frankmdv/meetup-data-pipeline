# Meetup Data Pipeline — Prueba Tecnica RappiPay

Este proyecto es un pipeline de datos completo construido sobre el dataset de Meetup. La idea era mostrar como se veria un flujo real en una empresa de datos: desde que llega el dato crudo hasta tenerlo disponible para consumo, con automatizacion, alertas y exportacion a S3.

El diseño sigue una arquictetura Medallon en tres capas (Bronze → Silver → Gold), orquestado con Apache Airflow sobre Astronomer y corriendo las transformaciones en Snowflake.

---

## Como correrlo

### Prerequisitos

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) corriendo
- [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli) instalado
- Bucket S3 con una integración de almacenamiento configurada en Snowflake

### 1. Configurar variables de entorno

```bash
cp .env.example .env
```

Completa los valores en `.env` con tus credenciales de Snowflake, el webhook de Slack y las claves de AWS.

### 2. Crear los objetos en Snowflake

Ejecuta los scripts de `sql/setup/` en orden desde un worksheet de Snowflake:

```
init_database.sql          → crea la base de datos y los schemas
s3_integration.sql         → configura la integracion con S3 y los stages de Bronze y Gold
create_bronze_tables.sql
create_silver_tables.sql
create_gold_tables.sql
```

El script `s3_integration.sql` genera un `STORAGE_AWS_IAM_USER_ARN` y un `STORAGE_AWS_EXTERNAL_ID` que hay que agregar en la politica de confianza del rol IAM que tiene acceso al bucket.

### 3. Levantar Airflow

```bash
astro dev start
```

La UI queda disponible en `http://localhost:8080` con usuario `admin` y contraseña `admin`.

### 4. Activar el pipeline

En la UI de Airflow, activa el DAG `meetup_master_dag`. Corre cada 15 minutos y dispara el resto de DAGs en secuencia. Los DAGs hijos (`meetup_bronze_dag`, `meetup_silver_dag`, `meetup_gold_dag`, `meetup_export_dag`) deben estar activos tambien pero no tienen schedule propio.

---

## Que tecnologias se usaron

| Componente | Eleccion |
|---|---|
| Data Warehouse | Snowflake |
| Orquestacion | Apache Airflow 3.x via Astronomer CLI |
| Almacenamiento | AWS S3 |
| Alertas | Slack Webhooks |
| Exportacion | Parquet con compresion Snappy |

Use Astronomer porque con un solo `astro dev start` se tiene scheduler, triggerer y webserver listos, sin configurar nada manualmente. Para una prueba con tiempo limitado agiliza la implementación de la prueba.

---

## Como funciona el pipeline

```
S3 (CSVs con datos de Meetup)
        │
        ▼
    Bronze  →  datos crudos, sin tocar
        │
        ▼
    Silver  →  limpios, normalizados, sin duplicados
        │
        ▼
      Gold  →  agregaciones listas para analisis
        │
        ▼
  S3 (Parquet)  →  exportacion final
```

Cada capa tiene una sola responsabilidad. Bronze no transforma nada, Silver no agrega, Gold no toca datos crudos. Eso facilita el debugging cuando algo sale mal.

### Bronze
Los datos llegan como CSVs a S3. Snowflake los carga con `COPY INTO` directo a las tablas de Bronze. No se aplica ninguna transformacion — si el dato viene con errores, asi queda registrado. Cada fila tiene `_loaded_at` y `_source_file` para saber de que batch viene.

### Silver
Aca es donde se limpia el dato. Cada tabla pasa por un `MERGE INTO` que:
- Solo procesa el ultimo batch de Bronze, no rescana todo el historico
- Deduplica usando `ROW_NUMBER()` sobre la clave primaria
- Detecta cambios con un hash de los campos (`SHA2`) — si el hash no cambio, no se actualiza nada
- Hace upsert: inserta si es nuevo, actualiza solo si cambio algo

### Gold
Las tablas de Gold son las que consume el negocio. Hay dos patrones segun el tipo de tabla:

- **MERGE**: para tablas que se actualizan con frecuencia (eventos, grupos, venues). Se upsertea solo lo que cambio.
- **SWAP**: para agregaciones que se tienen que reconstruir completas. Se crea una tabla temporal, se llena, y se hace swap atomico con la tabla real. Asi nunca hay un momento en que la tabla este incompleta.

| Tabla | Que responde |
|---|---|
| `GLD_FACT_EVENTS` | Todo sobre cada evento — quien lo organizo, donde fue, cuanta gente asistio, si era pago |
| `GLD_AGG_GROUP_PERFORMANCE` | Que tan activos son los grupos, cuanta gente los llena, como los califican |
| `GLD_AGG_CITY_STATS` | Que ciudades tienen mas actividad, mas grupos, mas asistentes |
| `GLD_AGG_TOPIC_TRENDS` | Que temas estan creciendo y en cuantos paises tienen grupos |
| `GLD_AGG_CATEGORY_SUMMARY` | Rendimiento por categoria: Tech, Sports, Arts, etc. |
| `GLD_AGG_VENUE_STATS` | Que lugares son los mas usados y mejor calificados |
| `GLD_AGG_MEMBER_ACTIVITY` | Segmentacion de miembros segun que tan activos estan (`active`, `at_risk`, `dormant`, `churned`) |

### Export
Las 7 tablas Gold se exportan a S3 en Parquet con compresion Snappy cada vez que corre el pipeline. Con `OVERWRITE = TRUE` siempre queda el snapshot mas reciente.

---

## Orquestacion

Hay un DAG maestro que corre cada 15 minutos y dispara todo en secuencia:

```
meetup_master_dag  (cada 15 min)
    │
    ├── generate_traffic   → sube eventos y miembros nuevos a S3
    ├── trigger_bronze     → carga los CSVs a Snowflake
    ├── trigger_silver     → limpia y transforma
    ├── trigger_gold       → genera las tablas de consumo
    └── trigger_export     → exporta a S3 en Parquet
```

Los DAGs hijos no tienen schedule propio, solo el master los dispara. Se uso `deferrable=True` en los triggers para que no ocupen un worker slot mientras esperan — eso evita deadlocks cuando el entorno tiene paralelismo limitado.

### meetup_master_dag
El orquestador principal. Corre cada 15 minutos y ejecuta las 5 etapas en secuencia. Si cualquier etapa falla, las siguientes no corren.

### meetup_bronze_dag
Carga los 9 archivos CSV desde S3 a las tablas Bronze usando `COPY INTO`. Todas las cargas corren en paralelo y al final corre una validacion que confirma cuantas filas llegaron en el ultimo batch. Snowflake lleva historial de que archivos cargo — si un archivo no cambio, no lo vuelve a cargar.

```
load_categories ┐
load_cities     │
load_topics     │
load_groups     ├──► validate_bronze
load_groups_topics │
load_members    │
load_members_topics│
load_venues     │
load_events     ┘
```

### meetup_silver_dag
Transforma y carga cada tabla desde Bronze a Silver usando `MERGE INTO`. Todas las tablas corren en paralelo porque cada una lee de su propia tabla Bronze sin depender de las demas. Al final valida que los conteos cuadren.

```
load_categories ┐
load_cities     │
load_topics     │
load_venues     ├──► validate_silver
load_groups     │
load_members    │
load_events     │
load_groups_topics │
load_members_topics┘
```

### meetup_gold_dag
Genera las 7 tablas Gold. `GLD_FACT_EVENTS` corre primero porque las otras 5 agregaciones la usan como fuente. `GLD_AGG_MEMBER_ACTIVITY` es independiente y corre en paralelo desde el inicio.

```
generate_fact_events ──► generate_group_performance ┐
                     └── generate_venue_stats        │
                     └── generate_city_stats         ├──► validate_gold
                     └── generate_topic_trends       │
                     └── generate_category_summary   │
generate_member_activity ───────────────────────────┘
```

### meetup_export_dag
Exporta las 7 tablas Gold a S3 en formato Parquet con compresion Snappy. Todas las exportaciones corren en paralelo y al final valida que los archivos llegaron al bucket.

```
export_fact_events       ┐
export_group_performance │
export_city_stats        │
export_topic_trends      ├──► validate_export
export_category_summary  │
export_venue_stats       │
export_member_activity   ┘
```

### Alertas a Slack
Si cualquier tarea falla, llega un mensaje al canal con el nombre del DAG, la tarea que fallo y el link directo a los logs. Cuando el pipeline completa exitosamente tambien llega una notificacion con la duracion total.

---

## Generacion de datos sinteticos

Para que la automatizacion cada 15 minutos tenga sentido, el pipeline genera datos nuevos antes de cada carga:
- **+50 eventos** nuevos con fechas futuras, asignados a grupos existentes
- **+100 miembros** nuevos uniendose a grupos reales

Eso simula la llegada de datos frescos en cada ejecucion y hace visible el flujo de punta a punta.

---

## Diccionario de Datos - Capa GOLD

El diccionario de datos cubre unicamente la capa Gold porque es la que esta pensada para consumo analitico. Bronze almacena los datos tal como llegan de la fuente y Silver los limpia y normaliza, pero ninguna de las dos esta diseñada para que alguien las consulte directamente. Gold es donde se aplican los calculos, las agregaciones y los joins necesarios para responder preguntas de negocio, por eso es la capa relevante que se documentó en detalle.

### `GLD_FACT_EVENTS`
Un registro por evento con toda la informacion consolidada — grupo, venue, ubicacion y metricas de asistencia en una sola fila.

| Campo | Tipo | Que es |
|---|---|---|
| `event_id` | STRING PK | ID unico del evento |
| `event_name` | STRING | Titulo del evento |
| `event_status` | STRING | `upcoming`, `past` o `cancelled` |
| `visibility` | STRING | Si es publico o privado |
| `event_at` | TIMESTAMP_NTZ | Cuando ocurre el evento |
| `event_month` | TIMESTAMP_NTZ | Mes del evento, util para agrupar en dashboards |
| `event_week` | TIMESTAMP_NTZ | Semana del evento |
| `event_day_name` | STRING | Dia de la semana (Monday, Tuesday...) |
| `duration_minutes` | INTEGER | Cuanto dura en minutos |
| `has_fee` | BOOLEAN | Si tiene costo de entrada |
| `fee_amount` | FLOAT | Cuanto cuesta |
| `fee_currency` | STRING | Moneda del costo |
| `confirmed_attendees` | INTEGER | Cuantos dijeron "yes" al RSVP |
| `maybe_attendees` | INTEGER | Cuantos dijeron "maybe" |
| `waitlist_count` | INTEGER | Cuantos estan en lista de espera |
| `rsvp_limit` | INTEGER | Capacidad maxima del evento |
| `rsvp_fill_rate_pct` | FLOAT | Que porcentaje de la capacidad se lleno |
| `rating_average` | FLOAT | Calificacion promedio de los asistentes |
| `rating_count` | INTEGER | Cuantas calificaciones recibio |
| `group_id` | INTEGER | Grupo que organizo el evento |
| `group_name` | STRING | Nombre del grupo |
| `category_id` | INTEGER | Categoria del grupo |
| `category_name` | STRING | Nombre de la categoria |
| `group_member_count` | INTEGER | Cuantos miembros tiene el grupo |
| `group_rating` | FLOAT | Calificacion del grupo |
| `join_mode` | STRING | Si el grupo es abierto o por aprobacion |
| `city_id` | INTEGER | Ciudad del grupo |
| `city` | STRING | Nombre de la ciudad |
| `state_code` | STRING | Estado o provincia |
| `country_code` | STRING | Codigo de pais |
| `timezone` | STRING | Zona horaria del evento |
| `venue_id` | INTEGER | Lugar donde ocurre el evento |
| `venue_name` | STRING | Nombre del lugar |
| `venue_city` | STRING | Ciudad del lugar |
| `venue_address` | STRING | Direccion fisica |
| `venue_latitude` | FLOAT | Latitud del lugar |
| `venue_longitude` | FLOAT | Longitud del lugar |
| `venue_normalised_rating` | FLOAT | Calificacion del lugar normalizada |
| `event_url` | STRING | Link publico al evento en Meetup |
| `_gold_loaded_at` | TIMESTAMP_NTZ | Cuando se actualizo este registro |

---

### `GLD_AGG_GROUP_PERFORMANCE`
Un registro por grupo con el historial completo de su actividad.

| Campo | Tipo | Que es |
|---|---|---|
| `group_id` | INTEGER PK | ID unico del grupo |
| `group_name` | STRING | Nombre del grupo |
| `city` | STRING | Ciudad donde opera |
| `state_code` | STRING | Estado o provincia |
| `country_code` | STRING | Pais |
| `category_id` | INTEGER | Categoria del grupo |
| `category_name` | STRING | Nombre de la categoria |
| `member_count` | INTEGER | Miembros actuales |
| `group_rating` | FLOAT | Calificacion general |
| `join_mode` | STRING | `open` o `approval` |
| `group_created_at` | TIMESTAMP_NTZ | Cuando se creo el grupo |
| `total_events` | INTEGER | Total de eventos organizados |
| `upcoming_events` | INTEGER | Eventos programados |
| `past_events` | INTEGER | Eventos realizados |
| `avg_attendees_per_event` | FLOAT | Asistentes promedio por evento |
| `max_attendees_event` | INTEGER | El evento mas concurrido |
| `total_confirmed_attendees` | INTEGER | Total de asistencias acumuladas |
| `avg_rsvp_fill_rate_pct` | FLOAT | Que tan llenos quedan sus eventos en promedio |
| `paid_events_count` | INTEGER | Cuantos eventos han cobrado entrada |
| `avg_event_fee` | FLOAT | Costo promedio de entrada |
| `avg_event_rating` | FLOAT | Calificacion promedio de sus eventos |
| `distinct_venues_used` | INTEGER | Cuantos lugares distintos han usado |
| `group_topics` | STRING | Lista de temas del grupo |
| `total_topics` | INTEGER | Cuantos temas tiene asociados |
| `_gold_loaded_at` | TIMESTAMP_NTZ | Ultima actualizacion |

---

### `GLD_AGG_CITY_STATS`
Estadisticas por ciudad, se reconstruye completa en cada ejecucion.

| Campo | Tipo | Que es |
|---|---|---|
| `city_id` | INTEGER | ID de la ciudad |
| `city_name` | STRING | Nombre de la ciudad |
| `state_code` | STRING | Estado o provincia |
| `country_code` | STRING | Codigo de pais |
| `country_name` | STRING | Nombre del pais |
| `latitude` | FLOAT | Latitud |
| `longitude` | FLOAT | Longitud |
| `city_registered_members` | INTEGER | Miembros de Meetup registrados en esa ciudad |
| `popularity_ranking` | INTEGER | Ranking de la ciudad en Meetup |
| `total_groups` | INTEGER | Grupos activos en la ciudad |
| `distinct_categories` | INTEGER | Variedad tematica, cuantas categorias hay |
| `avg_members_per_group` | FLOAT | Tamaño promedio de los grupos |
| `largest_group_size` | INTEGER | El grupo mas grande de la ciudad |
| `total_group_memberships` | INTEGER | Total de membresias sumando todos los grupos |
| `avg_group_rating` | FLOAT | Calificacion promedio de los grupos |
| `total_events` | INTEGER | Eventos totales en la ciudad |
| `upcoming_events` | INTEGER | Eventos proximos |
| `avg_event_attendees` | FLOAT | Asistentes promedio por evento |
| `total_attendees` | INTEGER | Total de asistencias acumuladas |
| `paid_events` | INTEGER | Eventos que cobraron entrada |
| `distinct_venues` | INTEGER | Lugares distintos utilizados |
| `_gold_loaded_at` | TIMESTAMP_NTZ | Ultima reconstruccion |

---

### `GLD_AGG_TOPIC_TRENDS`
Que tan adoptado esta cada tema y en cuantos lugares del mundo tiene presencia.

| Campo | Tipo | Que es |
|---|---|---|
| `topic_id` | INTEGER | ID del tema |
| `topic_name` | STRING | Nombre del tema (ej: Machine Learning) |
| `url_key` | STRING | Identificador del tema en URLs de Meetup |
| `parent_topic_id` | INTEGER | Tema padre, permite ver jerarquias |
| `topic_global_members` | INTEGER | Miembros globales del tema segun Meetup |
| `total_groups` | INTEGER | Cuantos grupos usan este tema |
| `cities_covered` | INTEGER | En cuantas ciudades hay grupos de este tema |
| `countries_covered` | INTEGER | En cuantos paises hay grupos de este tema |
| `avg_group_size` | FLOAT | Tamaño promedio de esos grupos |
| `total_group_members` | INTEGER | Total de miembros en grupos de este tema |
| `total_events` | INTEGER | Eventos organizados por esos grupos |
| `avg_event_attendees` | FLOAT | Asistentes promedio por evento |
| `total_attendees` | INTEGER | Total de asistencias acumuladas |
| `members_interested` | INTEGER | Miembros que tienen este tema como interes personal |
| `_gold_loaded_at` | TIMESTAMP_NTZ | Ultima reconstruccion |

---

### `GLD_AGG_CATEGORY_SUMMARY`
Resumen de rendimiento por categoria, Tech, Sports, Arts, etc.

| Campo | Tipo | Que es |
|---|---|---|
| `category_id` | INTEGER | ID de la categoria |
| `category_name` | STRING | Nombre completo |
| `short_name` | STRING | Nombre abreviado |
| `total_groups` | INTEGER | Grupos en esta categoria |
| `cities_with_groups` | INTEGER | Ciudades con al menos un grupo |
| `countries_with_groups` | INTEGER | Paises con al menos un grupo |
| `avg_group_size` | FLOAT | Tamaño promedio de los grupos |
| `total_memberships` | INTEGER | Total de membresias sumando todos los grupos |
| `avg_group_rating` | FLOAT | Calificacion promedio |
| `total_events` | INTEGER | Total de eventos organizados |
| `upcoming_events` | INTEGER | Eventos proximos |
| `avg_attendees_per_event` | FLOAT | Asistentes promedio |
| `total_attendees` | INTEGER | Total de asistencias acumuladas |
| `paid_events` | INTEGER | Eventos que cobraron entrada |
| `avg_fee_amount` | FLOAT | Costo promedio de entrada |
| `_gold_loaded_at` | TIMESTAMP_NTZ | Ultima reconstruccion |

---

### `GLD_AGG_VENUE_STATS`
Historial de uso de cada lugar, se actualiza con cada nuevo evento.

| Campo | Tipo | Que es |
|---|---|---|
| `venue_id` | INTEGER PK | ID unico del lugar |
| `venue_name` | STRING | Nombre del lugar |
| `address` | STRING | Direccion fisica |
| `city` | STRING | Ciudad |
| `state_code` | STRING | Estado o provincia |
| `country_code` | STRING | Pais |
| `latitude` | FLOAT | Latitud |
| `longitude` | FLOAT | Longitud |
| `venue_rating` | FLOAT | Calificacion propia del lugar |
| `venue_rating_count` | INTEGER | Cuantas calificaciones recibio |
| `normalised_rating` | FLOAT | Calificacion normalizada a escala estandar |
| `total_events_hosted` | INTEGER | Cuantos eventos se han hecho ahi |
| `distinct_groups` | INTEGER | Cuantos grupos distintos lo han usado |
| `distinct_categories` | INTEGER | Diversidad de categorias que han pasado por ese lugar |
| `avg_attendees` | FLOAT | Asistentes promedio por evento |
| `max_attendees` | INTEGER | El evento mas concurrido que alojo |
| `total_attendees` | INTEGER | Total de asistencias acumuladas |
| `avg_event_rating` | FLOAT | Calificacion promedio de los eventos realizados ahi |
| `paid_events_hosted` | INTEGER | Eventos de pago que alojo |
| `_gold_loaded_at` | TIMESTAMP_NTZ | Ultima actualizacion |

---

### `GLD_AGG_MEMBER_ACTIVITY`
Segmentacion de miembros segun actividad reciente, se recalcula en cada ejecucion.

| Campo | Tipo | Que es |
|---|---|---|
| `member_id` | INTEGER PK | ID unico del miembro |
| `member_name` | STRING | Nombre |
| `city` | STRING | Ciudad donde vive |
| `country_code` | STRING | Pais |
| `member_status` | STRING | Estado en Meetup |
| `joined_at` | TIMESTAMP_NTZ | Cuando se unio a Meetup |
| `last_visited_at` | TIMESTAMP_NTZ | Ultima vez que entro a la plataforma |
| `days_since_last_visit` | INTEGER | Dias desde su ultima visita, calculado en cada run |
| `activity_segment` | STRING | `active` ≤30d / `at_risk` ≤90d / `dormant` ≤365d / `churned` >365d |
| `total_groups` | INTEGER | Cuantos grupos tiene |
| `total_topics` | INTEGER | Cuantos temas de interes tiene registrados |
| `topics_of_interest` | STRING | Lista de sus temas de interes |
| `_gold_loaded_at` | TIMESTAMP_NTZ | Ultima reconstruccion |

---

## Estructura del proyecto

```
.
├── dags/
│   ├── meetup_master_dag.py      # El que corre cada 15 min y dispara todo
│   ├── meetup_bronze_dag.py      # Carga los CSVs desde S3
│   ├── meetup_silver_dag.py      # Limpieza y upsert
│   ├── meetup_gold_dag.py        # Agregaciones y fact table
│   └── meetup_export_dag.py      # Exporta a S3 en Parquet
├── sql/
│   ├── setup/                    # DDLs e integracion con S3, se ejecutan una sola vez
│   ├── bronze/                   # COPY INTO por tabla
│   ├── silver/                   # MERGE con deteccion de cambios
│   ├── gold/                     # Fact table y agregaciones
│   └── export/                   # Exportacion a S3
├── config/
│   ├── slack_alerts.py           # Callbacks de Slack para fallos y exitos
│   └── s3_config.py              # Constantes del bucket S3
├── scripts/
│   └── generate_traffic.py       # Genera datos sinteticos cada 15 min
├── Dockerfile                    # Astronomer Runtime 3.1
├── requirements.txt              # Dependencias Python
└── .env                          # Conexiones y variables (no se versiona)
```

---

## Algunas decisiones de diseño

- **El pipeline no reprocesa todo en cada run.** Silver filtra solo el ultimo batch de Bronze. Eso lo hace eficiente aunque la tabla tenga millones de filas historicas.
- **Los updates en Silver solo ocurren si algo cambio.** El hash detecta si el dato es igual al anterior, y si es asi, no se toca. Evita writes innecesarios en Snowflake.
- **Gold nunca tiene datos incompletos.** Las tablas que se reconstruyen usan un swap atomico — la tabla vieja y la nueva se intercambian en un solo paso, sin downtime.
- **Si Bronze recibe datos malformados, el pipeline no se detiene.** `ON_ERROR = CONTINUE` permite que filas con errores se salten sin bloquear el batch completo.
