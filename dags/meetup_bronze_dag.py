from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from config.slack_alerts import on_failure_callback, on_success_callback

SQL_DIR = Path(__file__).parent.parent / "sql" / "bronze"

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": on_failure_callback,
}

SNOWFLAKE_CONN = "snowflake_default"


def read_sql(filename: str) -> str:
    return (SQL_DIR / filename).read_text()


with DAG(
    dag_id="meetup_bronze_dag",
    description="Carga archivos CSV desde S3 a las tablas Bronze (append-only)",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    on_success_callback=on_success_callback,
    tags=["meetup", "bronze", "ingesta"],
) as dag:
    load_categories = SnowflakeOperator(
        task_id="load_categories",
        snowflake_conn_id=SNOWFLAKE_CONN,
        sql=read_sql("copy_into_categories.sql"),
    )

    load_cities = SnowflakeOperator(
        task_id="load_cities",
        snowflake_conn_id=SNOWFLAKE_CONN,
        sql=read_sql("copy_into_cities.sql"),
    )

    load_topics = SnowflakeOperator(
        task_id="load_topics",
        snowflake_conn_id=SNOWFLAKE_CONN,
        sql=read_sql("copy_into_topics.sql"),
    )

    load_groups = SnowflakeOperator(
        task_id="load_groups",
        snowflake_conn_id=SNOWFLAKE_CONN,
        sql=read_sql("copy_into_groups.sql"),
    )

    load_groups_topics = SnowflakeOperator(
        task_id="load_groups_topics",
        snowflake_conn_id=SNOWFLAKE_CONN,
        sql=read_sql("copy_into_groups_topics.sql"),
    )

    load_members = SnowflakeOperator(
        task_id="load_members",
        snowflake_conn_id=SNOWFLAKE_CONN,
        sql=read_sql("copy_into_members.sql"),
    )

    load_members_topics = SnowflakeOperator(
        task_id="load_members_topics",
        snowflake_conn_id=SNOWFLAKE_CONN,
        sql=read_sql("copy_into_members_topics.sql"),
    )

    load_venues = SnowflakeOperator(
        task_id="load_venues",
        snowflake_conn_id=SNOWFLAKE_CONN,
        sql=read_sql("copy_into_venues.sql"),
    )

    load_events = SnowflakeOperator(
        task_id="load_events",
        snowflake_conn_id=SNOWFLAKE_CONN,
        sql=read_sql("copy_into_events.sql"),
    )

    validate_bronze = SnowflakeOperator(
        task_id="validate_bronze",
        snowflake_conn_id=SNOWFLAKE_CONN,
        sql="""
            SELECT
                'BRZ_GROUPS' AS tabla,
                COUNT(*)       AS filas_ultimo_batch,
                MAX(_loaded_at) AS ultimo_batch
            FROM MEETUP_DB.BRONZE.BRZ_GROUPS
            WHERE _loaded_at = (SELECT MAX(_loaded_at) FROM MEETUP_DB.BRONZE.BRZ_GROUPS)
            UNION ALL
            SELECT 'BRZ_EVENTS', COUNT(*), MAX(_loaded_at)
            FROM MEETUP_DB.BRONZE.BRZ_EVENTS
            WHERE _loaded_at = (SELECT MAX(_loaded_at) FROM MEETUP_DB.BRONZE.BRZ_EVENTS)
            UNION ALL
            SELECT 'BRZ_MEMBERS', COUNT(*), MAX(_loaded_at)
            FROM MEETUP_DB.BRONZE.BRZ_MEMBERS
            WHERE _loaded_at = (SELECT MAX(_loaded_at) FROM MEETUP_DB.BRONZE.BRZ_MEMBERS)
            UNION ALL
            SELECT 'BRZ_VENUES', COUNT(*), MAX(_loaded_at)
            FROM MEETUP_DB.BRONZE.BRZ_VENUES
            WHERE _loaded_at = (SELECT MAX(_loaded_at) FROM MEETUP_DB.BRONZE.BRZ_VENUES);
        """,
    )

    (
        [
            load_categories,
            load_cities,
            load_topics,
            load_groups,
            load_groups_topics,
            load_members,
            load_members_topics,
            load_venues,
            load_events,
        ]
        >> validate_bronze
        )

