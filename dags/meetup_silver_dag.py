from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from config.slack_alerts import on_failure_callback, on_success_callback

SQL_DIR = Path(__file__).parent.parent / "sql" / "silver"

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
    dag_id="meetup_silver_dag",
    description="Transforma y carga datos desde Bronze a Silver (MERGE upsert)",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    on_success_callback=on_success_callback,
    tags=["meetup", "silver", "transformacion"],
) as dag:
    load_categories = SnowflakeOperator(
        task_id="load_categories",
        snowflake_conn_id=SNOWFLAKE_CONN,
        sql=read_sql("load_categories.sql"),
    )

    load_cities = SnowflakeOperator(
        task_id="load_cities",
        snowflake_conn_id=SNOWFLAKE_CONN,
        sql=read_sql("load_cities.sql"),
    )

    load_topics = SnowflakeOperator(
        task_id="load_topics",
        snowflake_conn_id=SNOWFLAKE_CONN,
        sql=read_sql("load_topics.sql"),
    )

    load_venues = SnowflakeOperator(
        task_id="load_venues",
        snowflake_conn_id=SNOWFLAKE_CONN,
        sql=read_sql("load_venues.sql"),
    )

    load_groups = SnowflakeOperator(
        task_id="load_groups",
        snowflake_conn_id=SNOWFLAKE_CONN,
        sql=read_sql("load_groups.sql"),
    )

    load_members = SnowflakeOperator(
        task_id="load_members",
        snowflake_conn_id=SNOWFLAKE_CONN,
        sql=read_sql("load_members.sql"),
    )

    load_events = SnowflakeOperator(
        task_id="load_events",
        snowflake_conn_id=SNOWFLAKE_CONN,
        sql=read_sql("load_events.sql"),
    )

    load_groups_topics = SnowflakeOperator(
        task_id="load_groups_topics",
        snowflake_conn_id=SNOWFLAKE_CONN,
        sql=read_sql("load_groups_topics.sql"),
    )

    load_members_topics = SnowflakeOperator(
        task_id="load_members_topics",
        snowflake_conn_id=SNOWFLAKE_CONN,
        sql=read_sql("load_members_topics.sql"),
    )

    validate_silver = SnowflakeOperator(
        task_id="validate_silver",
        snowflake_conn_id=SNOWFLAKE_CONN,
        sql="""
            SELECT
                'SLV_GROUPS' AS tabla,
                COUNT(*)     AS total_registros,
                MAX(updated_at) AS ultima_actualizacion
            FROM MEETUP_DB.SILVER.SLV_GROUPS
            UNION ALL
            SELECT 'SLV_EVENTS', COUNT(*), MAX(updated_at)
            FROM MEETUP_DB.SILVER.SLV_EVENTS
            UNION ALL
            SELECT 'SLV_MEMBERS', COUNT(*), MAX(updated_at)
            FROM MEETUP_DB.SILVER.SLV_MEMBERS
            UNION ALL
            SELECT 'SLV_VENUES', COUNT(*), MAX(updated_at)
            FROM MEETUP_DB.SILVER.SLV_VENUES;
        """,
    )

    [
        load_categories,
        load_cities,
        load_topics,
        load_venues,
        load_groups,
        load_members,
        load_events,
        load_groups_topics,
        load_members_topics,
    ] >> validate_silver
