from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from config.slack_alerts import on_failure_callback, on_success_callback

SQL_DIR = Path(__file__).parent.parent / "sql" / "export"

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
    dag_id="meetup_export_dag",
    description="Exporta las tablas Gold desde Snowflake hacia S3 en formato Parquet",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    on_success_callback=on_success_callback,
    tags=["meetup", "export", "s3"],
) as dag:

    export_fact_events = SnowflakeOperator(
        task_id="export_fact_events",
        snowflake_conn_id=SNOWFLAKE_CONN,
        sql=read_sql("export_fact_events.sql"),
    )

    export_group_performance = SnowflakeOperator(
        task_id="export_group_performance",
        snowflake_conn_id=SNOWFLAKE_CONN,
        sql=read_sql("export_group_performance.sql"),
    )

    export_city_stats = SnowflakeOperator(
        task_id="export_city_stats",
        snowflake_conn_id=SNOWFLAKE_CONN,
        sql=read_sql("export_city_stats.sql"),
    )

    export_topic_trends = SnowflakeOperator(
        task_id="export_topic_trends",
        snowflake_conn_id=SNOWFLAKE_CONN,
        sql=read_sql("export_topic_trends.sql"),
    )

    export_category_summary = SnowflakeOperator(
        task_id="export_category_summary",
        snowflake_conn_id=SNOWFLAKE_CONN,
        sql=read_sql("export_category_summary.sql"),
    )

    export_venue_stats = SnowflakeOperator(
        task_id="export_venue_stats",
        snowflake_conn_id=SNOWFLAKE_CONN,
        sql=read_sql("export_venue_stats.sql"),
    )

    export_member_activity = SnowflakeOperator(
        task_id="export_member_activity",
        snowflake_conn_id=SNOWFLAKE_CONN,
        sql=read_sql("export_member_activity.sql"),
    )

    validate_export = SnowflakeOperator(
        task_id="validate_export",
        snowflake_conn_id=SNOWFLAKE_CONN,
        sql="""
            SELECT 'fact_events'       AS tabla, COUNT(*) AS archivos FROM DIRECTORY(@MEETUP_DB.GOLD.S3_GOLD_STAGE) WHERE RELATIVE_PATH LIKE 'fact_events/%'
            UNION ALL
            SELECT 'group_performance', COUNT(*) FROM DIRECTORY(@MEETUP_DB.GOLD.S3_GOLD_STAGE) WHERE RELATIVE_PATH LIKE 'group_performance/%'
            UNION ALL
            SELECT 'city_stats',        COUNT(*) FROM DIRECTORY(@MEETUP_DB.GOLD.S3_GOLD_STAGE) WHERE RELATIVE_PATH LIKE 'city_stats/%'
            UNION ALL
            SELECT 'topic_trends',      COUNT(*) FROM DIRECTORY(@MEETUP_DB.GOLD.S3_GOLD_STAGE) WHERE RELATIVE_PATH LIKE 'topic_trends/%'
            UNION ALL
            SELECT 'category_summary',  COUNT(*) FROM DIRECTORY(@MEETUP_DB.GOLD.S3_GOLD_STAGE) WHERE RELATIVE_PATH LIKE 'category_summary/%'
            UNION ALL
            SELECT 'venue_stats',       COUNT(*) FROM DIRECTORY(@MEETUP_DB.GOLD.S3_GOLD_STAGE) WHERE RELATIVE_PATH LIKE 'venue_stats/%'
            UNION ALL
            SELECT 'member_activity',   COUNT(*) FROM DIRECTORY(@MEETUP_DB.GOLD.S3_GOLD_STAGE) WHERE RELATIVE_PATH LIKE 'member_activity/%';
        """,
    )

    [
        export_fact_events,
        export_group_performance,
        export_city_stats,
        export_topic_trends,
        export_category_summary,
        export_venue_stats,
        export_member_activity,
    ] >> validate_export
