from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from config.slack_alerts import on_failure_callback, on_success_callback

SQL_DIR = Path(__file__).parent.parent / "sql" / "gold"

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
    dag_id="meetup_gold_dag",
    description="Genera y actualiza las tablas Gold desde Silver (fact + agregaciones)",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    on_success_callback=on_success_callback,
    tags=["meetup", "gold", "agregacion"],
) as dag:

    generate_fact_events = SnowflakeOperator(
        task_id="generate_fact_events",
        snowflake_conn_id=SNOWFLAKE_CONN,
        sql=read_sql("generate_fact_events.sql"),
    )

    generate_member_activity = SnowflakeOperator(
        task_id="generate_member_activity",
        snowflake_conn_id=SNOWFLAKE_CONN,
        sql=read_sql("generate_member_activity.sql"),
    )

    generate_group_performance = SnowflakeOperator(
        task_id="generate_group_performance",
        snowflake_conn_id=SNOWFLAKE_CONN,
        sql=read_sql("generate_group_performance.sql"),
    )

    generate_venue_stats = SnowflakeOperator(
        task_id="generate_venue_stats",
        snowflake_conn_id=SNOWFLAKE_CONN,
        sql=read_sql("generate_venue_stats.sql"),
    )

    generate_city_stats = SnowflakeOperator(
        task_id="generate_city_stats",
        snowflake_conn_id=SNOWFLAKE_CONN,
        sql=read_sql("generate_city_stats.sql"),
    )

    generate_topic_trends = SnowflakeOperator(
        task_id="generate_topic_trends",
        snowflake_conn_id=SNOWFLAKE_CONN,
        sql=read_sql("generate_topic_trends.sql"),
    )

    generate_category_summary = SnowflakeOperator(
        task_id="generate_category_summary",
        snowflake_conn_id=SNOWFLAKE_CONN,
        sql=read_sql("generate_category_summary.sql"),
    )

    validate_gold = SnowflakeOperator(
        task_id="validate_gold",
        snowflake_conn_id=SNOWFLAKE_CONN,
        sql="""
            SELECT 'GLD_FACT_EVENTS'           AS tabla, COUNT(*) AS total FROM MEETUP_DB.GOLD.GLD_FACT_EVENTS
            UNION ALL
            SELECT 'GLD_AGG_GROUP_PERFORMANCE', COUNT(*) FROM MEETUP_DB.GOLD.GLD_AGG_GROUP_PERFORMANCE
            UNION ALL
            SELECT 'GLD_AGG_CITY_STATS',        COUNT(*) FROM MEETUP_DB.GOLD.GLD_AGG_CITY_STATS
            UNION ALL
            SELECT 'GLD_AGG_TOPIC_TRENDS',      COUNT(*) FROM MEETUP_DB.GOLD.GLD_AGG_TOPIC_TRENDS
            UNION ALL
            SELECT 'GLD_AGG_CATEGORY_SUMMARY',  COUNT(*) FROM MEETUP_DB.GOLD.GLD_AGG_CATEGORY_SUMMARY
            UNION ALL
            SELECT 'GLD_AGG_VENUE_STATS',       COUNT(*) FROM MEETUP_DB.GOLD.GLD_AGG_VENUE_STATS
            UNION ALL
            SELECT 'GLD_AGG_MEMBER_ACTIVITY',   COUNT(*) FROM MEETUP_DB.GOLD.GLD_AGG_MEMBER_ACTIVITY;
        """,
    )

    generate_fact_events >> [
        generate_group_performance,
        generate_venue_stats,
        generate_city_stats,
        generate_topic_trends,
        generate_category_summary,
    ]

    [
        generate_group_performance,
        generate_venue_stats,
        generate_city_stats,
        generate_topic_trends,
        generate_category_summary,
        generate_member_activity,
    ] >> validate_gold
