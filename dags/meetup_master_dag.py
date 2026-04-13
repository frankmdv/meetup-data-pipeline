from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

from config.slack_alerts import on_failure_callback, on_success_callback
from scripts.generate_traffic import generate_traffic

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": on_failure_callback,
}

with DAG(
    dag_id="meetup_master_dag",
    description="Orquesta el pipeline completo: Generate → Bronze → Silver → Gold → Export",
    schedule="*/15 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    on_success_callback=on_success_callback,
    tags=["meetup", "master", "orquestacion"],
) as dag:

    generate_data = PythonOperator(
        task_id="generate_traffic",
        python_callable=generate_traffic,
    )

    trigger_bronze = TriggerDagRunOperator(
        task_id="trigger_bronze",
        trigger_dag_id="meetup_bronze_dag",
        wait_for_completion=True,
        deferrable=True,
        poke_interval=30,
        reset_dag_run=True,
    )

    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver",
        trigger_dag_id="meetup_silver_dag",
        wait_for_completion=True,
        deferrable=True,
        poke_interval=30,
        reset_dag_run=True,
    )

    trigger_gold = TriggerDagRunOperator(
        task_id="trigger_gold",
        trigger_dag_id="meetup_gold_dag",
        wait_for_completion=True,
        deferrable=True,
        poke_interval=30,
        reset_dag_run=True,
    )

    trigger_export = TriggerDagRunOperator(
        task_id="trigger_export",
        trigger_dag_id="meetup_export_dag",
        wait_for_completion=True,
        deferrable=True,
        poke_interval=30,
        reset_dag_run=True,
    )

    generate_data >> trigger_bronze >> trigger_silver >> trigger_gold >> trigger_export
