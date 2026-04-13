import requests
from airflow.sdk import Variable


def _get_webhook_url() -> str:
    return Variable.get("SLACK_WEBHOOK_URL", default_var="")


def _send_slack_message(message: str) -> None:
    webhook_url = _get_webhook_url()
    if not webhook_url:
        print("[slack_alerts] SLACK_WEBHOOK_URL no configurado — omitiendo alerta")
        return
    try:
        response = requests.post(
            webhook_url,
            json={"text": message},
            timeout=10,
        )
        print(f"[slack_alerts] Slack respondió: {response.status_code}")
    except Exception as e:
        print(f"[slack_alerts] Error enviando a Slack: {e}")


def on_failure_callback(context: dict) -> None:
    dag_id       = context["dag"].dag_id
    task_id      = context["task_instance"].task_id
    logical_date = context.get("logical_date") or context.get("execution_date")
    execution_dt = logical_date.strftime("%Y-%m-%d %H:%M:%S UTC") if logical_date else "N/A"
    log_url      = context["task_instance"].log_url
    exception    = context.get("exception", "Sin detalle")

    message = (
        f":red_circle: *FALLO EN PIPELINE*\n"
        f">*DAG:*       `{dag_id}`\n"
        f">*Tarea:*     `{task_id}`\n"
        f">*Ejecución:* `{execution_dt}`\n"
        f">*Error:*     {exception}\n"
        f">*Logs:*      <{log_url}|Ver logs>"
    )
    _send_slack_message(message)


def on_success_callback(context: dict) -> None:
    dag_id       = context["dag"].dag_id
    logical_date = context.get("logical_date") or context.get("execution_date")
    execution_dt = logical_date.strftime("%Y-%m-%d %H:%M:%S UTC") if logical_date else "N/A"
    dag_run      = context["dag_run"]
    duration     = (
        dag_run.end_date - dag_run.start_date
        if dag_run.end_date and dag_run.start_date
        else None
    )
    duration_str = str(duration).split(".")[0] if duration else "N/A"

    message = (
        f":large_green_circle: *PIPELINE COMPLETADO*\n"
        f">*DAG:*        `{dag_id}`\n"
        f">*Ejecución:*  `{execution_dt}`\n"
        f">*Duración:*   `{duration_str}`"
    )
    _send_slack_message(message)
