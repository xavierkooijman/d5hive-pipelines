import logging
from utils.mailer import send_email

logger = logging.getLogger(__name__)


def on_failure(context: dict, config: dict):

    task_instance = context.get("task_instance")
    exception = context.get("exception")
    dag_id = context.get("dag").dag_id
    task_id = context.get("task").task_id
    log_url = task_instance.log_url

    logger.error(
        f"DAG {dag_id} task {task_id} failed"
        f"exception={exception}"
        f"log_url={log_url}"
    )
    email_config = config.get("email", {})
    html_content = f"""
        <h2>DAG {dag_id} task {task_id} failed</h2>
        <p><strong>Exception:</strong> {exception}</p>
        <p><a href="{log_url}">View Logs</a></p>
        """
    send_email(email_config, html_content)
