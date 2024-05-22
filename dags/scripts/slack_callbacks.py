from airflow.models import BaseOperator
from airflow.models import TaskInstance
from airflow.utils.decorators import apply_defaults
from airflow.utils.context import Context
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
import pandas as pd
from typing import Optional
from pathlib import Path



SLACK_WEBHOOK_URL = 'https://hooks.slack.com/services/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'


def error_callback_func(context: dict):
    """ Alert to slack channel on failed dag

    :param context: airflow context object
    """
    if not SLACK_WEBHOOK_URL:
        # Do nothing if slack webhook not set up
        return

    last_task: Optional[TaskInstance] = context.get('task_instance')
    dag_name = last_task.dag_id
    task_name = last_task.task_id
    error_message = context.get('exception') or context.get('reason')
    execution_date = context.get('execution_date')
    dag_run = context.get('dag_run')
    task_instances = dag_run.get_task_instances()
    file_and_link_template = "<{log_url}|{name}>"
    failed_tis = [file_and_link_template.format(log_url=ti.log_url, name=ti.task_id)
                  for ti in task_instances
                  if ti.state == 'failed']
    title = f':red_circle: Dag: *{dag_name}* has failed, with ({len(failed_tis)}) failed tasks'
    msg_parts = {
        'Execution date': execution_date,
        'Failed Tasks': ', '.join(failed_tis),
        'Error': error_message
    }
    msg = "\n".join([title, *[f"*{key}*: {value}" for key, value in msg_parts.items()]]).strip()

    SlackWebhookHook(
        webhook_token=SLACK_WEBHOOK_URL,
        message=msg,
    ).execute()


def warning_callback_func(context: Context):
    tests = context.get("test_names")
    results = context.get("test_results")

    warning_msgs = ""
    for test, result in zip(tests, results):
        warning_msg = f"""
        *Test*: {test}
        *Result*: {result}
        """
        warning_msgs += warning_msg

    if warning_msgs:
        slack_msg = f"""
        :large_yellow_circle: Airflow-DBT task with WARN.
        *Task*: {context.get('task_instance').task_id}
        *Dag*: {context.get('task_instance').dag_id}
        *Execution Time*: {context.get('execution_date')}
        *Log Url*: {context.get('task_instance').log_url}
        {warning_msgs}
        """

        # slack_hook = SlackWebhookHook(slack_webhook_conn_id="slack_conn_id")
        # slack_hook.send(text=slack_msg)

        SlackWebhookHook(
            webhook_token=SLACK_WEBHOOK_URL,
            message=slack_msg,
        ).execute()


