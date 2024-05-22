from pendulum import datetime
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.decorators import dag, task
from airflow.models import TaskInstance
from airflow.models.baseoperator import chain
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.utils.context import Context
from airflow.utils.task_group import TaskGroup
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.dbt.graph import DbtNode
# from cosmos.config import ProfileConfig, ProjectConfig, RenderConfig
# from cosmos.constants import LoadMode
# from cosmos import ExecutionConfig, ExecutionMode

from cosmos import DbtTaskGroup, ExecutionConfig, ExecutionMode, ProjectConfig, ProfileConfig, RenderConfig, LoadMode
from cosmos.constants import TestBehavior, ExecutionMode, TestBehavior, DbtResourceType, LoadMode
from cosmos.operators.local import DbtRunLocalOperator, DbtSnapshotLocalOperator
from airflow.models.baseoperator import chain

from typing import Optional
from pathlib import Path

DEFAULT_ARGS = {
    "depends_on_past": False,
    'start_date': datetime(2023,3,5),
    'email' : ['danieldeveloper01@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=10)
}

DAG_ID = os.path.basename(__file__).replace(".py", "")

EXECUTION_CONFIG = ExecutionConfig(execution_mode=ExecutionMode.LOCAL)

DBT_PROJECT_DIR = '/usr/local/airflow/dags/dbt/auciello'

DBT_CONFIG = ProfileConfig(
    profile_name='auciello',
    target_name='dev',
    profiles_yml_filepath=Path('/usr/local/airflow/dags/dbt/auciello/profiles.yml')
)

DBT_PROJECT_CONFIG = ProjectConfig(
    dbt_project_path=DBT_PROJECT_DIR,
    manifest_path='/usr/local/airflow/dags/dbt/auciello/target/manifest.json',
)

DAG_DOC_MD = '''
        ## Carga completa do Star Schema pelo DBT  
        ---
        O ETL está programada para ser executado duas vezes ao dia: __09h e 21h__  
        Em caso de erro um alerta será enviado para o canal do Slack: **airflow-dags-alert**  
        '''


SLACK_WEBHOOK_URL = 'https://hooks.slack.com/services/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'

def alert_slack_channel(context: dict):
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





class DbtSourceColoredOperator(EmptyOperator):
    ui_color = "#5FB825"
    ui_fgcolor = "white"

class DbtModelColoredOperator(DbtRunLocalOperator):
    ui_color = "#0094B3"
    ui_fgcolor = "#FFFFFF"

class DbtSnapshotColoredOperator(DbtSnapshotLocalOperator):
    ui_color = "#88447D"
    ui_fgcolor = "#FFFFFF"


def convert_source(dag: DAG, task_group: TaskGroup, node: DbtNode, **kwargs):
    """
    Return an instance of a desired operator to represent a dbt "source" node.
    """
    return DbtSourceColoredOperator(dag=dag, task_group=task_group, task_id=f"{node.name}_source")


# Cosmos will use this function to generate an empty task when it finds a exposure node, in the manifest.
def convert_exposure(dag: DAG, task_group: TaskGroup, node: DbtNode, **kwargs):
    """
    Return an instance of a desired operator to represent a dbt "exposure" node.
    """
    return EmptyOperator(dag=dag, task_group=task_group, task_id=f"{node.name}_exposure")


def convert_model(dag: DAG, task_group: TaskGroup, node: DbtNode, **kwargs):
    """
    Return an instance of a desired operator to represent a dbt "model" node.
    """
    return DbtModelColoredOperator(dag=dag, task_group=task_group, task_id=f"{node.name}", 
                                   profile_config=DBT_CONFIG, project_dir=DBT_PROJECT_DIR)


def convert_snapshot(dag: DAG, task_group: TaskGroup, node: DbtNode, **kwargs):
    """
    Return an instance of a desired operator to represent a dbt "snapshot" node.
    """
    return DbtSnapshotColoredOperator(dag=dag, task_group=task_group, task_id=f"{node.name}", 
                                   profile_config=DBT_CONFIG, project_dir=DBT_PROJECT_DIR)

@dag(
    dag_id=DAG_ID,
    description=DAG_ID,
    start_date=datetime(2023,1,1),
    schedule="0 0,12 * * *",
    catchup=False,
    dagrun_timeout=timedelta(hours=1),
    max_active_runs=1,
    concurrency=5,
    default_args=DEFAULT_ARGS,
    on_failure_callback=alert_slack_channel,
    default_view='graph',
    tags=['dbt','auciello','daily_etl'],
    doc_md=DAG_DOC_MD
)
def dbt_star_schema():  

    etl = DbtTaskGroup(
        group_id='etl',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        execution_config=EXECUTION_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['+path:models/star_schema+'],
            test_behavior=TestBehavior.AFTER_EACH,
            node_converters={
                DbtResourceType("source"): convert_source,  # known dbt node type to Cosmos (part of DbtResourceType)
                DbtResourceType("exposure"): convert_exposure,  # dbt node type new to Cosmos (will be added to DbtResourceType)
                DbtResourceType("model"): convert_model,  
                DbtResourceType("snapshot"): convert_snapshot,
            }
        ),
        on_warning_callback=warning_callback_func
    )



    send_email_on_error = EmailOperator(task_id="send_email_on_error",
                to="danieldeveloper01@gmail.com",
                subject="Airflow Error",
                html_content="""<h3>Ocorreu um erro na Dag. </h3>
                                <p>Dag: run_dbt_star_schema </p>  
                                """,
                trigger_rule="one_failed")

    send_email_on_success = EmailOperator(task_id="send_email_on_success",
                to="danieldeveloper01@gmail.com",
                subject="Airflow Success",
                html_content="""<h3>Dag Executada com sucesso. </h3>
                                <p>Dag: run_dbt_star_schema </p>  
                                """,
                trigger_rule="all_success")
    


    chain(
        etl,
        [send_email_on_error, send_email_on_success]
    )




dbt_star_schema()