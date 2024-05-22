from pendulum import datetime
import os 
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator

# from cosmos.airflow.task_group import DbtTaskGroup
# from cosmos.config import ProfileConfig, ProjectConfig, RenderConfig
# from cosmos.constants import LoadMode

from cosmos import DbtTaskGroup, ExecutionConfig, ExecutionMode, ProjectConfig, ProfileConfig, RenderConfig, LoadMode


from airflow.models.baseoperator import chain

from pathlib import Path

DAG_ID = os.path.basename(__file__).replace(".py", "")

DBT_CONFIG = ProfileConfig(
    profile_name='auciello',
    target_name='dev',
    profiles_yml_filepath=Path('/usr/local/airflow/dags/dbt/auciello/profiles.yml')
)

DBT_PROJECT_CONFIG = ProjectConfig(
    dbt_project_path='/usr/local/airflow/dags/dbt/auciello',
    manifest_path='/usr/local/airflow/dags/dbt/auciello/target/manifest.json'
)

DEFAULT_ARGS = {
    "depends_on_past": False,
    'start_date': datetime(2023,3,5),
    'email' : ['danieldeveloper01@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=10)
}

EXECUTION_CONFIG = ExecutionConfig(execution_mode=ExecutionMode.LOCAL)

@dag(
    dag_id=DAG_ID,
    description=DAG_ID,
    start_date=datetime(2023,1,1),
    schedule=None,
    catchup=False,
    dagrun_timeout=timedelta(hours=1),
    max_active_runs=1,
    concurrency=5,
    default_view='graph',
    tags=['dbt','auciello','auciello_google_forms'],
    default_args=DEFAULT_ARGS,
)
def dbt_google_forms():  


    dim_google_forms_books_9_titles = DbtTaskGroup(
        group_id='dim_google_forms_books_9_titles',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        execution_config=EXECUTION_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_MANIFEST,
            select=['dim_google_forms_books_9_titles']
        )
    )


    dim_google_forms_books_18_titles = DbtTaskGroup(
        group_id='dim_google_forms_books_18_titles',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        execution_config=EXECUTION_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_MANIFEST,
            select=['dim_google_forms_books_18_titles']
        )
    )


    dim_google_forms_books_27_titles = DbtTaskGroup(
        group_id='dim_google_forms_books_27_titles',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        execution_config=EXECUTION_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_MANIFEST,
            select=['dim_google_forms_books_27_titles']
        )
    )


    dim_google_forms_books_54_titles = DbtTaskGroup(
        group_id='dim_google_forms_books_54_titles',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        execution_config=EXECUTION_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_MANIFEST,
            select=['dim_google_forms_books_54_titles']
        )
    )

    dim_google_forms_books = DbtTaskGroup(
        group_id='dim_google_forms_books',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        execution_config=EXECUTION_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_MANIFEST,
            select=['dim_google_forms_books']
        )
    )

    wait_5_seconds_1 = BashOperator(task_id='wait_5_seconds_1', bash_command="sleep 5")
    wait_5_seconds_2 = BashOperator(task_id='wait_5_seconds_2', bash_command="sleep 5")
    wait_5_seconds_3 = BashOperator(task_id='wait_5_seconds_3', bash_command="sleep 5")


    call_read_bq_google_forms = TriggerDagRunOperator(task_id='call_read_bq_google_forms', trigger_dag_id='read_bq_google_forms')

    chain(
        dim_google_forms_books_9_titles, wait_5_seconds_1,
        dim_google_forms_books_18_titles, wait_5_seconds_2,
        dim_google_forms_books_27_titles, wait_5_seconds_3,
        dim_google_forms_books_54_titles,
        dim_google_forms_books,
        call_read_bq_google_forms
    )


dbt_google_forms()