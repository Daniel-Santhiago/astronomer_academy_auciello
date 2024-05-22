from pendulum import datetime
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.config import ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import LoadMode, TestBehavior


from airflow.models.baseoperator import chain

from pathlib import Path
import os



DBT_CONFIG = ProfileConfig(
    profile_name='auciello',
    target_name='dev',
    profiles_yml_filepath=Path('/usr/local/airflow/dags/dbt/auciello/profiles.yml')
)

DBT_PROJECT_CONFIG = ProjectConfig(
    dbt_project_path='/usr/local/airflow/dags/dbt/auciello',
)

DAG_ID = os.path.basename(__file__).replace(".py", "")

DEFAULT_ARGS = {
    "depends_on_past": False,
    'start_date': datetime(2023,3,5),
    'email' : ['danieldeveloper01@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=10)
}

@dag(
    dag_id=DAG_ID,
    description=DAG_ID,
    start_date=datetime(2023,1,1),
    schedule=None,
    catchup=False,
    default_view='graph',
    default_args=DEFAULT_ARGS,
    tags=['dbt','auciello','test']
)
def dbt_facebook():  

    facebook = DbtTaskGroup(
        group_id='facebook',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['+fact_facebook_insights_device'],
            test_behavior=TestBehavior.AFTER_EACH,
        )
    )

    # finish = EmptyOperator(task_id='finish')


    chain(
        facebook
    )

dbt_facebook()