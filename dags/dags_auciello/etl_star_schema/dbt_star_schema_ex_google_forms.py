'''
# DBT ETL Sessions
Tabelas necessárias para criar o modelo de sessões do GA4

'''
from pendulum import datetime
from datetime import datetime, timedelta
import os
from airflow.decorators import dag
from airflow.models.baseoperator import chain

from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.config import ProfileConfig, ProjectConfig, RenderConfig
from cosmos import ExecutionConfig, ExecutionMode, ProjectConfig, ProfileConfig, RenderConfig, LoadMode
from cosmos.constants import TestBehavior, ExecutionMode, DbtResourceType, LoadMode

from typing import Optional
from pathlib import Path

from scripts.slack_callbacks import error_callback_func, warning_callback_func
from scripts.custom_dbt_operators import convert_source \
                                , convert_model, convert_snapshot \
                                , convert_exposure, convert_test



DBT_CONFIG = ProfileConfig(
    profile_name='auciello',
    target_name='dev',
    profiles_yml_filepath=Path('/usr/local/airflow/dags/dbt/auciello/profiles.yml')
)

DAG_ID = os.path.basename(__file__).replace(".py", "")
EXECUTION_CONFIG = ExecutionConfig(execution_mode=ExecutionMode.LOCAL)

DBT_PROJECT_DIR = '/usr/local/airflow/dags/dbt/auciello'

DBT_PROJECT_CONFIG = ProjectConfig(
    dbt_project_path='/usr/local/airflow/dags/dbt/auciello',
    manifest_path=f"{DBT_PROJECT_DIR}/target/manifest.json",
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


SCHEDULE_CRON="0 0,12 * * *"

@dag(
    dag_id=DAG_ID,
    description=DAG_ID,
    start_date=datetime(2023,1,1),
    schedule=None,
    catchup=False,
    default_view='graph',
    max_active_runs=1,
    concurrency=5,
    default_args=DEFAULT_ARGS,
    on_failure_callback=error_callback_func,
    tags=['dbt','auciello','daily_etl'],
    doc_md=__doc__
)
def dbt_build():  

    etl = DbtTaskGroup(
        group_id='etl',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=["+star_schema+"],
            exclude=["star_schema.dim.google_forms"],
            test_behavior=TestBehavior.AFTER_EACH,
            node_converters={
                DbtResourceType("source"): convert_source,  
                DbtResourceType("exposure"): convert_exposure,  
                DbtResourceType("model"): convert_model,  
                DbtResourceType("snapshot"): convert_snapshot,
                DbtResourceType("test"): convert_test
            }
        ),
        on_warning_callback=warning_callback_func
    )

    chain(
        etl
    )

dbt_build()