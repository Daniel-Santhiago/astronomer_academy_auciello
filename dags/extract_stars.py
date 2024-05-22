'''
# DAG - Astronomer Academy
'''
import json
from pendulum import datetime
from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.http.operators.http import SimpleHttpOperator

from airflow.decorators import (
    dag,
    task,
)  # DAG and task decorators for interfacing with the TaskFlow API


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

def _start():
    print("hello")

def _end():
    print("bye")

def _print_stargazers(github_stats: str, date: str):
    github_stats_json = json.loads(github_stats)
    airflow_stars = github_stats_json.get("stargazers_count")
    print(f"As of {date}, Apache Airflow has {airflow_stars} stars on Github!")


@dag(
    dag_id=DAG_ID,
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["academy"],
    default_view='graph',
    default_args=DEFAULT_ARGS,
    doc_md=__doc__
)
def exec():
    """
    ### Basic ETL Dag
    This is a simple ETL data pipeline example that demonstrates the use of
    the TaskFlow API using three simple tasks for extract, transform, and load.
    For more information on Airflow's TaskFlow API, reference documentation here:
    https://airflow.apache.org/docs/apache-airflow/stable/tutorial_taskflow_api.html
    """

    get_date = BashOperator(
        task_id="get_date",
        bash_command="echo {{ data_interval_start }}"
    )

    query_github_stats = SimpleHttpOperator(
        task_id = 'query_github_stats',
        endpoint="{{ var.value.endpoint }}",
        method="GET",
        http_conn_id="github_api",
        log_response="True",

    )

    print_stargazers = PythonOperator(
        task_id='print_stargazers',
        python_callable=_print_stargazers,
        op_args=["{{ ti.xcom_pull(task_ids='query_github_stats') }}",
                 "{{ ti.xcom_pull(task_ids='get_date') }}"
                 ]
    )


    get_date >> query_github_stats >> print_stargazers


exec()
