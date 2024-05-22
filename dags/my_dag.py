'''
# DAG - Astronomer Academy
'''
import json
from pendulum import datetime
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import (
    dag,
    task
)  


DAG_ID = 'my_dag'

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

@dag(
    dag_id=DAG_ID,
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["example"],
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


    @task
    def peter_task(ti=None):
        ti.xcom_push(key='mobile_phone', value='iphone')

    @task
    def lorie_task(ti=None):
        ti.xcom_push(key='mobile_phone', value='galaxy')
    
    @task
    def bryan_task(ti=None):
        phone = ti.xcom_pull(task_ids=['peter_task','lorie_task'], key='mobile_phone')
        print(phone)
    
    peter_task() >> lorie_task() >> bryan_task()

exec()
