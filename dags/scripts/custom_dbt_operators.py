from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator

from cosmos.operators.local import DbtRunLocalOperator, DbtSnapshotLocalOperator, DbtTestLocalOperator
from cosmos.dbt.graph import DbtNode
from cosmos.config import ProfileConfig

from pathlib import Path


DBT_CONFIG = ProfileConfig(
    profile_name='auciello',
    target_name='dev',
    profiles_yml_filepath=Path('/usr/local/airflow/dags/dbt/auciello/profiles.yml')
)

DBT_PROJECT_DIR = '/usr/local/airflow/dags/dbt/auciello'

class DbtSourceColoredOperator(EmptyOperator):
    ui_color = "#5FB825"
    ui_fgcolor = "white"

def convert_source(dag: DAG, task_group: TaskGroup, node: DbtNode, **kwargs):
    """
    Return an instance of a desired operator to represent a dbt "source" node.
    """
    return DbtSourceColoredOperator(dag=dag, task_group=task_group, task_id=f"{node.name}_source")




class DbtModelColoredOperator(DbtRunLocalOperator):
    ui_color = "#0094B3"
    ui_fgcolor = "#FFFFFF"

def convert_model(dag: DAG, task_group: TaskGroup, node: DbtNode, **kwargs):
    """
    Return an instance of a desired operator to represent a dbt "model" node.
    """
    return DbtModelColoredOperator(dag=dag, 
                                   task_group=task_group, 
                                   task_id=f"{node.name}", 
                                   profile_config=DBT_CONFIG, 
                                   project_dir=DBT_PROJECT_DIR,
                                   select=[f"{node.name}"])



class DbtSnapshotColoredOperator(DbtSnapshotLocalOperator):
    ui_color = "#88447D"
    ui_fgcolor = "#FFFFFF"


def convert_snapshot(dag: DAG, task_group: TaskGroup, node: DbtNode, **kwargs):
    """
    Return an instance of a desired operator to represent a dbt "snapshot" node.
    """
    return DbtSnapshotColoredOperator(dag=dag, 
                                      task_group=task_group, 
                                      task_id=f"{node.name}", 
                                      profile_config=DBT_CONFIG, 
                                      project_dir=DBT_PROJECT_DIR,
                                      select=[f"{node.name}"])


# Cosmos will use this function to generate an empty task when it finds a exposure node, in the manifest.
def convert_exposure(dag: DAG, task_group: TaskGroup, node: DbtNode, **kwargs):
    """
    Return an instance of a desired operator to represent a dbt "exposure" node.
    """
    return EmptyOperator(dag=dag, task_group=task_group, task_id=f"{node.name}_exposure")


class DbtTestLocalColoredOperator(DbtTestLocalOperator):
    ui_color = "#C3B14C"
    ui_fgcolor = "#FFFFFF"


def convert_test(dag: DAG, task_group: TaskGroup, node: DbtNode, **kwargs):
    """
    Return an instance of a desired operator to represent a dbt "test" node.
    """
    return DbtTestLocalColoredOperator(dag=dag, 
                                      task_group=task_group, 
                                      task_id=f"{node.name}", 
                                      profile_config=DBT_CONFIG, 
                                      project_dir=DBT_PROJECT_DIR,
                                      select=[f"{node.name}"])