from pendulum import datetime
import os 
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.models import TaskInstance
from airflow.models.baseoperator import chain
from airflow.models import Variable
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

from airflow.utils.task_group import TaskGroup
# from cosmos.airflow.task_group import DbtTaskGroup
# from cosmos.config import ProfileConfig, ProjectConfig, RenderConfig
# from cosmos.constants import LoadMode

from cosmos import DbtTaskGroup, ExecutionConfig, ExecutionMode, ProjectConfig, ProfileConfig, RenderConfig, LoadMode


from typing import Optional
from pathlib import Path
import statistics as sts
import random
from datetime import datetime
import requests as req
import time
import pandas as pd
import pandas_gbq
from google.cloud import bigquery
from google.oauth2 import service_account
import urllib.parse
import sys



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

DAG_DOC_MD = '''
    ## DAG: Google Forms
    ---
    ### group_dbt_google_forms
    ___
    Os resultados de formulários do Google Forms são salvos em abas de uma planilha do Google Sheets.  
    Cada aba do Google Sheets é conectada ao `Bigquery` contendo uma réplica dos dados.  
    Os modelos do DBT formatam então cada tabela source e os compilam em um só modelo final.  
    1. dim_google_forms_books_9_titles
    2. dim_google_forms_books_18_titles
    3. dim_google_forms_books_27_titles
    4. dim_google_forms_books_54_titles
    5. dim_google_forms_books

    Os modelos acima são lidos em série com um bloco de espera de 5 segundos entre cada.  
    Isto irá evitar uma sobrecarga dos recursos do Google Sheets.  

    ### task_read_bq_google_forms

    O modelo criado anteriormente no DBT é lido em Python e salvo em um Dataframe do Pandas.  
    É adicionada uma condição nesta leitura para trazer apenas preenchimentos de formulário ainda não adicionados na tabela final.  
    '''


SLACK_WEBHOOK_URL = 'https://hooks.slack.com/services/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'

CREDENTIALS = service_account.Credentials.from_service_account_file('/usr/local/airflow/dags/dbt/auciello/dbt_keyfile.json')
API_KEY = "53051_893a991da3d77dd40d45ea2baca383da"


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


def read_bq_dim_google_forms_books_new(**kwargs):
    query = f'''
        select
            tray_order_id, client_name, form_id, form_datetime, email, covers, form_book_index, 
            original_query, search_query,
            user_book_title, user_book_author, user_book_publisher, amazon_url
        from
            star_schema.dim_google_forms_books
        where
            1=1
            and form_id not in (
                select form_id from `auciello-design.isbndb.search_books_append`
            )
        limit {kwargs['batch_size']}
    '''
 
    df_query = pandas_gbq.read_gbq(
                query,
                project_id='auciello-design',
                credentials = kwargs['CREDENTIALS'],
                dialect='standard'
            )
    
    df_query.to_csv('/usr/local/airflow/include/data/new_google_form_books.csv', sep=';')

    kwargs['ti'].xcom_push(key='new_books', value=df_query)

    return df_query

def get_new_books_from_isbndb_api(**kwargs):
    # df = pd.read_csv('/usr/local/airflow/include/data/new_google_form_books.csv', sep=';')
    df = kwargs['ti'].xcom_pull(task_ids='task_read_bq_google_forms', key='new_books')
    print(df)
    API_KEY = kwargs['API_KEY']
    header = {
            'accept': 'application/json',
            'Authorization': API_KEY,
            'Content-Type': 'application/json',
        }
    books_result_list = []
    for counter, query in df.iterrows():
    # for counter, query in enumerate(query_list):
        # query_url = urllib.parse.quote(query, safe=':/?=')
        query_url = urllib.parse.quote(query['search_query'], safe=':/?=')
        if query_url[0:3] == '978':
            request_query = 'https://api2.isbndb.com/book/'+str(query_url)
            resp = req.get(request_query, headers=header)
            try:
                resp.raise_for_status()
                books_json = resp.json()['book']
            except req.exceptions.HTTPError as e:
                print(f"Mensagem de Erro: {e}")
                sys.exit(1)
        else:
            request_query = 'https://api2.isbndb.com/search/books?text='+query_url
            resp = req.get(request_query, headers=header)
            try:
                resp.raise_for_status()
                books_json = resp.json()['data'][0]
            except req.exceptions.HTTPError as e:
                print(f"Mensagem de Erro: {e}")
                sys.exit(1)
        book_dict = {}    
        print(counter+1)
        # book_dict['request_time'] = datetime.datetime.now()
        # book_dict['request_counter'] = counter + 1
        book_dict['tray_order_id'] = query['tray_order_id']
        book_dict['client_name'] = query['client_name']
        book_dict['form_id'] = query['form_id']
        # book_dict['form_datetime'] = query['form_datetime']
        # book_dict['form_datetime'] = query['form_datetime'].apply(pd.Timestamp)
        try:
            book_dict['form_datetime'] = datetime.strptime(query['form_datetime'][0:19], "%Y-%m-%d %H:%M:%S")
        except TypeError:
            book_dict['form_datetime'] = query['form_datetime']
        book_dict['email'] = query['email']
        book_dict['covers'] = query['covers']
        book_dict['form_book_index'] = query['form_book_index']
        book_dict['original_query'] = query['original_query']
        book_dict['search_query'] = query['search_query']

        book_dict['user_book_title'] = query['user_book_title']
        book_dict['user_book_author'] = query['user_book_author']
        book_dict['user_book_publisher'] = query['user_book_publisher']
        book_dict['amazon_url'] = query['amazon_url']
        # book_dict['isbn10'] = books_json['isbn10']
        book_dict['isbn10'] = books_json.get('isbn10')
        book_dict['isbn13'] = books_json['isbn13']
        book_dict['title'] = books_json['title']
        # book_dict['authors'] = ','.join(books_json['authors'])
        try:
            book_dict['authors'] = ','.join(books_json['authors'])
        except KeyError:
            book_dict['authors'] = ''
        # book_dict['authors'] = ','.join(books_json.get('authors'))
        # book_dict['publisher'] = books_json['publisher']
        book_dict['publisher'] = books_json.get('publisher')
        # book_dict['image'] = books_json['image']
        book_dict['image'] = books_json.get('image')
        book_dict['image_formula'] = '=IMAGE("'+books_json.get('image')+'";4;180;150)'
        books_result_list.append(book_dict)
        # print(book_dict)
        time.sleep(1)
    df_books_result = pd.DataFrame(books_result_list)
    df_books_result['tray_order_id'] = df_books_result['tray_order_id'].astype(int)
    df_books_result['covers'] = df_books_result['covers'].astype(int)
    df_books_result['form_book_index'] = df_books_result['form_book_index'].astype(int)
    kwargs['ti'].xcom_push(key='books_result', value=df_books_result)
    return df_books_result


def dataframe_to_bigquery(**kwargs):
    df_books_result = kwargs['ti'].xcom_pull(key='books_result')
    print(df_books_result.tail(2))


    # credentials = service_account.Credentials.from_service_account_file('/usr/local/airflow/dags/dbt/auciello/dbt_keyfile.json')
    credentials = kwargs['CREDENTIALS']
    project_id = 'auciello-design'
    dataset = 'isbndb'
    table_name = 'search_books_append'
    
    client = bigquery.Client(credentials=credentials,project=project_id)
    table = client.get_table(f'{dataset}.{table_name}')
    generated_schema = [{'name':i.name, 'type':i.field_type} for i in table.schema]

    
    pandas_gbq.to_gbq(df_books_result, 
                    destination_table=f"{dataset}.{table_name}", 
                    project_id=project_id, 
                    table_schema = generated_schema,
                    if_exists='append', 
                    credentials=credentials)

@dag(
    dag_id=DAG_ID,
    description=DAG_ID,
    start_date=datetime(2023,1,1),
    schedule=None,
    catchup=False,
    dagrun_timeout=timedelta(hours=1),
    max_active_runs=1,
    concurrency=5,
    on_failure_callback=alert_slack_channel,
    default_view='graph',
    tags=['dbt','auciello','auciello_google_forms'],
    default_args=DEFAULT_ARGS,
    doc_md=DAG_DOC_MD
)
def dbt_google_forms():  

    @task_group(group_id='group_dbt_google_forms')
    def group_dbt_google_forms():
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

        dim_google_forms_books_9_titles >> wait_5_seconds_1 >> dim_google_forms_books_18_titles
        dim_google_forms_books_18_titles >> wait_5_seconds_2 >> dim_google_forms_books_27_titles
        dim_google_forms_books_27_titles >> wait_5_seconds_3 >> dim_google_forms_books_54_titles
        dim_google_forms_books_54_titles >> dim_google_forms_books



    task_read_bq_google_forms = PythonOperator(
        task_id='task_read_bq_google_forms',
        provide_context=True,
        python_callable=read_bq_dim_google_forms_books_new,
        op_kwargs={'batch_size': 500, 'CREDENTIALS': CREDENTIALS}
    )

    task_get_new_books_from_isbndb_api = PythonOperator(
        task_id='task_get_new_books_from_isbndb_api', 
        provide_context=True,
        python_callable=get_new_books_from_isbndb_api,
        op_kwargs={'API_KEY': API_KEY}
        
    )

    task_dataframe_to_bigquery = PythonOperator(
        task_id='task_dataframe_to_bigquery',
        provide_context=True,
        python_callable=dataframe_to_bigquery,
        op_kwargs={'CREDENTIALS': CREDENTIALS}
    )

    @task_group(group_id='group_dbt_google_forms_isbn')
    def group_dbt_google_forms_isbn():
        dim_google_forms_isbn = DbtTaskGroup(
            group_id='dim_google_forms_isbn',
            project_config=DBT_PROJECT_CONFIG,
            profile_config=DBT_CONFIG,
            execution_config=EXECUTION_CONFIG,
            render_config=RenderConfig(
                load_method=LoadMode.DBT_LS,
                select=['path:models/star_schema/dim/dim_google_forms_isbn.sql']
            )
        )

        dim_google_forms_isbn

    chain(
        group_dbt_google_forms(),
        task_read_bq_google_forms,
        task_get_new_books_from_isbndb_api,
        task_dataframe_to_bigquery,
        group_dbt_google_forms_isbn()
    )




dbt_google_forms()