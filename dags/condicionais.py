from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
import pandas as pd
import random

default_args = {
    "owner": "Javier",
    "depends_on_past": False,
    "start_date": datetime(2022, 7, 19, 11, 12),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG(
    "Aulapractica3",
    description='Extrai dados do Titanic da Internet e calcula a idade media para homens ou mulheres',
    default_args=default_args,
    schedule_interval=timedelta(minutes=2)
)

get_data = BashOperator(
    task_id='get-data',
    bash_command='curl https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv -o /usr/local/airflow/data/train.csv',
    dag=dag
)


def sorteia_h_m():
    return random.choice(['male', 'female'])


escolhe_h_m = PythonOperator(
    task_id="escolhe-h-m",
    python_callable=sorteia_h_m,
    dag=dag
)


def MoF(**context):
    value = context['task_instance'].xcom_pull(task_ids='escolhe-h-m')
    if value == 'male':
        return 'male_branch'
    if value == 'female':
        return 'female_branch'


male_female = BranchPythonOperator(
    task_id='condicional',
    python_callable=MoF,
    provide_context=True,
    dag=dag
)


def mean_male():
    df = pd.read_csv('/usr/local/airflow/data/train.csv')
    dfs = df.loc[df.Sex == 'male']
    print(f'Media de idade dos homens no Titanic: {dfs.Age.mean}')


male_branch = PythonOperator(
    task_id='male_branch',
    python_callable=mean_male,
    dag=dag
)


def mean_female():
    df = pd.read_csv('/usr/local/airflow/data/train.csv')
    dfs = df.loc[df.Sex == 'female']
    print(f'Media de idade das mulheres no Titanic: {dfs.Age.mean}')


female_branch = PythonOperator(
    task_id='female_branch',
    python_callable=mean_female,
    dag=dag
)


get_data >> escolhe_h_m >> male_female >> [male_branch, female_branch]
