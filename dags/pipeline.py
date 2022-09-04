from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
# from decouple import config
import pandas as pd
import requests
import glob
import os
import sqlalchemy
import zipfile
import json
import boto3
import pymongo

# Keys for AWS S3
AWS_ACCESS_KEY_ID = 'aws_access_key'
AWS_SECRET_ACCESS_KEY = 'aws_secret_access_key'
BUCKET_NAME = 'igti-bootcamp-ed'
path = '/usr/local/airflow/data/'

default_args = {
    "owner": "Javier",
    "depends_on_past": False,
    "start_date": datetime(2022, 7, 25, 13, 55),
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
    "PipelineDados",
    description='Pipeline automatizado para extrair dados de fontes externas, armazenar em Data Lake na AWS S3',
    default_args=default_args,
    schedule_interval='*/10 * * * *'
)


def get_data_from_monngo():
    # conectando com o banco de dados
    client = pymongo.MongoClient(
        'mongodb+srv://estudante_igti:SRwkJTDz2nA28ME9@unicluster.ixhvw.mongodb.net/')
    db = client['ibge']
    collection = db.get_collection('pnadc20203')
    mongo_docs = collection.find()
    # transformando o resultado em um dataframe
    docs = pd.DataFrame(mongo_docs)
    docs.pop('_id')
    # salvando o dataframe em um arquivo csv
    docs.to_csv(path + 'pnadc20203.csv', index=False)


task_get_data_from_mongo = PythonOperator(
    task_id='get_data_from_mongo',
    python_callable=get_data_from_monngo,
    dag=dag
)


def get_data_from_api():
    # conectando com API
    api = 'https://servicodados.ibge.gov.br/api/v1/localidades/distritos'
    response = requests.get(api)
    # transformando o resultado em um dataframe
    docs = pd.DataFrame(json.dumps(response))
    # salvando o dataframe em um arquivo csv
    docs.to_csv(path + 'distritos.csv', index=False)


task_get_data_from_api = PythonOperator(
    task_id='get_data_from_api',
    python_callable=get_data_from_api,
    dag=dag
)


def send_to_data_lake():
    # Start S3 client
    s3 = boto3.client(
        's3'
    )
    # Upload csv files to S3
    for file in glob.glob(path + '*.csv'):
        s3.upload_file(file, BUCKET_NAME, file)


task_send_to_data_lake = PythonOperator(
    task_id='send_to_data_lake',
    python_callable=send_to_data_lake,
    dag=dag
)

task_get_data_from_mongo >> task_send_to_data_lake
task_get_data_from_api >> task_send_to_data_lake
