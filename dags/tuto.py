"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "Javier",
    "depends_on_past": False,
    "start_date": datetime(2022, 7, 18),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    # "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}
# A dag (directed acyclic graph) is a collection of tasks with directional dependencies.
dag = DAG(
    "Aulapractica1",
    description='Basico de Bash Operators y Python Operator',
    default_args=default_args,
    schedule_interval=timedelta(minutes=2)
)

hello_bash = BashOperator(
    task_id="hellow_bash",
    bash_command='echo "Hello Airflow from bash"',
    dag=dag
)


def say_hello():
    print("Hello Airflow from Python")


hello_python = PythonOperator(
    task_id="hello_python",
    python_callable=say_hello,
    dag=dag
)

hello_bash >> hello_python  # orden de excecucion


