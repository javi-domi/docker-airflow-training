from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
import pandas as pd
import sqlalchemy
import zipfile

data_path = '/usr/local/airflow/data/microdados_enade_2019.zip/2019/3.DADOS'
arch_path = data_path + 'microdados_enade_2019.txt'

default_args = {
    "owner": "Javier",
    "depends_on_past": False,
    "start_date": datetime(2022, 7, 19, 13, 35),
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
    "Aulapractica4",
    description='Paralelismos',
    default_args=default_args,
    schedule_interval='*/10 * * * *'
)

start_preprocessing = BashOperator(
    task_id='start_preprocessing',
    bash_command='echo "Start Preprocessing"',
    dag=dag
)

get_data = BashOperator(
    task_id='get-data',
    bash_command='curl http://download.inep.gov.br/microdados/Enade_Microdados/microdados_enade_2019.zip -o /usr/local/airflow/data/microdados_enade_2019.zip',
    dag=dag
)


def unzip_file():
    with zipfile.ZipFile('/usr/local/airflow/data/microdados_enade_2019.zip', 'r') as zipped:
        zipped.extractall('/usr/local/airflow/data/')


unzip_data = PythonOperator(
    task_id='unzip_data',
    python_callable=unzip_file,
    dag=dag
)


def aplica_filtros():
    cols = ['CO_GRUPO', 'TP_SEXO', 'NU_IDADE', 'INTGER', 'NT_FG', 'NT_CE', 'QE_I01', 'QE_I02', 'QE_I04', 'QE_I05',
            'QE_I08']
    enade = pd.read_csv(arch_path, sep=';', decimal=',', usecols=cols)
    enade = enade.loc[
        (enade.NU_IDADE > 20) &
        (enade.NU_IDADE < 40) &
        (enade.NT_GER > 0)
        ]
    enade.to_csv(data_path + 'enade_filtrado.csv', index=False)


task_aplica_filtro = PythonOperator(
    task_id='aplica_filtro',
    python_callable=aplica_filtros,
    dag=dag
)


# Idade centralizada da media
def constroi_idade_centralizada():
    idade = pd.read_csv(data_path + 'enade_filtrado.csv', usecols=['NU_IDADE'])
    idade['idadecent'] = idade.NU_IDADE - idade.NU_IDADE.mean()
    idade[['idadecent']].to_csv(data_path + 'idade_cent.csv', index=False)


# Idade centralizada ao quadrado
def constroi_idade_cent_quad():
    idadecent = pd.read_csv(data_path + 'idade_cent.csv')
    idadecent['idade2'] = idadecent.idadecent ** 2
    idadecent[['idade2']].to_csv(data_path + 'idadequadrado.csv', index=False)


task_idade_cent = PythonOperator(
    task_id='constroi_idade_centralizada',
    python_callable=constroi_idade_centralizada,
    dag=dag
)

task_idade_quad = PythonOperator(
    task_id='constroi_idade_centralizada_ao_quadrado',
    python_callable=constroi_idade_cent_quad,
    dag=dag
)


def constroi_est_civil():
    filtro = pd.read_csv(data_path + 'enade_filtrado.csv', usecols=['QE_I01'])
    filtro['estcivil'] = filtro.QE_I01.replace({
        'A': 'Solteiro',
        'B': 'Casado',
        'C': 'Separado',
        'D': 'Viúvo',
        'E': 'Outro',
    })
    filtro[['estcivil']].to_csv(data_path + 'estcivil.csv', index=False)


task_est_civil = PythonOperator(
    task_id='constroi_est_civil',
    python_callable=constroi_est_civil,
    dag=dag
)


def constroi_cor_da_pele():
    filtro = pd.read_csv(data_path + 'enade_filtrado.csv', usecols=['QE_I01'])
    filtro['cor'] = filtro.QE_I02.replace({
        'A': 'Blanca',
        'B': 'Preta',
        'C': 'Amarela',
        'D': 'Parda',
        'E': 'Indigena',
        'F': '',
        ' ': ''
    })
    filtro[['estcivil']].to_csv(data_path + 'cor.csv', index=False)


task_cor_da_pele = PythonOperator(
    task_id='constroi_cor_da_pele',
    python_callable=constroi_cor_da_pele,
    dag=dag
)

def join_data():
    filtro = pd.read_csv(data_path + 'enade_filtgrado.csv')
    idadecent = pd.read_csv(data_path + 'idade_cent.csv')
    idadecentquadrado = pd.read_csv(data_path + 'idadequadrado.csv')
    estcivil = pd.read_csv(data_path + 'estcivil.csv')
    cor = pd.read_csv(data_path + 'cor.csv')

    final = pd.concat([
        filtro,
        idadecent,
        idadecentquadrado,
        estcivil,
        cor
    ],
    axis=1
    )
    final.to_csv(data_path + 'enade_tratado.csv', index=False)
    print(final)

task_join_data = PythonOperator(
    task_id='join_data',
    python_callable=join_data,
    dag=dag
)


def escreve_dw():
    final = pd.read_csv(data_path + 'enade_tratado.csv')
    engine = sqlalchemy.create_engine(
        "sql+pyodbc://data-base-connection"
    )
    final.to_sql('tratado', con=engine, index=False, if_exists='append')


task_escreve_dw = PythonOperator(
    task_id='escreve_dw',
    python_callable=escreve_dw,
    dag=dag
)

start_preprocessing >> get_data >> unzip_data >> task_aplica_filtro
task_aplica_filtro >> [task_idade_cent, task_est_civil, task_cor_da_pele]
task_idade_quad.set_upstream(task_idade_cent)

task_join_data.set_upstream([task_est_civil, task_cor_da_pele, task_idade_quad])
task_join_data >> task_escreve_dw
