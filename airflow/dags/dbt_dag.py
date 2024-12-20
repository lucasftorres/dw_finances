from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from dotenv import load_dotenv
import os


load_dotenv()

dbt_env = {
    'DB_HOST_PROD': os.getenv('DB_HOST_PROD'),
    'DB_PORT_PROD': os.getenv('DB_PORT_PROD'),
    'DB_NAME_PROD': os.getenv('DB_NAME_PROD'),
    'DB_USER_PROD': os.getenv('DB_USER_PROD'),
    'DB_PASS_PROD': os.getenv('DB_PASS_PROD'),
    'DB_SCHEMA_PROD': os.getenv('DB_SCHEMA_PROD'),
    'DB_THREADS_PROD': os.getenv('DB_THREADS_PROD'),
}


# Configuração do DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    dag_id='dbt_postgres_pipeline',
    default_args=default_args,
    description='Pipeline com dbt e PostgreSQL',
    schedule_interval=None,
    start_date=datetime(2024, 12, 1),
    catchup=False,
    tags=['finance', 'dbt'],
) as dag:

    # Comando dbt run
    dbt_seed = BashOperator(
        task_id='dbt_seed',
        bash_command='/home/lucas/projects/airflow-venv/bin/dbt seed --project-dir /home/lucas/projects/dw-finance/dbfinanceiro --profiles-dir /home/lucas/projects/dw-finance',
        env=dbt_env
    )

    # Comando dbt test
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='/home/lucas/projects/airflow-venv/bin/dbt run --project-dir /home/lucas/projects/dw-finance/dbfinanceiro --profiles-dir /home/lucas/projects/dw-finance',
        env=dbt_env
    )

    # Orquestração
    dbt_seed >> dbt_run
