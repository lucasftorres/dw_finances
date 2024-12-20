from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
import os
from datetime import datetime

# Carregar variáveis de ambiente
load_dotenv()

# Configuração do banco de dados
DB_HOST = os.getenv('DB_HOST_PROD')
DB_PORT = os.getenv('DB_PORT_PROD')
DB_NAME = os.getenv('DB_NAME_PROD')
DB_USER = os.getenv('DB_USER_PROD')
DB_PASS = os.getenv('DB_PASS_PROD')
DB_SCHEMA = os.getenv('DB_SCHEMA_PROD', 'public')  # Padrão: 'public'

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Criar conexão com o banco de dados
engine = create_engine(DATABASE_URL)

def apagar_views(schema='public', **kwargs):
    """Apaga todas as views de um esquema."""
    with engine.begin() as conn:  # Usar begin para garantir commit automático
        try:
            # Listar views no esquema
            print(f"Buscando views no esquema '{schema}'...")
            result = conn.execute(text("""
                SELECT table_name 
                FROM information_schema.views 
                WHERE table_schema = :schema;
            """), {'schema': schema})
            
            views = [row[0] for row in result]
            print(f"Views encontradas: {views}")
            
            # Apagar cada view
            for view in views:
                print(f"Tentando apagar view: {schema}.{view}")
                conn.execute(text(f"DROP VIEW IF EXISTS {schema}.{view} CASCADE;"))
                print(f"View {schema}.{view} apagada com sucesso!")
        except SQLAlchemyError as e:
            print(f"Erro ao apagar views: {e}")

def apagar_tabelas(tabelas, schema='public', **kwargs):
    """Apaga as tabelas especificadas."""
    with engine.begin() as conn:  # Usar begin para garantir commit automático
        for tabela in tabelas:
            try:
                print(f"Tentando apagar tabela: {schema}.{tabela}")
                conn.execute(text(f"DROP TABLE IF EXISTS {schema}.{tabela} CASCADE;"))
                print(f"Tabela {schema}.{tabela} apagada com sucesso!")
            except SQLAlchemyError as e:
                print(f"Erro ao apagar a tabela {schema}.{tabela}: {e}")

# Definir o DAG no Airflow
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'start_date': datetime(2024, 12, 1),
    'catchup': False,
}

with DAG(
    'apagar_views_tabelas',
    default_args=default_args,
    description='DAG para apagar views e tabelas no banco de dados PostgreSQL',
    schedule_interval=None,  # Sem intervalo, pois o DAG será executado manualmente
    catchup=False,
    tags=['finance', 'tickers']
) as dag:

    # Função para apagar views
    task_apagar_views = PythonOperator(
        task_id='apagar_views',
        python_callable=apagar_views,
        op_args=[DB_SCHEMA],
        provide_context=True
    )

    # Função para apagar tabelas
    task_apagar_tabelas = PythonOperator(
        task_id='apagar_tabelas',
        python_callable=apagar_tabelas,
        op_args=[['tickers', 'operacoes'], DB_SCHEMA],
        provide_context=True
    )

    with TaskGroup("extract_and_save", tooltip="Triggers para as tasks que irão gerar as tabelas e salvar no PostgreSQL") as extract_and_save:
        # Disparar a DAG 'extracao_dados_operacoes'
        trigger_extracao_dados = TriggerDagRunOperator(
            task_id='trigger_extracao_dados_operacoes',
            trigger_dag_id='extracao_dados_operacoes',  # Nome da DAG a ser disparada
            execution_date='{{ ds }}',  # Define a mesma execution_date
            reset_dag_run=True,  # Reinicia a execução da DAG se necessário
            wait_for_completion=False,  # Não aguarda a DAG finalizada
        )

        # Disparar a DAG 'extracao_dados_operacoes'
        trigger_create_info_s3 = TriggerDagRunOperator(
            task_id='trigger_extracao_info_s3',
            trigger_dag_id='extracao_info_s3',  # Nome da DAG a ser disparada
            execution_date='{{ ds }}',  # Define a mesma execution_date
            reset_dag_run=True,  # Reinicia a execução da DAG se necessário
            wait_for_completion=False,  # Não aguarda a DAG finalizada
        )

    # Definir a ordem das tarefas
    task_apagar_views >> task_apagar_tabelas >> extract_and_save
