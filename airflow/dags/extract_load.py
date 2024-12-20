#import
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
import yfinance as yf
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import boto3

load_dotenv()

# Carregar variáveis de ambiente
DB_HOST = os.getenv('DB_HOST_PROD')
DB_PORT = os.getenv('DB_PORT_PROD')
DB_NAME = os.getenv('DB_NAME_PROD')
DB_USER = os.getenv('DB_USER_PROD')
DB_PASS = os.getenv('DB_PASS_PROD')

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL)

# Configurações do S3
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_BUCKET_NAME = os.getenv('AWS_BUCKET_NAME')
AWS_REGION = os.getenv('AWS_REGION')

# Inicialização do cliente S3
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

# Função para carregar tickers do S3
def carregar_tickers_s3(**kwargs):
    s3_key = 'tickers.txt'  # Caminho no bucket

    try:
        # Baixar o arquivo do S3
        response = s3_client.get_object(Bucket=AWS_BUCKET_NAME, Key=s3_key)
        content = response['Body'].read().decode('utf-8')

        # Processar os tickers
        tickers = [linha.strip() for linha in content.splitlines()]

        # Passando a lista de tickers para o XCom
        kwargs['ti'].xcom_push(key='tickers', value=tickers)
    except Exception as e:
        print(f"Erro ao carregar o arquivo {s3_key} do S3: {e}")
        raise

# Função para extrair dados do ticker
def extrair_dados(**kwargs):
    # Recuperar os tickers usando o XCom
    ti = kwargs['ti']
    tickers = ti.xcom_pull(task_ids='carregar_tickers_s3', key='tickers')
    
    dataset = []
    for simbolo in tickers:
        try:
            dados = yf.Ticker(simbolo).history(period='1y', interval='1d')[['Open', 'High', 'Low', 'Close']]
            dados['simbolo'] = simbolo
            dataset.append(dados)
        except Exception as e:
            print(f"Erro ao extrair o ticker: {simbolo}: {e}")
    
    dados = pd.concat(dataset).reset_index()
    dados['Date'] = dados['Date'].astype(str)

    # Convertendo o DataFrame para lista de dicionários
    ti.xcom_push(key='dados', value=dados.to_dict(orient='records'))

# Função para criar a tabela de operações
def criar_tabela_dados(**kwargs):
    ti = kwargs['ti']
    dados_dict = ti.xcom_pull(task_ids='extrair_dados', key='dados')  # Recupera o dicionário
    
    # Converte o dicionário de volta para DataFrame
    df = pd.DataFrame.from_dict(dados_dict)
    df['actions'] = np.random.choice(['buy', 'sell'], size=len(df))
    df['quantity'] = np.random.randint(0, 30, size=len(df))
    
    # Armazenar no XCom para a próxima tarefa
    ti.xcom_push(key='dados', value=df.to_dict(orient='records'))



# Função para salvar o DataFrame no banco de dados PostgreSQL
def salvar_postgres(**kwargs):
    ti = kwargs['ti']
    operacoes = ti.xcom_pull(task_ids='criar_tabela_dados', key='dados')

    # Converter de volta para DataFrame
    df = pd.DataFrame.from_dict(operacoes)

    # Salva a tabela de valores diários no banco de dados
    df.to_sql('tickers', con=engine, if_exists='replace', index=False, schema='public')



# Função para criar e salvar o arquivo CSV no S3
def criar_e_salvar_csv(**kwargs):
    ti = kwargs['ti']
    dados = ti.xcom_pull(task_ids='criar_tabela_dados', key='dados')

    # Converte o dicionário de volta para DataFrame
    df = pd.DataFrame.from_dict(dados)

    # Cria colunas actions e quantity randomicamente
    df['actions'] = np.random.choice(['buy', 'sell'], size=len(df))
    df['quantity'] = np.random.randint(0, 30, size=len(df))

    # Seleciona as colunas desejadas
    df = df[['Date', 'simbolo', 'actions', 'quantity']]

    # Salva o .csv
    df.to_csv('/home/ubuntu/project/dbfinanceiro/seeds/operacoes.csv', index=False)



# Definindo o DAG no Airflow
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'start_date': datetime(2024, 12, 1),
    'catchup': False,
}

with DAG(
    'extracao_dados_operacoes',
    default_args=default_args,
    description='DAG para extração e processamento de dados de tickers',
    schedule_interval=None,  # Sem intervalo
    catchup=False,
    tags=['finance', 'tickers'],
) as dag:

    # Carregar tickers
    load_tickers = PythonOperator(
        task_id='carregar_tickers_s3',
        python_callable=carregar_tickers_s3,
        provide_context=True
    )

    # Extrair dados dos tickers
    extract_data = PythonOperator(
        task_id='extrair_dados',
        python_callable=extrair_dados,
        provide_context=True
    )

    # Criar tabela de operações
    create_data_table = PythonOperator(
        task_id='criar_tabela_dados',
        python_callable=criar_tabela_dados,
        provide_context=True
    )

    with TaskGroup("save_and_export", tooltip="Salvar no banco de dados e exportar para CSV") as save_and_export:
        save_postgres = PythonOperator(
            task_id='salvar_postgres',
            python_callable=salvar_postgres,
            provide_context=True
        )

        save_csv = PythonOperator(
            task_id='criar_e_salvar_csv',
            python_callable=criar_e_salvar_csv,
            provide_context=True
        )
    
    # Disparar a execução da DAG do dbt
    trigger_dbt = TriggerDagRunOperator(
            task_id='dbt_postgres',
            trigger_dag_id='dbt_postgres_pipeline',  # Nome da DAG a ser disparada
            execution_date='{{ ds }}',  # Define a mesma execution_date
            reset_dag_run=True,  # Reinicia a execução da DAG se necessário
            wait_for_completion=False,  # Não aguarda a DAG finalizada
        )

    # Definir a ordem das tarefas
    load_tickers >> extract_data >> create_data_table >> save_and_export >> trigger_dbt