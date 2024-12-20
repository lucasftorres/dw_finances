from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
from dotenv import load_dotenv
import os
import boto3

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
AWS_BUCKET_NAME = "dw-financeiro"

# Inicializa o cliente S3
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

# Função para carregar tickers do S3
def carregar_tickers_s3(caminho_arquivo_s3, **kwargs):
    try:
        # Faz o download do arquivo do S3
        response = s3_client.get_object(Bucket=AWS_BUCKET_NAME, Key=caminho_arquivo_s3)
        conteudo = response['Body'].read().decode('utf-8')
        tickers = [linha.strip() for linha in conteudo.splitlines()]

        # Salva os tickers no XCom
        kwargs['ti'].xcom_push(key='tickers', value=tickers)
    except Exception as e:
        raise ValueError(f"Erro ao carregar o arquivo {caminho_arquivo_s3} do S3: {e}")

# Função para extrair informações dos tickers
def extrair_informacoes_tickers(**kwargs):
    ti = kwargs['ti']
    tickers = ti.xcom_pull(task_ids='carregar_tickers_s3', key='tickers')

    if not tickers:
        raise ValueError("Nenhum ticker foi recuperado.")

    info = []
    for ticker in tickers:
        t = yf.Ticker(ticker)
        try:
            informacoes = {
                'ticker': ticker,
                'longName': t.info.get('longName', 'N/A'),
                'country': t.info.get('country', 'N/A'),
                'industry': t.info.get('industry', 'N/A'),
                'sector': t.info.get('sector', 'N/A'),
                'fullTimeEmployees': t.info.get('fullTimeEmployees', 'N/A')
            }
            info.append(informacoes)
        except KeyError:
            print(f"Erro ao obter informações para o ticker {ticker}.")
            continue

    ti.xcom_push(key='info', value=info)

# Função para criar e salvar CSV no S3
def csv_info(**kwargs):
    ti = kwargs['ti']
    info_dict = ti.xcom_pull(task_ids='extrair_informacoes_tickers', key='info')

    if not info_dict:
        raise ValueError("Nenhuma informação foi encontrada para salvar no CSV.")

    # Salva os dados em CSV diretamente na pasta seeds do dbt
    df = pd.DataFrame(info_dict)
    df.to_csv('/home/ubuntu/project/dbfinanceiro/seeds/info.csv', index=False)


# Configurações padrão da DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 12, 1),
    'catchup': False,
}

with DAG(
    'extracao_info_s3',
    default_args=default_args,
    description='DAG para extrair e processar informações de tickers de um arquivo no S3',
    schedule_interval=None,
    catchup=False,
    tags=['finance', 'tickers', 's3']
) as dag:

    # Carregar tickers do S3
    load_tickers_s3 = PythonOperator(
        task_id='carregar_tickers_s3',
        python_callable=carregar_tickers_s3,
        op_kwargs={'caminho_arquivo_s3': 'tickers.txt'},
        provide_context=True
    )

    # Extrair informações dos tickers
    extract_info = PythonOperator(
        task_id='extrair_informacoes_tickers',
        python_callable=extrair_informacoes_tickers,
        provide_context=True
    )

    # Criar o CSV no S3
    create_csv_info = PythonOperator(
        task_id='csv_info',
        python_callable=csv_info,
        provide_context=True
    )

    # Definir a ordem das tarefas
    load_tickers_s3 >> extract_info >> create_csv_info
