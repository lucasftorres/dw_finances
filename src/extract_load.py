#import
import yfinance as yf
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

DB_HOST=os.getenv('DB_HOST_PROD')
DB_PORT=os.getenv('DB_PORT_PROD')
DB_NAME=os.getenv('DB_NAME_PROD')
DB_USER=os.getenv('DB_USER_PROD')
DB_PASS=os.getenv('DB_PASS_PROD')
DB_SCHEMA=os.getenv('DB_SCHEMA_PROD')

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL)


commodities = ['GC=F', 'HG=F', 'NG=F', 'CL=F']
tech = ['AMD', 'AAPL', 'TSMC34.SA', 'MSFT', 'NVDA', 'GOOG', 'META', 'AMZN', 'UBER', 'NFLX']
financeiro = ['JPM', 'BAC', 'C', 'NU', 'ITUB', 'BBAS3.SA']
auto = ['GM', 'TSLA', 'TM', 'MBG.DE', 'F', 'STLA', 'HMC']
holding = ['WMT', 'PEP', 'KO', 'UL', 'ABEV', 'MDLZ']
moedas = ['EURUSD=X', 'GBP=X', 'EURBRL=X', 'BRL=X', 'GBPBRL=X']
crypto = ['BTC-USD', 'XRP-USD', 'ETH-USD']

tickers = [commodities, tech, financeiro, auto, holding, moedas, crypto]

def extrair_dados(simbolo, periodo='1y', intervalo='1d'):
    ticker = yf.Ticker(simbolo)
    dados = ticker.history(period=periodo, interval=intervalo)[['Open', 'High', 'Low', 'Close']]
    dados['simbolo'] = simbolo
    return dados

def concat_dados(tickers):
    dataset = []
    for setor in tickers:
            try:
                for simbolo in setor:
                    dados = extrair_dados(simbolo)
                    dataset.append(dados)
            except Exception as e:
                print(f"Erro ao extrair o ticker: {simbolo}: {e}")
    return pd.concat(dataset)


def criar_tabela_operacoes(df):
    df = df.reset_index()
    df['actions'] = np.random.choice(['buy','sell'], size=len(df))
    df['quantity'] = np.random.randint(0, 30, size=len(df))
    return df[['Date', 'simbolo', 'actions', 'quantity']]


def criar_csv(df):
    df.to_csv('src/operacoes.csv', index=False)

def salvar_postgres(df, schema='public'):
    df.to_sql('tickers', engine, if_exists='replace', index=True, index_label='Date', schema=schema) 

if __name__ == "__main__":
    dados_concatenados = concat_dados(tickers)
    operacoes = criar_tabela_operacoes(dados_concatenados)
    salvar_postgres(dados_concatenados, schema='public')
    criar_csv(operacoes)