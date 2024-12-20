# ELT Financeiro com Python, dbt e Airflow

## Objetivo: Um pipeline de dados criando dentro de instância EC2 que irá extrair os dados financeiros de empresas listadas em bolsas como NYSE, NASDAQ, B3 que poderão ser utilizados para análise e construção de dashboards.

### Extração de Dados(Python):

Fonte dos Dados:
*tickers.txt* O bucket S3 possue um arquivo contém uma lista de Tickers(Conjunto de caracteres que representam uma ação) que será o ponto de partida para buscar as informações de cada ação.
*Yahoo Finance* portal financeiro que reune dados dos Tickers.
*operacoes.csv* para representar a movimentações de compra e venda das ações foi elaborada um função para criar um .csv com dados fictícios gerados de forma aleatória.
*info.csv* representando informações como nome da empresa, localidade, setor, industria. Tabela gerada a partir de uma taks de uma DAG Airflow.


Libs:
*yfinance* é uma biblioteca que permite acessar dados financeiros a partir dos Tickers, funciona como uma API Wrappler, simplificando o processo, encapsulando as operações como autenticação, endpoints, formatação.
*sqlalchemy* responsável por criar a engine para a conexão com o banco de dados e verificação da existência da tabela no banco de dados.
*dotenv* para carregar as variáveis de ambiente que serão utilizadas para conexão e carga na AWS
*os* para interação com o sistema operacional e manipulação das variáveis para que sejam inseridas como variáveis.
*pandas e numpy* para manipulação de dados.
*boto3* também funciona com API Wrappler para acesso e interação com os serviços AWS.



### Carga de dados(RDS - Postgres):

A carga dos dados foi dividade em 2 etapas:
    - A tabela tickers que contém os dados mais recentes de cada ticker como valor de abertura, maior valor, menor valor, valor de fechamento. A tabela será inserida diretamente no RDS-Postgres.
    - As tabelas info e operações serão inseridas no RDS a partir do comando dbt seed, que buscará arquivos .csv que estejam presentes na pasta seeds.



### Transformação dos dados(dbt-core):
Nesta etapa as boas práticas do dbt recomendam criar algumas pastas dentro da pasta models:

projeto/models/staging:
Aqui será a realizada a primeira etapa de transformação para mudar o nome das colunas utilizando CTE SQL, é necessário criar o arquivo .sql que fará a transformação.

Importante incluir o arquivo .yml para documentar.

A partir da CTE o comando dbt run irá criar as VIEWS para cada tabela a partir dos scripts .sql


### Orquestração
O pipeline foi criado de forma encadeada:

    1. clear_database.py (DAG que irá realizar a limpeza no RDS, excluindo TABLE e VIEWS criadas anteriormente,)
    2. extract_load.py (DAG para extração dos dados de operações e preços)
    3. create_info.py (DAG para extração de informações)
    4. dbt_dag.py (DAG para executar as transformações nas tabelas do RDS-PostgreSQL)


