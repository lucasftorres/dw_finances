# ELT Financeiro com Python, AWS e dbt

## Objetivo: Neste projeto iremos extrair os dados financeiros com Pytho, armazenar na AWS em um Banco de Dados Postgres e utilizar o dbt-core para realizar a tranformação.

### Extração de Dados(Python):

Libs:
*yfinance* fará a extração dados financeiros como valores de Abertura, Fechamento, mais alto do período e mais baixo do período.
*sqlalchemy* responsável por criar a engine para a conexão com o banco de dados e verificação da existência da tabela no banco de dados.
*dotenv* para carregar as variáveis de ambiente que serão utilizadas para conexão e carga na AWS
*os* para interação com o sistema operacional e manipulação das variáveis para que sejam inseridas como variáveis e não estejam visíveis no projeto pois o arquivo .env está incluso no .gitignore
*pandas e numpy* para manipulação básica.

Fonte dos Dados:
*Yahoo Finance* portal financeiro que reune dados dos Tickers - simbolos utilizados para representar cada commodite, ação, moeda ou crypto.
*CSV* para representar a movimentação das operações financeiras um arquivo .csv é criado a partir dos dados coletados com o yfinance. Durante a criação as operações e quantidades de ativos vendidos são gerados de forma aleatória com o Numpy.



### Carga de dados(AWS - Postgres):
A carga dos dados extraídos do yfinance será feita diretamente no banco de dados Postgres - AWS, já o arquivo csv ele será gerado na pasta projeto/seeds do projeto no dbt-core.

O comando 'dbt seed' irá inserir no banco de dados o quaisquer arquivos .csv que existam na pasta projeto/seeds criando automáticamente o schema.


### Transformação dos dados(dbt-core):
Nesta etapa as boas práticas do dbt recomenda criar algumas pastas dentro da pasta models:

projeto/models/staging:
Aqui será a realizada a primeira etapa de transformação para mudar o nome das colunas utilizando CTE SQL, é necessário criar o arquivo .sql que fará a transformação.

Importante incluir o arquivo .yml para documentar.

A partir da CTE o dbt-core irá criar as VIEWS para cada tabela inserida.