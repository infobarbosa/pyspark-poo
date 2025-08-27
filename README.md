# Engenharia de Software com PySpark
- Author: Prof. Barbosa  
- Contact: infobarbosa@gmail.com  
- Github: [infobarbosa](https://github.com/infobarbosa)

Este repositório é um guia passo a passo para refatorar um script PySpark monolítico, aplicando conceitos de Programação Orientada a Objetos (POO), organização de código e testes para criar uma aplicação mais robusta, manutenível e testável.

## Sumário
1. [Configuração Inicial](#configuração-inicial)
2. [O Ponto de Partida: Script com Inferência de Schema](#o-ponto-de-partida-script-com-inferência-de-schema)
3. [Passo 0: A Importância de Definir Schemas Explícitos](#passo-0-a-importância-de-definir-schemas-explícitos)
4. [Planejamento](#planejamento)
5. [Passo 1: Centralizando as Configurações](#passo-1-centralizando-as-configurações)
6. [Passo 2: Gerenciando a Sessão Spark](#passo-2-gerenciando-a-sessão-spark)
7. [Passo 3: Unificando a Leitura e Escrita de Dados (I/O)](#passo-3-unificando-a-leitura-e-escrita-de-dados-io)
8. [Passo 4: Isolando a Lógica de Negócio](#passo-4-isolando-a-lógica-de-negócio)
9. [Passo 5: Orquestrando a Aplicação no `main.py`](#passo-5-orquestrando-a-aplicação-no-mainpy)
10. [Passo 6: Aplicando Injeção de Dependências com uma Classe `Pipeline`](#passo-6-aplicando-injeção-de-dependências-com-uma-classe-pipeline)
11. [Passo 7: Adicionando Logging e Tratamento de Erros](#passo-7-adicionando-logging-e-tratamento-de-erros)
12. [Passo 8: Gerenciando Dependências com `requirements.txt`](#passo-8-gerenciando-dependências-com-requirementstxt)
13. [Passo 9: Garantindo a Qualidade do Código com Linter e Formatador](#passo-9-garantindo-a-qualidade-do-código-com-linter-e-formatador)
14. [Passo 10: Empacotando a Aplicação para Distribuição](#passo-10-empacotando-a-aplicação-para-distribuição)
15. [Passo 11: Garantido a qualidade com testes](#passo-11-garantindo-a-qualidade-com-testes)

---

### Configuração Inicial

Antes de começar, prepare seu ambiente:

ATENÇÃO! Se estiver utilizando Cloud9, utilize esse [tutorial](https://github.com/infobarbosa/data-engineering-cloud9]).

1. **Instale o Java 17:**
    ```bash
    sudo apt upgrade -y && sudo apt update -y

    ```

    ```bash
    sudo apt install -y openjdk-17-jdk

    ```

2.  **Crie uma pasta para o projeto:**
    ```bash
    mkdir -p data-engineering-pyspark/src
    mkdir -p data-engineering-pyspark/data/input
    mkdir -p data-engineering-pyspark/data/output
    
    ```
    
    ```bash
    cd data-engineering-pyspark
    
    ```

3.  **Crie um ambiente virtual e instale as dependências:**
    ```bash
    python3 -m venv .venv
    
    ```

    ```bash
    source .venv/bin/activate
    
    ```

    ```bash
    pip install pyspark
    
    ```

4.  **Baixe os datasets:**
    Execute o script para baixar os dados necessários para a pasta `data/`.
    
    **Clientes**
    ```bash
    curl -L -o ./data/input/clientes.gz https://raw.githubusercontent.com/infobarbosa/dataset-json-clientes/main/data/clientes.json.gz
    
    ```

    Um olhada rápida no arquivo de clientes
    ```bash
    gunzip -c data/input/clientes.gz | head -n 5

    ```

    **Pedidos**
    ```bash
    curl -L -o ./data/input/pedidos.gz https://raw.githubusercontent.com/infobarbosa/datasets-csv-pedidos/main/data/pedidos/pedidos-2024-01.csv.gz

    ```

    Uma olhada rápida no arquivo de pedidos
    ```bash
    gunzip -c ./data/input/pedidos.gz | head -n 5
    
    ```



### O Ponto de Partida: Script com Inferência de Schema

Vamos começar com um script monolítico. Copie o código abaixo e cole no seu arquivo `src/main.py`.

Note que, ao ler os arquivos (`.json` e `.csv`), **não estamos definindo um schema**. Estamos deixando o Spark "adivinhar" a estrutura e os tipos de dados.

```python
# src/main.py (Versão 1: com inferência de schema)
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

print("Abrindo a sessao spark")
spark = SparkSession.builder.appName("Analise de Pedidos").getOrCreate()

print("Abrindo o dataframe de clientes, deixando o Spark inferir o schema")
clientes = spark.read.option("compression", "gzip").json("data/input/clientes.gz")

clientes.printSchema()
clientes.show(5, truncate=False)

print("Abrindo o dataframe de pedidos, deixando o Spark inferir o schema")
# Para CSV, a inferência exige uma passagem extra sobre os dados (inferSchema=True)
pedidos = spark.read.option("compression", "gzip") \
                    .option("header", "true") \
                    .option("inferSchema", "true") \
                    .option("sep", ";") \
                    .csv("data/input/pedidos.gz")

pedidos.printSchema()

print("Adicionando a coluna valor_total")
pedidos = pedidos.withColumn("valor_total", F.col("valor_unitario") * F.col("quantidade"))
pedidos.show(5, truncate=False)

print("executando a logica de negocio para obter os top 10 clientes em valor total de pedidos")
calculado = pedidos.groupBy("id_cliente") \
    .agg(F.sum("valor_total").alias("valor_total")) \
    .orderBy(F.desc("valor_total")) \
    .limit(10)

print("criando o dataframe final incluindo os dados do cliente")
pedidos_clientes = calculado.join(clientes, clientes.id == calculado.id_cliente, "inner") \
    .select(calculado.id_cliente, clientes.nome, clientes.email, calculado.valor_total)

pedidos_clientes.show(20, truncate=False)

pedidos_clientes.write.mode("overwrite").parquet("data/output/pedidos_por_cliente")

spark.stop()
```

Agora execute:
```bash
spark-submit src/main.py

```

O output é longo, mas a parte que nos interessa são as linhas a seguir:
```
+----------+---------------------+-------------------------------------+-----------+
|id_cliente|nome                 |email                                |valor_total|
+----------+---------------------+-------------------------------------+-----------+
|2130      |José Miguel da Mata  |jose.miguel.da.matayqwfaf@outlook.com|6100.0     |
|3152      |Rafaela Aragão       |rafaela.aragaofzcjqe@gmail.com       |5700.0     |
|3342      |Mariana Rocha        |mariana.rochaytztlz@hotmail.com      |6000.0     |
|4130      |Ana Vitória Gonçalves|ana.vitoria.goncalvesjtlhdv@gmail.com|5900.0     |
|4281      |Maria Cecília Castro |maria.cecilia.castronuscva@gmail.com |5700.0     |
|4928      |Giovanna Barros      |giovanna.barroswxrhqf@live.com       |6000.0     |
|9346      |Felipe Pires         |felipe.pirespfgkrh@live.com          |10000.0    |
|12911     |晃 佐藤              |Huang .Zuo Teng xfpnwb@outlook.com   |7000.0     |
|13045     |Daniela Cavalcante   |daniela.cavalcantetkjrto@hotmail.com |6500.0     |
|14653     |Bryan Souza          |bryan.souzazxoccx@live.com           |7500.0     |
+----------+---------------------+-------------------------------------+-----------+
```

Se o output acima não estiver aparecendo, verifique se o Spark está rodando.

---

### Passo 0: A Importância de Definir Schemas Explícitos

Este script funciona, mas depender da inferência de schema é uma má prática em produção. Vamos entender o porquê.<br>
Deixar o Spark adivinhar o schema (`inferSchema`) é conveniente para exploração de dados, mas traz três grandes problemas para pipelines de dados sérios:

1.  **Desempenho:** Para inferir o schema, o Spark precisa ler os dados uma vez apenas para analisar a estrutura e os tipos. Depois, ele lê os dados uma segunda vez para de fato carregá-los. Isso pode dobrar o tempo de leitura, um custo enorme para datasets grandes.
2.  **Precisão:** O Spark pode interpretar um tipo de dado de forma errada. Uma coluna de CEP (`"01234-567"`) pode ser lida como `integer` (e virar `1234567`), ou uma data em formato específico pode virar `string`. Isso causa erros silenciosos que corrompem a análise.
3.  **Imprevisibilidade:** Se uma nova partição de dados chega com um tipo diferente (ex: um `id` que era `long` de repente contém um `string`), a inferência pode quebrar o pipeline ou, pior, mudar o tipo da coluna para `string`, escondendo o problema de qualidade dos dados.

A solução é **sempre** definir o schema explicitamente.

### Exemplo Prático: O Perigo em Ação

Vamos simular um problema comum. Imagine que temos um arquivo CSV simples em `data/input/codigos.csv` com códigos de produtos. Note que alguns códigos possuem zeros à esquerda, que são importantes.

**1. Crie o arquivo `data/input/codigos.csv` com o seguinte conteúdo:**
```csv
codigo,categoria
0101,A
0202,B
303,C
```

**2. Crie um script temporário para executar o teste (ex: `test_schema.py`):**
Agora, vamos tentar ler este arquivo com `inferSchema` e verificar o comprimento de cada código, que deveria ser 4 caracteres.

```python
# test_schema.py (TENTATIVA COM INFERÊNCIA DE SCHEMA)
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("ExemploInferSchema").getOrCreate()

# Lendo com inferSchema=True
df_codigos = spark.read.csv("data/input/codigos.csv", header=True, inferSchema=True)

print("Schema inferido pelo Spark:")
df_codigos.printSchema()

print("Dados lidos (com perda de dados!):")
df_codigos.show()

try:
    # Esta operação vai falhar!
    print("Tentando calcular o comprimento dos códigos...")
    df_comprimento = df_codigos.withColumn("comprimento", F.length(F.col("codigo")))
    df_comprimento.show()
except Exception as e:
    print(f"\nERRO! A operação falhou. Causa: O Spark inferiu 'codigo' como número e a função 'length' só funciona com texto.")

spark.stop()
```

**3. Execute e veja o erro:**
```bash
spark-submit test_schema.py
```
Ao executar, você verá duas coisas:
1.  O schema para a coluna `codigo` foi inferido como `IntegerType` (inteiro).
2.  Nos dados exibidos, os zeros à esquerda foram perdidos (`0101` virou `101`).
3.  A aplicação quebra com um erro, pois a função `length()` não pode ser aplicada a uma coluna do tipo inteiro.

Este é um erro silencioso que se tornou um problema real. A inferência não só corrompeu os dados, como também causou uma falha na aplicação.

**4. A Solução: Schema Explícito**
A solução é definir o schema explicitamente, tratando o código como `StringType` (texto).

```python
# test_schema.py (VERSÃO CORRETA COM SCHEMA EXPLÍCITO)
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder.appName("ExemploSchemaExplicito").getOrCreate()

# Definindo o schema correto
schema_correto = StructType([
    StructField("codigo", StringType(), True),
    StructField("categoria", StringType(), True)
])

# Lendo com o schema correto
df_codigos_correto = spark.read.csv("data/input/codigos.csv", header=True, schema=schema_correto)

print("Schema explícito definido:")
df_codigos_correto.printSchema()

print("Dados lidos corretamente:
")
df_codigos_correto.show()

# Agora a operação funciona!
df_comprimento_correto = df_codigos_correto.withColumn("comprimento", F.length(F.col("codigo")))
print("Cálculo do comprimento bem-sucedido:")
df_comprimento_correto.show()

spark.stop()
```
Com o schema explícito, os dados são lidos corretamente, os zeros à esquerda são preservados e a operação de string funciona como esperado. Este exemplo simples demonstra por que definir schemas é uma regra de ouro em pipelines de dados robustos.

---
A solução é **sempre** definir o schema explicitamente.


**1. Defina os Schemas com `StructType`:**

Vamos usar `StructType` e `StructField` para declarar a estrutura exata dos nossos dados.

```python
# Importações necessárias para definir o schema
from pyspark.sql.types import (StructType, StructField, StringType, LongType, 
                               ArrayType, DateType, FloatType, TimestampType)

# Schema para o dataframe de clientes
schema_clientes = StructType([
    StructField("id", LongType(), True),
    StructField("nome", StringType(), True),
    StructField("data_nasc", DateType(), True),
    StructField("cpf", StringType(), True),
    StructField("email", StringType(), True),
    StructField("interesses", ArrayType(StringType()), True)
])

# Schema para o dataframe de pedidos
schema_pedidos = StructType([
    StructField("id_pedido", StringType(), True),
    StructField("produto", StringType(), True),
    StructField("valor_unitario", FloatType(), True),
    StructField("quantidade", LongType(), True),
    StructField("data_criacao", TimestampType(), True),
    StructField("uf", StringType(), True),
    StructField("id_cliente", LongType(), True)
])
```

**2. Atualize o `src/main.py` para usar os Schemas:**

Agora, substitua todo o conteúdo do `src/main.py` pela versão abaixo. Este será nosso **ponto de partida oficial** para a refatoração.

```python
# src/main.py (Versão 2: Ponto de Partida Oficial com Schema Explícito)
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType, LongType, 
                               ArrayType, DateType, FloatType, TimestampType)

spark = SparkSession.builder.appName("Analise de Pedidos").getOrCreate()

print("Definindo schema do dataframe de clientes")
schema_clientes = StructType([
    StructField("id", LongType(), True),
    StructField("nome", StringType(), True),
    StructField("data_nasc", DateType(), True),
    StructField("cpf", StringType(), True),
    StructField("email", StringType(), True),
    StructField("interesses", ArrayType(StringType()), True)
])
print("Abrindo o dataframe de clientes")
clientes = spark.read.option("compression", "gzip").json("data/input/clientes.gz", schema=schema_clientes)

clientes.show(5, truncate=False)

print("Definindo schema do dataframe de pedidos")
schema_pedidos = StructType([
    StructField("id_pedido", StringType(), True),
    StructField("produto", StringType(), True),
    StructField("valor_unitario", FloatType(), True),
    StructField("quantidade", LongType(), True),
    StructField("data_criacao", TimestampType(), True),
    StructField("uf", StringType(), True),
    StructField("id_cliente", LongType(), True)
])

print("Abrindo o dataframe de pedidos")
pedidos = spark.read.option("compression", "gzip").csv("data/input/pedidos.gz", header=True, schema=schema_pedidos, sep=";")

print("Adicionando a coluna valor_total")
pedidos = pedidos.withColumn("valor_total", F.col("valor_unitario") * F.col("quantidade"))
pedidos.show(5, truncate=False)

print("Calculando o valor total de pedidos por cliente e filtrar os 10 maiores")
calculado = pedidos.groupBy("id_cliente") \
    .agg(F.sum("valor_total").alias("valor_total")) \
    .orderBy(F.desc("valor_total")) \
    .limit(10)

calculado.show(10, truncate=False)

print("Fazendo a junção dos dataframes")
pedidos_clientes = calculado.join(clientes, clientes.id == calculado.id_cliente, "inner") \
    .select(calculado.id_cliente, clientes.nome, clientes.email, calculado.valor_total)

pedidos_clientes.show(20, truncate=False)

print("Escrevendo o resultado em parquet")
pedidos_clientes.write.mode("overwrite").parquet("data/output/pedidos_por_cliente")

spark.stop()
```
Com nosso ponto de partida agora robusto e performático, podemos começar a refatoração para a Programação Orientada a Objetos.

---

### Planejamento

Nosso objetivo é evoluir de um simples script para uma aplicação PySpark bem estruturada. Para isso, vamos organizar nosso código em diretórios, onde cada um terá uma responsabilidade única. Esta é a estrutura que vamos construir:

```
.
└── src/
    ├── __init__.py
    ├── config/
    │   ├── __init__.py
    │   └── settings.py         # <-- Para centralizar os caminhos dos arquivos
    ├── session/
    │   ├── __init__.py
    │   └── spark_session.py    # <-- Classe para gerenciar a sessão Spark
    ├── io_utils/
    │   ├── __init__.py
    │   └── data_handler.py     # <-- Classe para ler e escrever dados (I/O)
    ├── processing/
    │   ├── __init__.py
    │   └── transformations.py  # <-- Classe para a lógica de negócio
    └── main.py                 # <-- Orquestrador principal da aplicação
```

Vamos seguir este plano passo a passo.

---

### Passo 1: Centralizando as Configurações

É uma boa prática não deixar "strings mágicas" (como caminhos de arquivos) espalhadas pelo código. Vamos centralizá-las em um único lugar.

**1. Crie o diretório e o arquivo de inicialização:**

```bash
mkdir -p src/config
touch src/config/__init__.py

```

**2. Crie o arquivo `src/config/settings.py`:**

Este arquivo conterá os caminhos para nossos dados de entrada e para a pasta de saída onde salvaremos o resultado.
```bash
touch src/config/settings.py
```

**3. Adicione o seguinte código ao `src/config/settings.py`:**

```python
# src/config/settings.py

# Caminhos para os dados de entrada (fontes)
CLIENTES_PATH = "data/input/clientes.gz"
PEDIDOS_PATH = "data/input/pedidos.gz"

# Caminho para os dados de saída (destino)
OUTPUT_PATH = "data/output/pedidos_por_cliente"
```

---

**4. Faça ajustes no script `src/main.py`**
- Importe o pacote config.settings:
  ```python
  from config.settings import CLIENTES_PATH, PEDIDOS_PATH, OUTPUT_PATH
  ```

- Substitua os paths explícitos pelas respectivas variáveis
  
  Clientes
  ```python
  clientes = spark.read.option("compression", "gzip").json(CLIENTES_PATH, schema=schema_clientes)
  ```

  Pedidos
  ```python
  pedidos = spark.read.option("compression", "gzip").csv(PEDIDOS_PATH, header=True, schema=schema_pedidos, sep=";")
  ```

  Resultado
  ```python
  pedidos_clientes.write.mode("overwrite").parquet(OUTPUT_PATH)
  ```

### Passo 2: Gerenciando a Sessão Spark

A criação da `SparkSession` também pode ser isolada para ser mais reutilizável e fácil de configurar.

**1. Crie o diretório e o arquivo de inicialização:**

```bash
mkdir -p src/session
touch src/session/__init__.py

```

**2. Crie o arquivo `src/session/spark_session.py`:**
```bash
touch src/session/spark_session.py

```

**3. Adicione o seguinte código a ele:**

Esta classe simples será responsável por fornecer uma sessão Spark configurada para nossa aplicação.

```python
# src/session/spark_session.py
from pyspark.sql import SparkSession

class SparkSessionManager:
    """
    Gerencia a criação e o acesso à sessão Spark.
    """
    @staticmethod
    def get_spark_session(app_name: str = "alun-data-eng-pyspark-app") -> SparkSession:
        """
        Cria e retorna uma sessão Spark.

        :param app_name: Nome da aplicação Spark.
        :return: Instância da SparkSession.
        """
        return SparkSession.builder \
            .appName(app_name) \
            .master("local[*]") \
            .getOrCreate()

```

**4. Faça os ajustes em `src/main.py`**
- Importando o pacote
  ```python
  from session.spark_session import SparkSessionManager
  ```

- Instanciando a sessão spark
  ```python
  spark = SparkSessionManager.get_spark_session()
  ```
---

### Passo 3: Unificando a Leitura e Escrita de Dados (I/O)

Vamos criar uma classe que lida com todas as operações de entrada (leitura) e saída (escrita) de dados.

**1. Crie o diretório e o arquivo de inicialização:**

```bash
mkdir -p src/io_utils
touch src/io_utils/__init__.py
```

**2. Crie o arquivo `src/io_utils/data_handler.py`:**
```bash
touch src/io_utils/data_handler.py
```

**3. Adicione o seguinte código a ele:**

Esta classe irá conter a lógica para ler os arquivos de clientes e pedidos, e também um novo método para escrever nosso resultado final em formato Parquet.

```python
# src/io_utils/data_handler.py
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (StructType, StructField, StringType, LongType,
                               ArrayType, DateType, FloatType, TimestampType)

class DataHandler:
    """
    Classe responsável pela leitura (input) e escrita (output) de dados.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def _get_schema_clientes(self) -> StructType:
        """Define e retorna o schema para o dataframe de clientes."""
        return StructType([
            StructField("id", LongType(), True),
            StructField("nome", StringType(), True),
            StructField("data_nasc", DateType(), True),
            StructField("cpf", StringType(), True),
            StructField("email", StringType(), True),
            StructField("interesses", ArrayType(StringType()), True)
        ])

    def _get_schema_pedidos(self) -> StructType:
        """Define e retorna o schema para o dataframe de pedidos."""
        return StructType([
            StructField("id_pedido", StringType(), True),
            StructField("produto", StringType(), True),
            StructField("valor_unitario", FloatType(), True),
            StructField("quantidade", LongType(), True),
            StructField("data_criacao", TimestampType(), True),
            StructField("uf", StringType(), True),
            StructField("id_cliente", LongType(), True)
        ])

    def load_clientes(self, path: str) -> DataFrame:
        """Carrega o dataframe de clientes a partir de um arquivo JSON."""
        schema = self._get_schema_clientes()
        return self.spark.read.option("compression", "gzip").json(path, schema=schema)

    def load_pedidos(self, path: str) -> DataFrame:
        """Carrega o dataframe de pedidos a partir de um arquivo CSV."""
        schema = self._get_schema_pedidos()
        return self.spark.read.option("compression", "gzip").csv(path, header=True, schema=schema, sep=";")

    def write_parquet(self, df: DataFrame, path: str):
        """
        Salva o DataFrame em formato Parquet, sobrescrevendo se já existir.

        :param df: DataFrame a ser salvo.
        :param path: Caminho de destino.
        """
        df.write.mode("overwrite").parquet(path)
        print(f"Dados salvos com sucesso em: {path}")

```

**4. Faça os ajustes em `main.py`:**

- Importar DataHandler do pacote io_utils.data_handler:
  ```python
  from io_utils.data_handler import DataHandler
  ```

- Criar uma instância da classe DataHandler:
  ```python
  dh = DataHandler(spark)
  ```

- Substituir a carga dos dataframes de clientes e pedidos pelos seguintes trechos:
  ```python
  clientes = dh.load_clientes(path = CLIENTES_PATH)
  ```

  ```python
  pedidos = dh.load_pedidos(path = PEDIDOS_PATH)
  ```

- Substituir a escrita de dados parquet pelo seguinte trecho:
  ```python
  dh.write_parquet(df=pedidos_clientes, path=OUTPUT_PATH)
  ```

---

### Passo 4: Isolando a Lógica de Negócio

Esta etapa é semelhante à anterior, mas vamos garantir que o arquivo esteja no lugar certo.

**1. Crie o diretório e o arquivo de inicialização:**

```bash
mkdir -p src/processing
touch src/processing/__init__.py
```

**2. Crie o arquivo `src/processing/transformations.py`:**
```bash
touch src/processing/transformations.py
```

**3. Adicione o seguinte código a ele:**

Esta classe contém as regras de negócio puras, que transformam um DataFrame de entrada em um DataFrame de saída.

```python
# src/processing/transformations.py
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

class Transformation:
    """
    Classe que contém as transformações e regras de negócio da aplicação.
    """

    def add_valor_total_pedidos(self, pedidos_df: DataFrame) -> DataFrame:
        """Adiciona a coluna 'valor_total' (valor_unitario * quantidade) ao DataFrame de pedidos."""
        return pedidos_df.withColumn("valor_total", F.col("valor_unitario") * F.col("quantidade"))

    def get_top_10_clientes(self, pedidos_df: DataFrame) -> DataFrame:
        """Calcula o valor total de pedidos por cliente e retorna os 10 maiores."""
        return pedidos_df.groupBy("id_cliente") \
            .agg(F.sum("valor_total").alias("valor_total")) \
            .orderBy(F.desc("valor_total")) \
            .limit(10)

    def join_pedidos_clientes(self, pedidos_df: DataFrame, clientes_df: DataFrame) -> DataFrame:
        """Faz a junção entre os DataFrames de pedidos e clientes."""
        return pedidos_df.join(clientes_df, clientes_df.id == pedidos_df.id_cliente, "inner") \
            .select(pedidos_df.id_cliente, clientes_df.nome, clientes_df.email, pedidos_df.valor_total)
```

**4. Faça os seguintes ajustes em `main.py` :**
  - Importe o pacote processing.transformations
    ```python
    from processing.transformations import Transformation
    ```

  - Crie uma instância da classe Transformation
    ```python
    transformer = Transformation()
    ```

  - Substitua `pedidos = pedidos.withColumn("valor_total"...` por:
    ```python
    pedidos = transformer.add_valor_total_pedidos(pedidos)
    ```

  - Substitua `calculado = pedidos.groupBy("id_cliente")...` por:
    ```python
    calculado = transformer.get_top_10_clientes(pedidos)
    ``` 

  - Substitua `pedidos_clientes = calculado.join(clientes,...` por:
    ```python
    pedidos_clientes = transformer.join_pedidos_clientes(calculado, clientes)
    ```

  - Faça o teste:
    ```bash
    spark-submit src/main.py

    ```
  
---

### Passo 5: Orquestrando a Aplicação no `main.py`

Agora, vamos juntar todas as peças. O `main.py` se tornará um orquestrador limpo e legível, que apenas chama os métodos das nossas classes.

**1. Substitua todo o conteúdo do `src/main.py` pelo código abaixo:**

```python
# src/main.py
from session.spark_session import SparkSessionManager
from io_utils.data_handler import DataHandler
from processing.transformations import Transformation
import config.settings as settings

def main():
    """
    Função principal que orquestra a execução do pipeline de dados.
    """
    # 1. Inicialização
    spark = SparkSessionManager.get_spark_session("Análise de Pedidos com POO")
    data_handler = DataHandler(spark)
    transformer = Transformation()

    print("Pipeline iniciado...")

    # 2. Carga de Dados (Input)
    print("Carregando dados de clientes e pedidos...")
    clientes_df = data_handler.load_clientes(settings.CLIENTES_PATH)
    pedidos_df = data_handler.load_pedidos(settings.PEDIDOS_PATH)

    # 3. Transformações (Processing)
    print("Aplicando transformações...")
    pedidos_com_valor_total_df = transformer.add_valor_total_pedidos(pedidos_df)
    top_10_clientes_df = transformer.get_top_10_clientes(pedidos_com_valor_total_df)
    resultado_final_df = transformer.join_pedidos_clientes(top_10_clientes_df, clientes_df)

    # 4. Exibição e Salvamento (Output)
    print("Top 10 clientes com maior valor total de pedidos:")
    resultado_final_df.show(10, truncate=False)

    print("Salvando resultado em formato Parquet...")
    data_handler.write_parquet(resultado_final_df, settings.OUTPUT_PATH)

    # 5. Finalização
    spark.stop()
    print("Pipeline concluído com sucesso!")


if __name__ == "__main__":
    main()
```

Faça o teste:
```bash
spark-submit src/main.py

```

#### O que ganhamos com esta nova estrutura?

-   **Organização Superior:** Cada parte da aplicação tem seu lugar. Se precisar alterar algo sobre a sessão Spark, você sabe que deve ir em `src/session`. Se a forma de ler um arquivo mudar, o lugar é `src/io_utils`.
-   **Configuração Centralizada:** Mudar os caminhos dos arquivos de entrada ou saída agora é trivial e seguro, sem risco de quebrar a lógica da aplicação.
-   **Máxima Reutilização:** Cada componente (`DataHandler`, `Transformation`, `SparkSessionManager`) pode ser facilmente importado e reutilizado em outros projetos ou notebooks.
-   **Testabilidade Aprimorada:** A lógica de negócio em `Transformation` continua pura e fácil de testar. Agora, também podemos testar o `DataHandler` de forma isolada, se necessário.

---

### Passo 6: Aplicando Injeção de Dependências com uma Classe `Pipeline`

Até agora, nossa função `main` está fazendo duas coisas: criando os objetos (`DataHandler`, `Transformation`) e orquestrando as chamadas dos métodos. Vamos dar um passo adiante na organização do código usando um padrão chamado **Injeção de Dependências (DI)**.

A ideia é simples: em vez de uma classe ou função criar os objetos de que precisa (suas "dependências"), ela os recebe de fora, geralmente em seu construtor. Isso desacopla o código e, mais importante, torna-o muito mais fácil de testar.

Vamos criar uma classe `Pipeline` que conterá toda a lógica de orquestração. O `main.py` se tornará a **"Raiz de Composição"** (`Composition Root`), o único lugar responsável por montar e "ligar" os componentes da nossa aplicação.

**1. Crie o arquivo `src/pipeline/pipeline.py`:**

Este arquivo irá abrigar nossa nova classe orquestradora.

```bash
mkdir -p src/pipeline
touch src/pipeline/__init__.py

```

```bash
touch src/pipeline/pipeline.py

```

**2. Adicione o seguinte código ao `src/pipeline/pipeline.py`:**

A classe `Pipeline` receberá a sessão Spark como uma dependência em seu construtor. Ela então usará essa sessão para inicializar seus próprios componentes, como o `DataHandler`.

```python
# src/pipeline/pipeline.py
from pyspark.sql import SparkSession
from io_utils.data_handler import DataHandler
from processing.transformations import Transformation
import config.settings as settings

class Pipeline:
    """
    Encapsula a lógica de execução do pipeline de dados.
    As dependências são injetadas para facilitar os testes e a manutenção.
    """
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.data_handler = DataHandler(self.spark)
        self.transformer = Transformation()

    def run(self):
        """
        Executa o pipeline completo: carga, transformação, e salvamento.
        """
        print("Pipeline iniciado...")

        # Carga de Dados
        print("Carregando dados de clientes e pedidos...")
        clientes_df = self.data_handler.load_clientes(settings.CLIENTES_PATH)
        pedidos_df = self.data_handler.load_pedidos(settings.PEDIDOS_PATH)

        # Transformações
        print("Aplicando transformações...")
        pedidos_com_valor_total_df = self.transformer.add_valor_total_pedidos(pedidos_df)
        top_10_clientes_df = self.transformer.get_top_10_clientes(pedidos_com_valor_total_df)
        resultado_final_df = self.transformer.join_pedidos_clientes(top_10_clientes_df, clientes_df)

        # Exibição e Salvamento
        print("Top 10 clientes com maior valor total de pedidos:")
        resultado_final_df.show(10, truncate=False)

        print("Salvando resultado em formato Parquet...")
        self.data_handler.write_parquet(resultado_final_df, settings.OUTPUT_PATH)

        print("Pipeline concluído com sucesso!")
```

**3. Refatore o `src/main.py` para ser a Raiz de Composição:**

Agora, o `main.py` fica muito mais limpo. Sua única responsabilidade é inicializar os objetos e iniciar o processo.

Substitua todo o conteúdo do `src/main.py` por este código:

```python
# src/main.py
from session.spark_session import SparkSessionManager
from pipeline.pipeline import Pipeline

def main():
    """
    Função principal que atua como a "Raiz de Composição".
    Configura e executa o pipeline.
    """
    # 1. Inicialização da sessão Spark
    spark = SparkSessionManager.get_spark_session("Análise de Pedidos com DI")
    
    # 2. Injeção de Dependência e Execução
    # A sessão Spark é "injetada" na criação do pipeline
    pipeline = Pipeline(spark)
    pipeline.run()

    # 3. Finalização
    spark.stop()

if __name__ == "__main__":
    main()
```

**4. Garanta que o diretório `src` seja um pacote Python:**

Para que os imports como `from pipeline.pipeline import Pipeline` funcionem corretamente, o Python precisa tratar o diretório `src` como um "pacote". Para isso, crie um arquivo `__init__.py` vazio dentro dele.

```bash
touch src/__init__.py

```

**5. Faça o teste:**
```bash
spark-submit src/main.py

```

#### O Grande Ganho: Testabilidade

Por que fizemos tudo isso? **Para facilitar os testes.**

Imagine que você queira testar a classe `Pipeline` sem ler arquivos reais do disco. Com a injeção de dependências, você poderia criar um `DataHandler` "falso" (um *mock*) que retorna DataFrames de teste pré-definidos e injetá-lo no `Pipeline`. O `Pipeline` executaria sua lógica sem saber que está usando dados falsos, permitindo que você verifique o resultado de forma rápida e isolada.

Este design nos prepara para o próximo nível de maturidade de software: **testes automatizados**.

---

### Passo 7: Adicionando Logging e Tratamento de Erros

Uma aplicação robusta não usa `print()` para registrar seu progresso e não quebra sem dar informações claras. Vamos substituir nossos `prints` por um sistema de **logging** profissional e adicionar um **tratamento de erros** para tornar nosso pipeline mais resiliente.

A sua aplicação (o ponto de entrada, como main.py ou app.py) é responsável por configurar o logging. É aqui que você decide para onde as mensagens vão (console, arquivo, etc.), qual o formato delas e qual o nível mínimo de severidade a ser registrado.

Os seus módulos e pacotes (as "bibliotecas" do seu projeto) nunca devem configurar o logging. Eles devem apenas pedir um logger e usá-lo para enviar mensagens.<br>
Isso evita que um módulo sobreponha a configuração de outro, garantindo um comportamento uniforme e previsível em todo o projeto.

#### A Hierarquia de Loggers
O módulo `logging` do Python organiza os loggers em uma hierarquia baseada em nomes separados por pontos. Por exemplo, um logger chamado pacote1.modulo1 é filho do logger pacote1, que por sua vez é filho do logger raiz (root).

A grande vantagem é que, por padrão, as mensagens de um logger filho são propagadas para os "handlers" (manipuladores) do seu logger pai. É por isso que podemos configurar o logger raiz uma única vez e todos os outros loggers do projeto enviarão suas mensagens para os handlers configurados nele.

A melhor prática é obter um logger em cada módulo usando a variável especial __name__:

```python
import logging
logger = logging.getLogger(__name__)
```

**1. Configure o Logging**

  - Em `src/main.py`, adicione as linhas abaixo:
    ```python
    # importe o pacote logging
    import logging
    # outros imports...
    ```

    ```python
    # Crie a configuração do logging
    def configurar_logging():
      """Configura o logging para todo o projeto."""
      logging.basicConfig(
          # Nível mínimo de severidade para ser registrado.
          # DEBUG < INFO < WARNING < ERROR < CRITICAL
          level=logging.INFO,

          # Formato da mensagem de log.
          format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
          datefmt='%Y-%m-%d %H:%M:%S',

          # Lista de handlers. Aqui, estamos logando para um arquivo e para o console.
          handlers=[
              logging.FileHandler("dataeng-pyspark-poo.log"), # Log para arquivo
              logging.StreamHandler()                         # Log para o console (terminal)
          ]
      )
      logging.info("Logging configurado.")

    ```

    ```python
    # Chame a configuração do logging no ponto de entrada da aplicação
    if __name__ == "__main__":

      configurar_logging()

      logging.info("Aplicação iniciada.")

      # continua para a sua lógica
    ```

  - Em todas as classes, adicione a configuração do logger no início do arquivo e substitua todos os `print()` por chamadas ao `logging`.<br>

    Abaixo está um exemplo na classe `src/pipeline.py`:

    ```python
    # src/pipeline.py
    import logging
    from pyspark.sql import SparkSession
    from io_utils.data_handler import DataHandler
    from processing.transformations import Transformation
    import config.settings as settings

    logger = logging.getLogger(__name__)

    class Pipeline:
        # ... (o construtor __init__ permanece o mesmo) ...

        def run(self):
            logger.info("Pipeline iniciado...")
            # ... (substitua os prints por logging.info) ...
            logger.info("Pipeline concluído com sucesso!")
    ```

**2. Tratamento de erros em `data_handler.py`:**

O `DataHandler` pode gerar erros durante a leitura de arquivos. Vamos adicionar um bloco `try...except`**
  - Importando o pacote
  
    ```python
    from pyspark.errors import AnalysisException 
    ```

  - Adicionalmente vamos precisar do pacote `logging` também:
    ```python
    import logging

    ```

  - Adicione a configuração do logging:
    ```python
    # Configuração centralizada do logging
    logger = logging.getLogger(__name__)

    ```

  - Substituindo o trecho de código que carrega os dados de **pedidos** por:
    
    ```python
    # src/io_utils/data_handler.py
    def load_pedidos(self, path: str) -> DataFrame:
        """Carrega o dataframe de pedidos a partir de um arquivo CSV."""
        schema = self._get_schema_pedidos()
        try:
            return self.spark.read.option("compression", "gzip").csv(path, header=True, schema=schema, sep=";")
        except AnalysisException as e:
            if "PATH_NOT_FOUND" in str(e):
                logger.error(f"Arquivo não encontrado: {path}")

            raise Exception(f"Erro ao carregar pedidos: {e}")
    ```

**3. Tratamento de erros em `pipeline.py`:**
  - Substitua o trecho `pedidos_df = self.data_handler...` por:
    ```python
    try:
        pedidos_df = self.data_handler.load_pedidos(settings.PEDIDOS_PATH)
    except Exception as e:
        logger.error(f"Problemas ao carregar dados de pedidos: {e}")
        return  # Interrompe o pipeline se os pedidos não puderem ser carregados

    ```

**4. Tratamento de Erros em `main.py`:**

O ponto de entrada da nossa aplicação (`main.py`) é o lugar ideal para capturar qualquer erro que possa ocorrer durante a execução do pipeline. Vamos envolver a chamada `pipeline.run()` em um bloco `try...except`.

Atualize o `src/main.py` com o seguinte código:

```python
# src/main.py
import logging
from session.spark_session import SparkSessionManager
from pipeline import Pipeline

def configurar_logging():
  """Configura o logging para todo o projeto."""
  logging.basicConfig(
      level=logging.INFO,
      format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
      datefmt='%Y-%m-%d %H:%M:%S',

      handlers=[
          logging.FileHandler("dataeng-pyspark-poo.log"),
          logging.StreamHandler()
      ]
  )
  logging.info("Logging configurado.")

def main():
    """
    Função principal que configura e executa o pipeline,
    com tratamento de erros.
    """
    spark = None  # Inicializa a variável spark
    try:
        spark = SparkSessionManager.get_spark_session("Análise de Pedidos com DI e Logging")
        
        pipeline = Pipeline(spark)
        pipeline.run()

    except Exception as e:
        logging.error(f"Ocorreu um erro inesperado na execução do programa: {e}")
    finally:
        if spark:
            spark.stop()
            logging.info("Sessão Spark finalizada.")

if __name__ == "__main__":
    main()
```

**5. Testando:**
  - Execute o comando a seguir no terminal:
    ```bash
    mv data/input/pedidos.gz data/input/pedidos.gz.backup

    ```

  - Execute a aplicação:
    ```bash
    spark-submit src/main.py
    
    ```

  - Verifique a mensagem de erro
  
  - Para voltar o arquivo original, execute:
    ```bash
    mv data/input/pedidos.gz.backup data/input/pedidos.gz
    
    ```


**6. Conclusão:**

Com essas mudanças, se um arquivo não for encontrado, a aplicação não vai mais quebrar com um stack trace gigante. Em vez disso, ela registrará uma mensagem de erro clara e finalizará a sessão Spark de forma segura.

---

### Passo 8: Gerenciando Dependências com `requirements.txt`

Para garantir que nossa aplicação funcione da mesma forma em qualquer máquina, precisamos fixar as versões das bibliotecas que usamos.

**1. Crie o arquivo `requirements.txt`:**

Na raiz do seu projeto, crie um arquivo chamado `requirements.txt`.

```bash
touch requirements.txt

```

**2. Adicione a dependência do PySpark:**

Abra o `requirements.txt` e adicione a versão exata do PySpark que você está usando. Você pode descobrir a versão com o comando `pip show pyspark`.

```
# requirements.txt
pyspark==4.0.0
```
*(Nota: use a versão que estiver instalada no seu ambiente)*

**3. Atualize as instruções de instalação:**

A partir de agora, a forma correta de instalar as dependências do projeto é:

```bash
pip install -r requirements.txt

```
Isso garante que qualquer pessoa que execute seu projeto usará exatamente a mesma versão do PySpark.

---

### Passo 9: Garantindo a Qualidade do Código com Linter e Formatador

Para manter nosso código limpo, legível e livre de erros comuns, vamos usar duas ferramentas padrão da indústria: `ruff` (linter) e `black` (formatador).

**1. Adicione as ferramentas ao `requirements.txt`:**

```
# requirements.txt
pyspark==4.0.0
ruff==0.12.9
black==25.1.0
```
*(Nota: você pode usar versões mais recentes se desejar)*

**2. Instale as novas dependências:**

```bash
pip install -r requirements.txt

```

**3. Como usar as ferramentas:**

-   **Para verificar a qualidade do código (Linting):**
    Execute o `ruff` na raiz do projeto. Ele apontará problemas de estilo, bugs potenciais e código não utilizado.
    ```bash
    ruff check .

    ```

-   **Para formatar o código automaticamente (Formatação):**
    Execute o `black` na raiz do projeto. Ele irá reformatar todos os seus arquivos `.py` para um estilo consistente.
    ```bash
    black .

    ```

Adotar essas ferramentas torna o código mais profissional e fácil de manter, especialmente ao trabalhar em equipe.

---

### Passo 10: Empacotando a Aplicação para Distribuição

O passo final da jornada de um engenheiro de software é tornar sua aplicação distribuível. Em vez de pedir para alguém clonar seu repositório e executar um script, vamos empacotar nosso pipeline em um formato que pode ser instalado com `pip` e executado com um simples comando no terminal.

**1. Crie o arquivo `pyproject.toml`:**

Este é o arquivo de configuração padrão para projetos Python modernos. Crie-o na raiz do seu projeto.

```bash
touch pyproject.toml

```

**2. Adicione o conteúdo de configuração:**

Copie o seguinte conteúdo para o seu `pyproject.toml`. Ele define o nome do nosso pacote, a versão, as dependências e, o mais importante, um *script de ponto de entrada*.

```toml
# pyproject.toml
[build-system]
requires = ["setuptools>=61.0"]
build-backend = "setuptools.build_meta"

[project]
name = "dataeng_pyspark_data_pipeline"
version = "0.1.2"
authors = [
  { name="infobarbosa", email="infobarbosa@gmail.com" },
]
description = "Um pipeline de dados com PySpark estruturado com boas práticas de engenharia de software."
readme = "README.md"
requires-python = ">=3.8"
license = "MIT"
classifiers = [
    "Programming Language :: Python :: 3",
    "Operating System :: OS Independent",
]
dependencies = [
    "pyspark==4.0.0"
]

[project.optional-dependencies]
dev = [
    "ruff==0.12.9",
    "black==25.1.0",
    "build==1.3.0"
]

[project.scripts]
run-data-pipeline = "main:main"

[tool.setuptools]
package-dir = {"" = "src"}
packages = {find = {where = ["src"]}}

```

**3. Crie um arquivo `MANIFEST.in`:**

  - O arquivo:
  ```bash
  touch MANIFEST.in

  ```

  - O conteúdo:
  ```
  include requirements.txt
  include README.md

  ```

**4. Crie o arquivo `README.md`:**

Este é o arquivo que será exibido quando alguém acessar o repositório.
```bash
echo "[DATAENG] Meu projeto bem estruturado de dados com PySpark" > README.md

```

**5. Adicione o pacote `build` a `requirements.txt`:**
  - Configurando o arquivo:

    ```
    # requirements.txt
    pyspark==4.0.0
    ruff==0.12.9
    black==25.1.0
    build==1.3.0
    ```

  - Instalando:
    ```bash
    pip install -r requirements.txt

    ```

**6. Construa o pacote:**

```bash
python -m build

```

Você verá que um novo diretório `dist/` foi criado, contendo o arquivo `.whl` (Wheel).

**8. Instale e execute sua aplicação:**

Agora, para testar, você pode instalar sua própria aplicação como se fosse qualquer outra biblioteca.

  - Desinstalando a versão anterior se existir
    ```bash
    # Desinstale a versão de desenvolvimento se já existir
    pip uninstall dataeng_pyspark_data_pipeline -y

    ```

  - Instalando a versão distribuída
    ```bash
    # Instala o pacote que acabamos de criar
    pip install dist/*.whl

    ```

    [OPCIONAL] - Caso tenha instado antes e precise forçar a reinstalação:
    ```
    pip install --force-reinstall dist/dataeng_pyspark_data_pipeline-0.1.0-py3-none-any.whl

    ```

  - Executando a aplicação
    ```bash
    spark-submit --master local[*] \
    --py-files dist/dataeng_pyspark_data_pipeline-0.1.0-py3-none-any.whl \
    src/main.py

    ```

### Passo 11: Garantido a qualidade com testes

Até agora, construímos uma aplicação robusta, bem estruturada e distribuível. Mas como podemos garantir que a lógica de negócio — o coração da nossa aplicação — está funcionando corretamente e continuará funcionando conforme o projeto evolui? A resposta é: **testes automatizados**.

Testar a lógica de transformação de dados é crucial porque:
- **Valida a Correção:** Garante que seus cálculos e regras de negócio estão sendo aplicados exatamente como o esperado.
- **Previne Regressões:** Se você fizer uma alteração no futuro que quebre uma lógica existente, o teste irá falhar, alertando-o imediatamente.
- **Facilita a Manutenção:** Com uma suíte de testes robusta, você pode refatorar e melhorar seu código com a confiança de que não está introduzindo bugs.

Vamos focar em **testes unitários** para nossa classe `Transformation`, pois ela contém a lógica pura, sem depender de I/O (leitura/escrita de arquivos).

**1. Adicione o `pytest` às Dependências:**

`pytest` é o framework de testes mais popular para Python. Vamos adicioná-lo ao nosso projeto.

Atualize o `requirements.txt`:
```
# requirements.txt
pyspark==4.0.0
ruff==0.12.9
black==25.1.0
build==1.3.0
pytest==8.4.1  # Adicione esta linha
```
*(Nota: você pode usar uma versão mais recente do pytest se desejar)*

E instale a nova dependência:
```bash
pip install -r requirements.txt

```

**2. Crie a Estrutura de Testes:**

É uma convenção criar um diretório `tests` na raiz do projeto, separado do código-fonte (`src`).

```bash
mkdir tests
touch tests/__init__.py

```

Dentro deste diretório, criaremos um arquivo para testar nossas transformações. O nome do arquivo deve começar com `test_`.

```bash
touch tests/test_transformations.py

```

**3. Escrevendo Nosso Primeiro Teste:**

Vamos escrever um teste para o método `add_valor_total_pedidos` da nossa classe `Transformation`. O teste seguirá 3 passos: **Arrange** (Preparar), **Act** (Agir) e **Assert** (Verificar).

Adicione o seguinte código ao arquivo `tests/test_transformations.py`:

```python
# tests/test_transformations.py
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType
from src.processing.transformations import Transformation

@pytest.fixture(scope="session")
def spark_session():
    """
    Cria uma SparkSession para ser usada em todos os testes.
    A sessão é finalizada automaticamente ao final da execução dos testes.
    """
    spark = SparkSession.builder \
        .appName("PySpark Unit Tests") \
        .master("local[*]") \
        .getOrCreate()
    yield spark
    spark.stop()

def test_add_valor_total_pedidos(spark_session):
    """
    Testa a função add_valor_total_pedidos para garantir que a coluna 'valor_total'
    é calculada corretamente.
    """
    # 1. Arrange (Preparar os dados de entrada e o resultado esperado)
    transformer = Transformation()

    schema_entrada = StructType([
        StructField("produto", StringType(), True),
        StructField("valor_unitario", FloatType(), True),
        StructField("quantidade", LongType(), True),
    ])
    dados_entrada = [
        ("Produto A", 10.0, 2),
        ("Produto B", 5.5, 3),
        ("Produto C", 100.0, 1)
    ]
    df_entrada = spark_session.createDataFrame(dados_entrada, schema_entrada)

    schema_esperado = StructType([
        StructField("produto", StringType(), True),
        StructField("valor_unitario", FloatType(), True),
        StructField("quantidade", LongType(), True),
        StructField("valor_total", FloatType(), True)
    ])
    dados_esperados = [
        ("Produto A", 10.0, 2, 20.0),
        ("Produto B", 5.5, 3, 16.5),
        ("Produto C", 100.0, 1, 100.0)
    ]
    df_esperado = spark_session.createDataFrame(dados_esperados, schema_esperado)

    # 2. Act (Executar a função a ser testada)
    df_resultado = transformer.add_valor_total_pedidos(df_entrada)

    # 3. Assert (Verificar se o resultado é o esperado)
    # Coletamos os dados dos DataFrames para comparar como listas de dicionários
    resultado_coletado = sorted([row.asDict() for row in df_resultado.collect()], key=lambda x: x['produto'])
    esperado_coletado = sorted([row.asDict() for row in df_esperado.collect()], key=lambda x: x['produto'])

    assert df_resultado.count() == df_esperado.count(), "O número de linhas não corresponde ao esperado."
    assert df_resultado.columns == df_esperado.columns, "As colunas não correspondem ao esperado."
    assert resultado_coletado == esperado_coletado, "O conteúdo dos DataFrames não é igual."

```

**O que este código faz?**
- **`@pytest.fixture`**: Cria uma `SparkSession` que pode ser reutilizada por vários testes. É mais eficiente do que criar uma nova sessão para cada teste.
- **`test_add_valor_total_pedidos`**:
    - **Arrange**: Criamos um pequeno DataFrame de entrada (`df_entrada`) com dados de teste e o DataFrame exato que esperamos como saída (`df_esperado`).
    - **Act**: Chamamos o método `add_valor_total_pedidos` com nosso DataFrame de teste.
    - **Assert**: Comparamos o resultado. Como a ordem das linhas em um DataFrame não é garantida, a forma mais segura de comparar é coletar os resultados (`.collect()`), ordená-los e então comparar as listas de objetos Python. Também verificamos se o número de linhas e as colunas são idênticos.

**4. Adicionando um Segundo Teste:**

Para solidificar o conceito, vamos adicionar um teste para o método `get_top_10_clientes`. A estratégia será criar um cenário com mais de 10 clientes para garantir que a lógica de agregação, soma e limite funcione corretamente.

Adicione o seguinte teste ao final do arquivo `tests/test_transformations.py`:

```python
def test_get_top_10_clientes(spark_session):
    """
    Testa a função get_top_10_clientes para garantir que ela agrupa,
    soma os valores totais por cliente e retorna apenas os 10 maiores,
    ordenados corretamente.
    """
    # 1. Arrange
    transformer = Transformation()

    schema_entrada = StructType([
        StructField("id_cliente", LongType(), True),
        StructField("valor_total", FloatType(), True),
    ])
    # Criando 12 clientes para garantir que o limit(10) funcione
    dados_entrada = [
        (1, 100.0), (2, 200.0), (1, 50.0),   # Cliente 1: total 150.0
        (3, 300.0), (4, 400.0), (5, 500.0),
        (6, 600.0), (7, 700.0), (8, 800.0),
        (9, 900.0), (10, 1000.0), (11, 1100.0),
        (12, 50.0)
    ]
    df_entrada = spark_session.createDataFrame(dados_entrada, schema_entrada)

    # O resultado esperado deve conter os 10 clientes com maiores valores,
    # ordenados de forma decrescente.
    schema_esperado = StructType([
        StructField("id_cliente", LongType(), True),
        StructField("valor_total", FloatType(), True)
    ])
    dados_esperados = [
        (11, 1100.0),
        (10, 1000.0),
        (9, 900.0),
        (8, 800.0),
        (7, 700.0),
        (6, 600.0),
        (5, 500.0),
        (4, 400.0),
        (3, 300.0),
        (2, 200.0) # Cliente 1 (total 150.0) e 12 (total 50.0) devem ficar de fora
    ]
    df_esperado = spark_session.createDataFrame(dados_esperados, schema_esperado)

    # 2. Act
    df_resultado = transformer.get_top_10_clientes(df_entrada)

    # 3. Assert
    # A ordem é importante neste teste, então coletamos os dados como estão
    resultado_coletado = [row.asDict() for row in df_resultado.collect()]
    esperado_coletado = [row.asDict() for row in df_esperado.collect()]

    assert df_resultado.count() == 10, "O DataFrame resultante deve ter exatamente 10 linhas."
    assert df_resultado.columns == df_esperado.columns, "As colunas não correspondem ao esperado."
    assert resultado_coletado == esperado_coletado, "Os dados dos 10 maiores clientes não correspondem ao esperado."

```

**5. Executando os Testes:**

Para rodar todos os testes do seu projeto, basta executar o comando `pytest` na raiz do seu diretório:

```bash
pytest
```

Agora, o `pytest` encontrará e executará os dois testes. A saída deve ser:

```
============================= test session starts ==============================
...
collected 2 items

tests/test_transformations.py ..                                         [100%]

============================== 2 passed in ...s ===============================
```

Com dois testes, sua rede de segurança está ainda mais forte. Você pode seguir este padrão para testar todas as funções críticas da sua lógica de negócio.

---

## Parabéns! 
Você completou a jornada de transformar um simples script em uma aplicação Python robusta, de alta qualidade e distribuível.


## Desafio

Agora é a sua vez! Neste desafio você deve criar um projeto que resolva a seguinte questão:

A alta gestão da empresa deseja um relatório de pedidos de venda cujo pagamentos recusados (status=false) e que na avaliação de fraude foram classificados como legítimos (fraude=false).<br>
O relatório deve ter os seguintes atributos:
  1. Estado (UF) onde o pedido foi feito
  2. Forma de pagamento
  3. Valor total do pedido
  4. Data do pedido

O relatório deve compreender pedidos apenas do ano de 2025.

### Critérios de avaliação
Seu projeto deve contemplar os seguintes requisitos:

1. **Schemas explícitos**
  - TODOS os dataframes devem ter seus schemas explicitamente definidos (sem inferência)
2. **Orientação a objetos**
  - TODOS os componentes do projeto devem ser encapsulados em CLASSES.
3. **Injeção de Dependências**
  - UTILIZAR o `main.py` como Aggregation Root
  - INSTANCIAR todas as dependências no fluxo principal em `main.py`
  - INJETAR as dependências via aggregation root
  - As seguintes classes serão avaliadas como dependência: 
    * Classes de configuração
    * Classes de gerenciamento de sessão spark
    * Classes de leitura e escrita de dados
    * Classes de lógica de negócios
    * Classes de orquestração do pipeline
4. **Configurações centralizadas**
  - DEFINIR um pacote de configurações 
  - DEFINIR pelo menos UMA classe de configuração 
  - UTILIZAR a configuração no fluxo principal
5. **Sessão Spark**
  - DEFINIR um pacote de gerenciamento da sessão spark
  - CRIAR uma classe de gerenciamento de sessão spark
  - UTILIZAR a sessão spark no fluxo principal
6. **Leitura e Escrita de Dados (I/O)**
  - DEFINIR pelo menos um pacote de leitura e escrita de dados
  - CRIAR pelo menos uma classe de leitura e escrita de dados
  - UTILIZAR os pacotes de leitura e escrita no fluxo principal
7. **Lógica de Negócio**
  - DEFINIR um pacote de lógica de negócios
  - CRIAR pelo menos uma classe de lógica de negócios
  - UTILIZAR o pacote de lógica de negócios no fluxo principal
8. **Orquestração do pipeline**
  - DEFINIR um pacote de orquestração do pipeline
  - CRIAR pelo menos uma classe de orquestração do pipeline
  - UTILIZAR o pacote de orquestração no fluxo principal
9. **Logging**
  - IMPORTAR o pacote `logging` na classe de lógica de negócios.
  - CONFIGURAR o logging
    * Exemplo: `logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')`
  - UTILIZAR o logging para registro das etapas do pipeline.
10. **Tratamento de Erros**
  - UTILIZAR a estrutura `try/catch` para tratamento de erros na classe de lógica de negócios.
  - UTILIZAR logging para registro do erro capturado.
11. **Empacotamento da aplicação**
  - CRIAR o arquivo `pyproject.toml`
  - CRIAR o arquivo `requirements.txt`
  - CRIAR o arquivo `README.md`
  - CRIAR o arquivo `MANIFEST.in`
12. **Testes unitários**
  - CRIAR pelo menos um teste unitário para a classe de lógica de negócios.
  - O teste deve ser executado com sucesso.
  - Utilizar o pacote `pytest`.

--

### Material de apoio
	Todo o material de apoio, instruções e conteúdo pedagógico pode ser encontrado no repositório https://github.com/infobarbosa/pyspark-poo .

--

### Datasets
#### Dataset de Pagamentos

O dataset de pagamentos está disponível no seguinte repositório:
```
https://github.com/infobarbosa/dataset-json-pagamentos
```
Utilize os arquivos no caminho `dataset-json-pagamentos/data/pagamentos`.<br>
As especificações do dataset (formato, estrutura de atributos, etc) estão disponíveis no próprio repositório.

#### Dataset de pedidos
O dataset de pedidos está disponível no seguinte repositório:
```
https://github.com/infobarbosa/datasets-csv-pedidos
```
Utilize os arquivos no caminho `datasets-csv-pedidos/data/pedidos/`.<br>
As especificações do dataset (formato, estrutura de atributos, etc) estão disponíveis no próprio repositório.

---


