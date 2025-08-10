# Engenharia de Software com PySpark: De Script a uma Aplicação Robusta
- Author: Prof. Barbosa  
- Contact: infobarbosa@gmail.com  
- Github: [infobarbosa](https://github.com/infobarbosa)

Este repositório é um guia passo a passo para refatorar um script PySpark monolítico, aplicando conceitos de Programação Orientada a Objetos (POO), organização de código e testes para criar uma aplicação mais robusta, manutenível e testável.

## Sumário
1. [Configuração Inicial](#configuração-inicial)
2. [O Ponto de Partida: Script com Inferência de Schema](#o-ponto-de-partida-script-com-inferência-de-schema)
3. [Passo 0: A Importância de Definir Schemas Explícitos](#passo-0-a-importância-de-definir-schemas-explícitos)
4. [Rumo à Engenharia de Software: O Plano de Batalha](#rumo-à-engenharia-de-software-o-plano-de-batalha)
5. [Passo 1: Centralizando as Configurações](#passo-1-centralizando-as-configurações)
6. [Passo 2: Gerenciando a Sessão Spark](#passo-2-gerenciando-a-sessão-spark)
7. [Passo 3: Unificando a Leitura e Escrita de Dados (I/O)](#passo-3-unificando-a-leitura-e-escrita-de-dados-io)
8. [Passo 4: Isolando a Lógica de Negócio](#passo-4-isolando-a-lógica-de-negócio)
9. [Passo 5: Orquestrando a Aplicação no `main.py`](#passo-5-orquestrando-a-aplicação-no-mainpy)
10. [Passo 6: Aplicando Injeção de Dependências com uma Classe `Pipeline`](#passo-6-aplicando-injeção-de-dependências-com-uma-classe-pipeline)

---

### Configuração Inicial

Antes de começar, prepare seu ambiente:

1.  **Clone o repositório:**
    ```bash
    git clone git@github.com:infobarbosa/pyspark-poo.git
    cd pyspark-poo
    ```

2.  **Crie um ambiente virtual e instale as dependências:**
    ```bash
    python -m venv .venv
    source .venv/bin/activate
    pip install pyspark
    ```

3.  **Baixe os datasets:**
    Execute o script para baixar os dados necessários para a pasta `data/`.
    ```bash
    ./download-datasets.sh
    ```

### O Ponto de Partida: Script com Inferência de Schema

Vamos começar com um script monolítico. Copie o código abaixo e cole no seu arquivo `src/main.py`.

Note que, ao ler os arquivos (`.json` e `.csv`), **não estamos definindo um schema**. Estamos deixando o Spark "adivinhar" a estrutura e os tipos de dados.

```python
# src/main.py (Versão 1: com inferência de schema)
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("Analise de Pedidos").getOrCreate()

# Abrir o dataframe de clientes, deixando o Spark inferir o schema
clientes = spark.read.option("compression", "gzip").json("data/clientes.gz")

clientes.printSchema()
clientes.show(5, truncate=False)

# Abrir o dataframe de pedidos, deixando o Spark inferir o schema
# Para CSV, a inferência exige uma passagem extra sobre os dados (inferSchema=True)
pedidos = spark.read.option("compression", "gzip") \
                    .option("header", "true") \
                    .option("inferSchema", "true") \
                    .option("sep", ";") \
                    .csv("data/pedidos.gz")

pedidos.printSchema()
pedidos = pedidos.withColumn("valor_total", F.col("valor_unitario") * F.col("quantidade"))
pedidos.show(5, truncate=False)

# O resto da lógica de negócio...
calculado = pedidos.groupBy("id_cliente") \
    .agg(F.sum("valor_total").alias("valor_total")) \
    .orderBy(F.desc("valor_total")) \
    .limit(10)

pedidos_clientes = calculado.join(clientes, clientes.id == calculado.id_cliente, "inner") \
    .select(calculado.id_cliente, clientes.nome, clientes.email, calculado.valor_total)

pedidos_clientes.show(20, truncate=False)

spark.stop()
```

Este script funciona, mas depender da inferência de schema é uma má prática em produção. Vamos entender o porquê.

---

### Passo 0: A Importância de Definir Schemas Explícitos

Deixar o Spark adivinhar o schema (`inferSchema`) é conveniente para exploração de dados, mas traz três grandes problemas para pipelines de dados sérios:

1.  **Desempenho:** Para inferir o schema, o Spark precisa ler os dados uma vez apenas para analisar a estrutura e os tipos. Depois, ele lê os dados uma segunda vez para de fato carregá-los. Isso pode dobrar o tempo de leitura, um custo enorme para datasets grandes.
2.  **Precisão:** O Spark pode interpretar um tipo de dado de forma errada. Uma coluna de CEP (`"01234-567"`) pode ser lida como `integer` (e virar `1234567`), ou uma data em formato específico pode virar `string`. Isso causa erros silenciosos que corrompem a análise.
3.  **Imprevisibilidade:** Se uma nova partição de dados chega com um tipo diferente (ex: um `id` que era `long` de repente contém um `string`), a inferência pode quebrar o pipeline ou, pior, mudar o tipo da coluna para `string`, escondendo o problema de qualidade dos dados.

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

# Schema do dataframe de clientes
schema_clientes = StructType([
    StructField("id", LongType(), True),
    StructField("nome", StringType(), True),
    StructField("data_nasc", DateType(), True),
    StructField("cpf", StringType(), True),
    StructField("email", StringType(), True),
    StructField("interesses", ArrayType(StringType()), True)
])
# Abrir o dataframe de clientes com schema explícito
clientes = spark.read.option("compression", "gzip").json("data/clientes.gz", schema=schema_clientes)

clientes.show(5, truncate=False)

# Schema do dataframe de pedidos
schema_pedidos = StructType([
    StructField("id_pedido", StringType(), True),
    StructField("produto", StringType(), True),
    StructField("valor_unitario", FloatType(), True),
    StructField("quantidade", LongType(), True),
    StructField("data_criacao", TimestampType(), True),
    StructField("uf", StringType(), True),
    StructField("id_cliente", LongType(), True)
])

# Abrir o dataframe de pedidos com schema explícito
pedidos = spark.read.option("compression", "gzip").csv("data/pedidos.gz", header=True, schema=schema_pedidos, sep=";")
pedidos = pedidos.withColumn("valor_total", F.col("valor_unitario") * F.col("quantidade"))
pedidos.show(5, truncate=False)

# Calcular o valor total de pedidos por cliente e filtrar os 10 maiores
calculado = pedidos.groupBy("id_cliente") \
    .agg(F.sum("valor_total").alias("valor_total")) \
    .orderBy(F.desc("valor_total")) \
    .limit(10)

calculado.show(10, truncate=False)

# Fazer a junção dos dataframes
pedidos_clientes = calculado.join(clientes, clientes.id == calculado.id_cliente, "inner") \
    .select(calculado.id_cliente, clientes.nome, clientes.email, calculado.valor_total)

pedidos_clientes.show(20, truncate=False)

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
    ├── io/
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

**3. Adicione o seguinte código ao `src/config/settings.py`:**

```python
# src/config/settings.py

# Caminhos para os dados de entrada (fontes)
CLIENTES_PATH = "data/clientes.gz"
PEDIDOS_PATH = "data/pedidos.gz"

# Caminho para os dados de saída (destino)
OUTPUT_PATH = "data/output/pedidos_por_cliente"
```

---

### Passo 2: Gerenciando a Sessão Spark

A criação da `SparkSession` também pode ser isolada para ser mais reutilizável e fácil de configurar.

**1. Crie o diretório e o arquivo de inicialização:**

```bash
mkdir -p src/session
touch src/session/__init__.py
```

**2. Crie o arquivo `src/session/spark_session.py`:**

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

---

### Passo 3: Unificando a Leitura e Escrita de Dados (I/O)

Vamos criar uma classe que lida com todas as operações de entrada (leitura) e saída (escrita) de dados.

**1. Crie o diretório e o arquivo de inicialização:**

```bash
mkdir -p src/io
touch src/io/__init__.py
```

**2. Crie o arquivo `src/io/data_handler.py`:**

**3. Adicione o seguinte código a ele:**

Esta classe irá conter a lógica para ler os arquivos de clientes e pedidos, e também um novo método para escrever nosso resultado final em formato Parquet.

```python
# src/io/data_handler.py
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

---

### Passo 4: Isolando a Lógica de Negócio

Esta etapa é semelhante à anterior, mas vamos garantir que o arquivo esteja no lugar certo.

**1. Crie o diretório e o arquivo de inicialização:**

```bash
mkdir -p src/processing
touch src/processing/__init__.py
```

**2. Crie o arquivo `src/processing/transformations.py`:**

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

---

### Passo 5: Orquestrando a Aplicação no `main.py`

Agora, vamos juntar todas as peças. O `main.py` se tornará um orquestrador limpo e legível, que apenas chama os métodos das nossas classes.

**1. Substitua todo o conteúdo do `src/main.py` pelo código abaixo:**

```python
# src/main.py
from session.spark_session import SparkSessionManager
from io.data_handler import DataHandler
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

#### O que ganhamos com esta nova estrutura?

-   **Organização Superior:** Cada parte da aplicação tem seu lugar. Se precisar alterar algo sobre a sessão Spark, você sabe que deve ir em `src/session`. Se a forma de ler um arquivo mudar, o lugar é `src/io`.
-   **Configuração Centralizada:** Mudar os caminhos dos arquivos de entrada ou saída agora é trivial e seguro, sem risco de quebrar a lógica da aplicação.
-   **Máxima Reutilização:** Cada componente (`DataHandler`, `Transformation`, `SparkSessionManager`) pode ser facilmente importado e reutilizado em outros projetos ou notebooks.
-   **Testabilidade Aprimorada:** A lógica de negócio em `Transformation` continua pura e fácil de testar. Agora, também podemos testar o `DataHandler` de forma isolada, se necessário.

---

### Passo 6: Aplicando Injeção de Dependências com uma Classe `Pipeline`

Até agora, nossa função `main` está fazendo duas coisas: criando os objetos (`DataHandler`, `Transformation`) e orquestrando as chamadas dos métodos. Vamos dar um passo adiante na organização do código usando um padrão chamado **Injeção de Dependências (DI)**.

A ideia é simples: em vez de uma classe ou função criar os objetos de que precisa (suas "dependências"), ela os recebe de fora, geralmente em seu construtor. Isso desacopla o código e, mais importante, torna-o muito mais fácil de testar.

Vamos criar uma classe `Pipeline` que conterá toda a lógica de orquestração. O `main.py` se tornará a **"Raiz de Composição"** (`Composition Root`), o único lugar responsável por montar e "ligar" os componentes da nossa aplicação.

**1. Crie o arquivo `src/pipeline.py`:**

Este arquivo irá abrigar nossa nova classe orquestradora.

```bash
touch src/pipeline.py
```

**2. Adicione o seguinte código ao `src/pipeline.py`:**

A classe `Pipeline` receberá a sessão Spark como uma dependência em seu construtor. Ela então usará essa sessão para inicializar seus próprios componentes, como o `DataHandler`.

```python
# src/pipeline.py
from pyspark.sql import SparkSession
from io.data_handler import DataHandler
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
from pipeline import Pipeline

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

Para que os imports como `from pipeline import Pipeline` funcionem corretamente, o Python precisa tratar o diretório `src` como um "pacote". Para isso, crie um arquivo `__init__.py` vazio dentro dele.

```bash
touch src/__init__.py
```

#### O Grande Ganho: Testabilidade

Por que fizemos tudo isso? **Para facilitar os testes.**

Imagine que você queira testar a classe `Pipeline` sem ler arquivos reais do disco. Com a injeção de dependências, você poderia criar um `DataHandler` "falso" (um *mock*) que retorna DataFrames de teste pré-definidos e injetá-lo no `Pipeline`. O `Pipeline` executaria sua lógica sem saber que está usando dados falsos, permitindo que você verifique o resultado de forma rápida e isolada.

Este design nos prepara para o próximo nível de maturidade de software: **testes automatizados**.




