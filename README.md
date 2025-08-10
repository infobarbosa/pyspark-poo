# PySpark com POO: Um Guia de Refatoração

Este repositório é um guia passo a passo para refatorar um script PySpark monolítico, aplicando conceitos de Programação Orientada a Objetos (POO), organização de código e testes para criar uma aplicação mais robusta, manutenível e testável.

## Sumário
1. [Configuração Inicial](#configuração-inicial)
2. [O Ponto de Partida: Script Monolítico](#o-ponto-de-partida-script-monolítico)
3. [Rumo à Orientação a Objetos: O Plano](#rumo-à-orientação-a-objetos-o-plano)
4. [Passo 1: Separando a Leitura de Dados](#passo-1-separando-a-leitura-de-dados)

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

### O Ponto de Partida: Script Monolítico

Atualmente, todo o nosso código está em um único arquivo: `src/main.py`.

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, LongType, ArrayType, DateType, FloatType

spark = SparkSession.builder.appName("Analise de Pedidos").getOrCreate()

# Schema do dataframe de clientes
schema_clientes = StructType(
    [
        StructField("id", LongType(), True),
        StructField("nome", StringType(), True),
        StructField("data_nasc", DateType(), True),
        StructField("cpf", StringType(), True),
        StructField("email", StringType(), True),
        StructField("interesses", ArrayType(StringType()), True)
    ]
)
# Abrir o dataframe de clientes
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

# Abrir o dataframe de pedidos
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

Este script funciona, mas mistura todas as responsabilidades:
-   Configuração do Spark.
-   Definição de schemas.
-   Leitura de dados.
-   Transformações e lógica de negócio.
-   Exibição de resultados.

Isso torna o código difícil de reutilizar, testar e dar manutenção.

### Rumo à Orientação a Objetos: O Plano

Nosso objetivo é refatorar este script, separando as responsabilidades em diferentes classes e módulos. A estrutura que queremos alcançar é a seguinte:

```
.
├── src/
│   ├── __init__.py
│   ├── data_loader.py     # <-- Classe para carregar dados
│   ├── transformations.py # <-- Classes para lógica de negócio
│   └── main.py            # <-- Orquestrador principal
├── tests/
│   ├── __init__.py
│   ├── test_data_loader.py
│   └── test_transformations.py
└── README.md
```

### Passo 1: Separando a Leitura de Dados

A primeira responsabilidade que vamos isolar é a **leitura de dados**. Toda a lógica de schemas e `spark.read` ficará contida em uma classe dedicada.

**1. Crie o arquivo `src/data_loader.py`:**

Crie um novo arquivo chamado `data_loader.py` dentro da pasta `src/`.

Copie e cole o seguinte código nele. Esta classe encapsula a criação dos schemas e a lógica de leitura dos arquivos, recebendo a sessão Spark como uma dependência (Injeção de Dependência).

```python
# src/data_loader.py
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType, DateType, FloatType, TimestampType


class DataLoader:
    """
    Classe responsável por carregar os dados de clientes e pedidos.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def _get_schema_clientes(self) -> StructType:
        """Define e retorna o schema para o dataframe de clientes."""
        return StructType(
            [
                StructField("id", LongType(), True),
                StructField("nome", StringType(), True),
                StructField("data_nasc", DateType(), True),
                StructField("cpf", StringType(), True),
                StructField("email", StringType(), True),
                StructField("interesses", ArrayType(StringType()), True)
            ]
        )

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

```

**2. Refatore o `src/main.py`:**

Agora, vamos modificar o `main.py` para usar nossa nova classe `DataLoader`. Isso simplificará muito o script principal, tornando-o mais um "orquestrador" do que um executor de tarefas.

Substitua todo o conteúdo do `src/main.py` pelo código abaixo:

```python
# src/main.py
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Importe a classe que acabamos de criar
from data_loader import DataLoader

# 1. Inicialização
spark = SparkSession.builder.appName("Analise de Pedidos").getOrCreate()
data_loader = DataLoader(spark)

# 2. Carga de Dados (agora usando nossa classe)
clientes = data_loader.load_clientes("data/clientes.gz")
pedidos = data_loader.load_pedidos("data/pedidos.gz")

clientes.show(5, truncate=False)

# 3. Transformações (o código de negócio permanece aqui por enquanto)
pedidos_com_valor_total = pedidos.withColumn("valor_total", F.col("valor_unitario") * F.col("quantidade"))
pedidos_com_valor_total.show(5, truncate=False)

# Calcular o valor total de pedidos por cliente e filtrar os 10 maiores
top_10_clientes = pedidos_com_valor_total.groupBy("id_cliente") \
    .agg(F.sum("valor_total").alias("valor_total")) \
    .orderBy(F.desc("valor_total")) \
    .limit(10)

top_10_clientes.show(10, truncate=False)

# Fazer a junção dos dataframes
pedidos_clientes = top_10_clientes.join(clientes, clientes.id == top_10_clientes.id_cliente, "inner") \
    .select(top_10_clientes.id_cliente, clientes.nome, clientes.email, top_10_clientes.valor_total)

pedidos_clientes.show(20, truncate=False)

# 4. Finalização
spark.stop()
```

#### O que ganhamos com isso?

-   **Separação de Responsabilidades:** A lógica de leitura de dados está agora isolada em `DataLoader`. Se precisarmos mudar como um arquivo é lido (ex: mudar de JSON para Parquet), só precisamos alterar a classe `DataLoader`, sem tocar no `main.py`.
-   **Código mais Limpo:** O `main.py` está mais enxuto e focado na orquestração do fluxo de dados.
-   **Reutilização:** A classe `DataLoader` pode ser reutilizada em outras partes do projeto ou em outros projetos.

---

### Passo 2: Isolando a Lógica de Negócio (Transformações)

Agora que a leitura de dados está organizada, o próximo gargalo é a lógica de negócio, que ainda está misturada no `main.py`. Vamos isolar essas regras em uma nova classe.

**1. Crie o arquivo `src/transformations.py`:**

Crie o arquivo `src/transformations.py`. Ele abrigará uma classe `Transformation` com métodos que recebem um DataFrame, aplicam uma regra de negócio e retornam um novo DataFrame.

Copie e cole o seguinte código nele:

```python
# src/transformations.py
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

**2. Refatore o `src/main.py` mais uma vez:**

Vamos usar nossa nova classe `Transformation` no `main.py`. O script principal ficará extremamente limpo, apenas orquestrando as chamadas para as classes de carga e transformação.

Substitua o conteúdo do `src/main.py` por esta versão final:

```python
# src/main.py
from pyspark.sql import SparkSession

# Importe as classes que criamos
from data_loader import DataLoader
from transformations import Transformation


def main():
    """
    Função principal que orquestra a execução do pipeline de dados.
    """
    # 1. Inicialização
    spark = SparkSession.builder.appName("Analise de Pedidos").getOrCreate()
    data_loader = DataLoader(spark)
    transformer = Transformation()

    # 2. Carga de Dados
    clientes_df = data_loader.load_clientes("data/clientes.gz")
    pedidos_df = data_loader.load_pedidos("data/pedidos.gz")

    # 3. Transformações
    pedidos_com_valor_total_df = transformer.add_valor_total_pedidos(pedidos_df)
    top_10_clientes_df = transformer.get_top_10_clientes(pedidos_com_valor_total_df)
    resultado_final_df = transformer.join_pedidos_clientes(top_10_clientes_df, clientes_df)

    # 4. Exibição de Resultados
    print("Top 10 clientes com maior valor total de pedidos:")
    resultado_final_df.show(10, truncate=False)

    # 5. Finalização
    spark.stop()


if __name__ == "__main__":
    main()

```

#### O que ganhamos com isso?

-   **Alta Coesão e Baixo Acoplamento:** Cada classe tem uma responsabilidade única e bem definida. `DataLoader` carrega dados, `Transformation` aplica regras de negócio e `main` orquestra o fluxo.
-   **Testabilidade:** Agora é muito mais fácil testar nossa lógica de negócio. Podemos criar testes unitários para a classe `Transformation` passando DataFrames "mockados" (de mentira) e validar a saída, sem precisar de uma sessão Spark completa ou ler arquivos reais.
-   **Clareza:** O `main.py` se tornou um roteiro legível do que a aplicação faz, sem se perder em detalhes de implementação.

---

### Passo 3: Adicionando Testes Unitários

A maior vantagem da nossa nova estrutura é a capacidade de testar a lógica de negócio de forma isolada. Vamos criar testes para a classe `Transformation` para garantir que ela funcione como esperado.

**1. Crie a estrutura de testes:**

Primeiro, precisamos de um lugar para os nossos testes. Crie uma nova pasta chamada `tests` na raiz do projeto. Dentro dela, crie um arquivo vazio chamado `__init__.py` para que o Python a reconheça como um pacote.

```bash
mkdir tests
touch tests/__init__.py
```

**2. Instale o `pytest`:**

`pytest` é um framework de testes popular para Python.

```bash
pip install pytest
```

**3. Crie o arquivo de teste `tests/test_transformations.py`:**

Este arquivo conterá os testes para nossa classe `Transformation`. Note que criamos uma sessão Spark e DataFrames de exemplo diretamente no teste. Isso torna nossos testes independentes dos arquivos em `data/` e muito mais rápidos de executar.

Copie e cole o código abaixo no novo arquivo:

```python
# tests/test_transformations.py
import pytest
from pyspark.sql import SparkSession
from src.transformations import Transformation

@pytest.fixture(scope="session")
def spark():
    """Cria uma sessão Spark para os testes."""
    return SparkSession.builder \
        .appName("Testes de Transformacao") \
        .master("local[2]") \
        .getOrCreate()

@pytest.fixture
def transformation():
    """Fornece uma instância da classe Transformation para os testes."""
    return Transformation()

def test_add_valor_total_pedidos(spark, transformation):
    # Dados de entrada
    pedidos_data = [
        (1, 10.0, 2),
        (2, 5.0, 3)
    ]
    pedidos_df = spark.createDataFrame(pedidos_data, ["id_cliente", "valor_unitario", "quantidade"])

    # Executa a transformação
    resultado_df = transformation.add_valor_total_pedidos(pedidos_df)

    # Valida o resultado
    assert "valor_total" in resultado_df.columns
    resultado = resultado_df.collect()
    assert resultado[0]["valor_total"] == 20.0
    assert resultado[1]["valor_total"] == 15.0

def test_get_top_10_clientes(spark, transformation):
    # Dados de entrada
    pedidos_data = [
        (1, 100.0),
        (2, 200.0),
        (3, 50.0)
    ]
    pedidos_df = spark.createDataFrame(pedidos_data, ["id_cliente", "valor_total"])

    # Executa a transformação
    resultado_df = transformation.get_top_10_clientes(pedidos_df)

    # Valida o resultado
    assert resultado_df.count() == 3 # Menos de 10, então todos devem retornar
    resultado = resultado_df.orderBy("id_cliente").collect()
    assert resultado[0]["id_cliente"] == 1
    assert resultado[1]["id_cliente"] == 2
    assert resultado[2]["id_cliente"] == 3
    
    # Verifica a ordenação
    primeiro_cliente = resultado_df.first()
    assert primeiro_cliente["id_cliente"] == 2 # Cliente 2 tem o maior valor
    assert primeiro_cliente["valor_total"] == 200.0

```

**4. Execute os testes:**

Com o arquivo de teste criado, volte para o terminal na raiz do projeto e execute o `pytest`:

```bash
pytest
```

Você deverá ver uma saída indicando que os 2 testes passaram com sucesso.

### Conclusão

Parabéns! Você refatorou com sucesso um script PySpark monolítico para uma estrutura organizada, orientada a objetos e, o mais importante, testável.

**Principais aprendizados:**
-   **Separação de Responsabilidades:** Isolar a carga de dados, as transformações e a orquestração em classes distintas.
-   **Injeção de Dependência:** Passar dependências como a sessão Spark para as classes em vez de criá-las globalmente.
-   **Testabilidade:** Escrever testes unitários para a lógica de negócio, usando dados de exemplo para garantir que o código funcione corretamente e de forma independente.

Esta abordagem, embora pareça mais trabalhosa no início, economiza muito tempo e esforço a longo prazo, resultando em um código de maior qualidade, mais fácil de manter e de evoluir.




