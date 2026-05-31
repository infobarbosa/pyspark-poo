# Engenharia de Software com PySpark
- Author: Prof. Barbosa  
- Contact: infobarbosa@gmail.com  
- Github: [infobarbosa](https://github.com/infobarbosa)

Este repositório é um guia passo a passo para refatorar um script PySpark monolítico, aplicando conceitos de Programação Orientada a Objetos (POO), organização de código e testes para criar uma aplicação mais robusta, manutenível e testável.

## Sumário
- [Configuração Inicial](#configuração-inicial)
- [Script Inicial (Monolítico)](#script-inicial)
- [Passo 1: Schemas Explícitos](#passo-1-schemas-explícitos)
- [Planejamento da Refatoração](#planejamento)
- [Passo 2: Centralizando as Configurações](#passo-2-centralizando-as-configurações)
- [Passo 3: Gerenciando a Sessão Spark](#passo-3-gerenciando-a-sessão-spark)
- [Passo 4: Pacote de Leitura e Escrita de Dados (I/O)](#passo-4-pacote-de-leitura-e-escrita-de-dados-io)
- [Passo 5: Isolando a Lógica de Negócio](#passo-5-isolando-a-lógica-de-negócio)
- [Passo 6: Refatoração de main.py](#passo-6-refatoração-de-mainpy)
- [Passo 7: Injeção de Dependências](#passo-7-injeção-de-dependências)
- [Passo 8: Logging](#passo-8-logging)
- [Passo 9: Tratamento de Erros](#passo-9-tratamento-de-erros)
- [Passo 10: Gestão de Dependências](#passo-10-gestão-de-dependências)
- [Passo 11: Qualidade do Código com Linter e Formatador](#passo-11-qualidade-do-código-com-linter-e-formatador)
- [Passo 12: Empacotamento da Aplicação para Distribuição](#passo-12-empacotamento-da-aplicação-para-distribuição)
- [Passo 13: Testes Automatizados](#passo-13-testes-automatizados)
- [Desafio Final](#desafio)

---

## Configuração Inicial

Antes de começar, prepare seu ambiente:

ATENÇÃO! Se estiver utilizando Cloud9, utilize esse [tutorial](https://github.com/infobarbosa/data-engineering-cloud9).


1.  **Crie uma pasta para o projeto:**

```bash
mkdir -p data-engineering-pyspark/src
mkdir -p data-engineering-pyspark/data/input
mkdir -p data-engineering-pyspark/data/output

```

2.  **Crie um ambiente virtual e instale as dependências:**
```bash
python3 -m venv data-engineering-pyspark/.venv

```

```bash
source ./data-engineering-pyspark/.venv/bin/activate

```

```bash
pip install pyspark

```

3.  **Baixe os datasets:**
Faça o clone dos repositórios:

* Clientes
```sh
git clone https://github.com/infobarbosa/dataset-json-clientes ./data-engineering-pyspark/data/input/dataset-json-clientes

```

```sh
zcat ./data-engineering-pyspark/data/input/dataset-json-clientes/data/clientes.json.gz | head -5

```

Output esperado:
```
{"id": 1, "nome": "Isabel Abreu", "data_nasc": "1982-10-26", "cpf": "512.084.739-05", "email": "isabel.abreusigycp@outlook.com", "interesses": ["Filmes"], "carteira_investimentos": {"FIIs": 11533.69, "CDB": 26677.01}}
{"id": 2, "nome": "Natália Ramos", "data_nasc": "1971-04-26", "cpf": "780.369.125-03", "email": "natalia.ramosrzmyqb@hotmail.com", "interesses": ["Viagens"], "carteira_investimentos": {}}
{"id": 3, "nome": "Larissa Garcia", "data_nasc": "2006-12-03", "cpf": "608.275.134-53", "email": "larissa.garciaviennn@outlook.com", "interesses": ["Livros"], "carteira_investimentos": {}}
{"id": 4, "nome": "Milena Freitas", "data_nasc": "2007-09-07", "cpf": "674.158.392-00", "email": "milena.freitasrgsswy@gmail.com", "interesses": ["Astronomia", "Lazer", "Religião"], "carteira_investimentos": {}}
{"id": 5, "nome": "Caleb Gonçalves", "data_nasc": "1989-06-05", "cpf": "703.465.219-80", "email": "caleb.goncalveslkcgfn@gmail.com", "interesses": ["Astronomia", "Música"], "carteira_investimentos": {"CDB": 13423.81, "Criptomoedas": 45986.93}}

```

* Pedidos
```sh
git clone https://github.com/infobarbosa/datasets-csv-pedidos ./data-engineering-pyspark/data/input/datasets-csv-pedidos

```

```sh
zcat ./data-engineering-pyspark/data/input/datasets-csv-pedidos/data/pedidos/pedidos-2026-01.csv.gz | head -5

```

Output esperado:
```
ID_PEDIDO;PRODUTO;VALOR_UNITARIO;QUANTIDADE;DATA_CRIACAO;UF;ID_CLIENTE
f198e8f7-033d-414d-b032-20975e84edde;LIQUIDIFICADOR;300.0;1;2026-01-05T18:36:28;MG;8409
97969db5-9304-4b80-b19e-3a9d60ce6520;CELULAR;1000.0;3;2026-01-01T11:58:48;DF;934
f1db6c7e-0701-42fd-90b2-638b57cefe38;NOTEBOOK;1500.0;2;2026-01-17T15:28:57;MG;5872
3994d9fa-6609-4818-8efa-c3a570a6116a;GELADEIRA;2000.0;1;2026-01-27T13:37:31;MA;174
```

* Pagamentos
```sh
git clone https://github.com/infobarbosa/dataset-json-pagamentos ./data-engineering-pyspark/data/input/dataset-json-pagamentos

```

```sh
zcat ./data-engineering-pyspark/data/input/dataset-json-pagamentos/data/pagamentos/pagamentos-2026-01.json.gz | head -5

```

Output esperado:
```
{"id_pedido": "f198e8f7-033d-414d-b032-20975e84edde", "forma_pagamento": "PIX", "valor_pagamento": 285.0, "status": true, "data_processamento": "2026-01-06T02:29:21.830930", "avaliacao_fraude": {"fraude": false, "score": 0.12}}
{"id_pedido": "97969db5-9304-4b80-b19e-3a9d60ce6520", "forma_pagamento": "PIX", "valor_pagamento": 2850.0, "status": true, "data_processamento": "2026-01-01T22:26:07.151965", "avaliacao_fraude": {"fraude": false, "score": 0.11}}
{"id_pedido": "f1db6c7e-0701-42fd-90b2-638b57cefe38", "forma_pagamento": "PIX", "valor_pagamento": 2850.0, "status": true, "data_processamento": "2026-01-17T15:48:54.507491", "avaliacao_fraude": {"fraude": false, "score": 0.83}}
{"id_pedido": "3994d9fa-6609-4818-8efa-c3a570a6116a", "forma_pagamento": "CARTAO_CREDITO", "valor_pagamento": 2000.0, "status": true, "data_processamento": "2026-01-27T20:50:41.884628", "avaliacao_fraude": {"fraude": false, "score": 0.56}}
{"id_pedido": "04065285-5a0b-4631-af25-ea318f389b83", "forma_pagamento": "CARTAO_CREDITO", "valor_pagamento": 900.0, "status": true, "data_processamento": "2026-01-23T15:30:16.761626", "avaliacao_fraude": {"fraude": false, "score": 0.02}}
```

---

## Script inicial

Vamos começar com um script monolítico. 
```bash
touch ./data-engineering-pyspark/src/main.py

```

Adicione o conteúdo abaixo no arquivo `src/main.py`:

```python
# src/main.py
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

print("Abrindo a sessao spark")
spark = SparkSession.builder.appName("Analise de Pedidos").getOrCreate()

print("Abrindo o dataframe de clientes, deixando o Spark inferir o schema")
clientes = spark.read.option("compression", "gzip").json("./data-engineering-pyspark/data/input/dataset-json-clientes/data/clientes.json.gz")

clientes.printSchema()
clientes.show(5, truncate=False)

print("Abrindo o dataframe de pedidos, deixando o Spark inferir o schema")
pedidos = spark.read.option("compression", "gzip") \
                    .option("header", "true") \
                    .option("inferSchema", "true") \
                    .option("sep", ";") \
                    .csv("./data-engineering-pyspark/data/input/datasets-csv-pedidos/data/pedidos/")

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

pedidos_clientes.write.mode("overwrite").parquet("./data-engineering-pyspark/data/output/pedidos_por_cliente")

spark.stop()
```

Agora execute:
```bash
spark-submit ./data-engineering-pyspark/src/main.py

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

### Verificando o arquivo parquet
```sh
pip install parquet-tools

```

```sh
parquet-tools show ./data-engineering-pyspark/data/output/pedidos_por_cliente

```

```sh
ls ./data-engineering-pyspark/data/output/pedidos_por_cliente/

```

O comando abaixo inspeciona o arquivo e retorna seus metadados:
```sh
parquet-tools inspect ./data-engineering-pyspark/data/output/pedidos_por_cliente/*.parquet

```


---

## Passo 1: Schemas Explícitos

Este script funciona, mas depender da inferência de schema é uma má prática em produção. Vamos entender o porquê.<br>
Deixar o Spark "adivinhar" o schema (`inferSchema`) é conveniente para exploração de dados, mas traz três grandes problemas para pipelines de dados sérios:

1.  **Desempenho:** Para inferir o schema, o Spark precisa ler os dados uma vez apenas para analisar a estrutura e os tipos. Depois, ele lê os dados uma segunda vez para de fato carregá-los. Isso pode dobrar o tempo de leitura, um custo enorme para datasets grandes.
2.  **Precisão:** O Spark pode interpretar um tipo de dado de forma errada. Uma coluna de CEP (`"01234-567"`) pode ser lida como `integer` (e virar `1234567`), ou uma data em formato específico pode virar `string`. Isso causa erros silenciosos que corrompem a análise.
3.  **Imprevisibilidade:** Se uma nova partição de dados chega com um tipo diferente (ex: um `id` que era `long` de repente contém um `string`), a inferência pode quebrar o pipeline ou, pior, mudar o tipo da coluna para `string`, escondendo o problema de qualidade dos dados.

A solução é **sempre** definir o schema explicitamente.

### Exemplo 1:

Vamos simular um problema comum. Imagine que temos um arquivo CSV simples em `data/input/codigos.csv` com códigos de produtos. Note que alguns códigos possuem zeros à esquerda, que são importantes.

1. Baixe o arquivo `/tmp/data.csv`:
  ```bash
  wget -P /tmp https://raw.githubusercontent.com/infobarbosa/pyspark-poo/main/assets/data/data.csv

  ```

2. Baixe o script `infer-schema.py`:

  ```bash
  wget -P /tmp https://raw.githubusercontent.com/infobarbosa/pyspark-poo/main/assets/scripts/infer-schema.py

  ```

O script `infer-schema.py` tem o seguinte conteúdo:
```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql import functions as F

# Inicializa a SparkSession
spark = SparkSession.builder.appName("RiscoInferSchemaSalarios").getOrCreate()

# --- Cenário: Confusão de Bônus por Causa do inferSchema ---

# --- Abordagem 1: O Risco do inferSchema=True ---
print("--- 1. Lendo com inferSchema (Abordagem Perigosa) ---")

# O Spark vai "olhar" os dados e tentar adivinhar o tipo de cada coluna.
# Ele verá '0101' (string) e 101 (int) na mesma coluna e pode decidir
# converter tudo para inteiro, pois é o tipo mais "comum" ou que se encaixa.

df = spark.read.option("inferSchema", "true").csv("/tmp/data.csv", header=True)

print("Schema inferido pelo Spark:")
df.printSchema()
# Resultado esperado: 'cod_bonus' será inferido como 'long' ou 'integer',
# o que fará com que "0101" seja lido como o número 101.

print("\nDados como o Spark os leu (com 'cod_bonus' corrompido):")
df.show()

# Agora, vamos simular o pagamento de um bônus.
# O bônus é para o código 101 (Diretor), no valor de 50% do salário.
cod_bonus_diretor = 101
percentual_bonus = 0.5

# A lógica de negócio errada:
# O analista João Silva, cujo código era "0101", agora tem o código 101.
# Ele receberá indevidamente o bônus do diretor!
print(f"\nCalculando bônus de {percentual_bonus:.0%} para o código '{cod_bonus_diretor}'...")
df_bonus = df_inferido.withColumn(
    "valor_bonus",
    F.when(F.col("cod_bonus") == cod_bonus_diretor, F.col("salario") * percentual_bonus).otherwise(0)
)

print("\nResultado do cálculo de bônus (INCORRETO):")
df_bonus.show()
print("PROBLEMA: João Silva (Analista) recebeu o bônus que era para Carlos Oliveira (Diretor)!")

spark.stop()

```

3. Execute e veja o erro:
```bash
spark-submit /tmp/infer-schema.py

```

### Exemplo 2 (corrigido):

1. Baixe o script `schema-definido.py`:

  ```bash
  wget -P /tmp https://raw.githubusercontent.com/infobarbosa/pyspark-poo/main/assets/scripts/schema-definido.py

  ```

O script `schema-definido.py` tem o seguinte conteúdo:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql import functions as F

# Inicializa a SparkSession
spark = SparkSession.builder.appName("CalculoDeBonus").getOrCreate()

# O bônus é para o código 101 (Diretor), no valor de 50% do salário.
cod_bonus_diretor = 101
percentual_bonus = 0.5

# --- Abordagem 2: A Solução com Schema Definido Manualmente ---
print("\n--- 2. Lendo com Schema Definido (Abordagem Segura) ---")

# Definindo explicitamente que 'cod_bonus' é uma String.
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("nome", StringType(), True),
    StructField("cargo", StringType(), True),
    StructField("salario", DoubleType(), True),
    StructField("cod_bonus", StringType(), True) # A definição correta!
])

# Criando o DataFrame com o schema seguro
df = spark.read.option("header", "true").schema(schema).csv("/tmp/data.csv")

print("Schema definido manualmente:")
df.printSchema()

print("\nDados lidos corretamente (preservando o '0' em '0101'):")
df.show()

# Agora, o cálculo de bônus funcionará como esperado.
# O bônus será aplicado ao 'cod_bonus' numérico 101, mas como nossa
# coluna agora é String, precisamos fazer o cast.
print(f"\nCalculando bônus de {percentual_bonus:.0%} para o código '{cod_bonus_diretor}' (de forma segura)...")
df = df.withColumn(
    "valor_bonus",
    F.when(F.col("cod_bonus") == str(cod_bonus_diretor), F.col("salario") * percentual_bonus).otherwise(0)
)

print("\nResultado do cálculo de bônus (CORRETO):")
df.show()
print("SUCESSO: Apenas Carlos Oliveira (Diretor) recebeu o bônus, como esperado.")

# Finaliza a SparkSession
spark.stop()

```

2. Execute:
```bash
spark-submit /tmp/schema-definido.py

```

---

### Definindo os schemas do projeto

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

Substitua todo o conteúdo do `src/main.py` pela versão abaixo.

```python
# src/main.py
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
clientes = spark.read.option("compression", "gzip").json("./data-engineering-pyspark/data/input/dataset-json-clientes/data/clientes.json.gz", schema=schema_clientes)

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
pedidos = spark.read.option("compression", "gzip").csv("./data-engineering-pyspark/data/input/datasets-csv-pedidos/data/pedidos/", header=True, schema=schema_pedidos, sep=";")

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
pedidos_clientes.write.mode("overwrite").parquet("./data-engineering-pyspark/data/output/pedidos_por_cliente")

spark.stop()
```
Com nosso ponto de partida agora robusto e performático, podemos começar a refatoração para a Programação Orientada a Objetos.

---

## Planejamento

Nosso objetivo é evoluir de um simples script para uma aplicação PySpark bem estruturada. Para isso, vamos organizar nosso código em diretórios, onde cada um terá uma responsabilidade única. Esta é a estrutura que vamos construir:

```
.
└── src/
    ├── __init__.py
    ├── config/
    │   ├── __init__.py
    │   └── settings.py         # <-- Para centralizar configurações do projeto
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

## Passo 2: Centralizando as Configurações

É uma boa prática *NÃO* deixar "strings mágicas" (como caminhos de arquivos) espalhadas pelo código. Vamos centralizá-las em um único lugar.

### Pacote `config`
**1. Crie o diretório e o arquivo de inicialização:**

```bash
mkdir -p ./data-engineering-pyspark/src/config
touch ./data-engineering-pyspark/src/config/__init__.py

```

**2. Crie o arquivo `src/config/settings.py`:**

Este arquivo conterá os caminhos para nossos dados de entrada e para a pasta de saída onde salvaremos o resultado.
```bash
touch ./data-engineering-pyspark/src/config/settings.py

```

**3. Adicione o seguinte código ao `src/config/settings.py`:**

```python
# src/config/settings.py

# Caminhos para os dados de entrada (fontes)
CLIENTES_PATH = "./data-engineering-pyspark/data/input/dataset-json-clientes/data/clientes.json.gz"
PEDIDOS_PATH = "./data-engineering-pyspark/data/input/datasets-csv-pedidos/data/pedidos/"

# Caminho para os dados de saída (destino)
OUTPUT_PATH = "./data-engineering-pyspark/data/output/pedidos_por_cliente"
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

---

### Externalizando configurações
Manter a configuração em um arquivo .py é bom, mas misturar código (Python) com dados de configuração puros não é o ideal. 
Ambientes de produção modernos usam formatos como YAML ou JSON, que são agnósticos de linguagem e mais fáceis de serem gerenciados por ferramentas de automação (como Docker, Kubernetes, etc.).

**Solução**: Usar um arquivo YAML para nossas configurações.
1. Instale a dependência `pyyaml`:
```bash
pip install pyyaml

```

2. Crie um arquivo `config/settings.yaml`:
```sh
mkdir ./data-engineering-pyspark/config

```

```bash
touch ./data-engineering-pyspark/config/settings.yaml

```

3. Adicione o seguinte conteúdo ao arquivo `config/settings.yaml`:
  ```yaml
  # src/config/settings.yaml
  spark:
    app_name: "Analise de Pedidos"

  paths:
    clientes: "./data-engineering-pyspark/data/input/dataset-json-clientes/data/clientes.json.gz"
    pedidos: "./data-engineering-pyspark/data/input/datasets-csv-pedidos/data/pedidos/"
    output: "./data-engineering-pyspark/data/output/pedidos_por_cliente"

  file_options:
    pedidos_csv:
      compression: "gzip"
      header: True
      sep: ";"
      
  ```

4. Substitua todo o conteúdo do arquivo `src/config/settings.py`:

  ```python
  # src/config/settings.py
  import yaml

  def carregar_config(path: str = "./data-engineering-pyspark/config/settings.yaml") -> dict:
      """Carrega um arquivo de configuração YAML."""
      with open(path, 'r') as file:
          return yaml.safe_load(file)
      
  ```

5. Ajuste a importação em `main.py`:

  ```python
  from config.settings import carregar_config
  ``` 

6. Logo após o import defina a variável `config` em `main.py`:

  ```python
  config = carregar_config()
  ```

7. Defina agora a variável `app_name` em `main.py`:

  ```python
  app_name = config['spark']['app_name']
  print(f"Obtido o app name: {app_name}")

  ```

8. Utilize `app_name` para criar a sessão spark em `main.py`

  ```
  spark = SparkSession.builder.appName(app_name).getOrCreate()
  ```

9. Faça o ajuste do trecho a seguir:

  ```python
  path_clientes = config['paths']['clientes']
  print(f"Obtido o path de clientes: {path_clientes}")
  clientes = spark.read.option("compression", "gzip").json(path_clientes, schema=schema_clientes)
  ```

10. Faça o ajuste do trecho a seguir:

  ```python
  print("Abrindo o dataframe de pedidos")
  path_pedidos = config['paths']['pedidos']
  compression_pedidos = config['file_options']['pedidos_csv']['compression']
  header_pedidos = config['file_options']['pedidos_csv']['header']
  separator_pedidos = config['file_options']['pedidos_csv']['sep']

  print(f"""
  Obtidos os seguintes parâmetros de pedidos: 
  - path: {path_pedidos}
  - compression: {compression_pedidos}
  - header: {header_pedidos}
  - separator: {separator_pedidos}
  """)

  pedidos = spark.read.option("compression", compression_pedidos).csv(path_pedidos, header=True, schema=schema_pedidos, sep=separator_pedidos)
  ```

11. Faça o ajuste do trecho a seguir:

  ```python
  print("Escrevendo o resultado em parquet")
  path_output = config['paths']['output']
  print(f"Obtido o path de saída: {path_output}")
  pedidos_clientes.write.mode("overwrite").parquet(path_output)
  ```

---

## Passo 3: Gerenciando a Sessão Spark

A criação da `SparkSession` também pode ser isolada para ser mais reutilizável e fácil de configurar.

1. Crie o diretório e o arquivo de inicialização:

```bash
mkdir -p ./data-engineering-pyspark/src/session
touch ./data-engineering-pyspark/src/session/__init__.py

```

2. Crie o arquivo `src/session/spark_session.py`:
```bash
touch ./data-engineering-pyspark/src/session/spark_session.py

```

3. Adicione o seguinte código a ele:

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

4. Faça os ajustes em `src/main.py`
- Importando o pacote
  ```python
  from session.spark_session import SparkSessionManager
  ```

- Instanciando a sessão spark
  ```python
  spark = SparkSessionManager.get_spark_session(app_name=app_name)
  
  ```
---

## Passo 4: Pacote de Leitura e Escrita de Dados (I/O)

Vamos criar uma classe que lida com todas as operações de entrada (leitura) e saída (escrita) de dados.

1. Crie o diretório e o arquivo de inicialização:

```bash
mkdir -p ./data-engineering-pyspark/src/io_utils

```

```bash
touch ./data-engineering-pyspark/src/io_utils/__init__.py

```

2. Crie o arquivo `src/io_utils/data_handler.py`:
```bash
touch ./data-engineering-pyspark/src/io_utils/data_handler.py

```

3. Adicione o seguinte código a ele:

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

      def load_pedidos(self, path: str, compression: str, header:bool, sep:str) -> DataFrame:
          """Carrega o dataframe de pedidos a partir de um arquivo CSV."""
          schema = self._get_schema_pedidos()
          return self.spark.read.option("compression", compression).csv(path, header=header, schema=schema, sep=sep)

      def write_parquet(self, df: DataFrame, path: str):
          """
          Salva o DataFrame em formato Parquet, sobrescrevendo se já existir.

          :param df: DataFrame a ser salvo.
          :param path: Caminho de destino.
          """
          df.write.mode("overwrite").parquet(path)
          print(f"Dados salvos com sucesso em: {path}")

  ```

4. Faça os ajustes em `main.py`:

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
  print("Abrindo o dataframe de clientes")
  path_clientes = config['paths']['clientes']
  print(f"Obtido o path de clientes: {path_clientes}")
  clientes = dh.load_clientes(path = path_clientes)

  ```

  ```python
  print("Abrindo o dataframe de pedidos")
  path_pedidos = config['paths']['pedidos']
  compression_pedidos = config['file_options']['pedidos_csv']['compression']
  header_pedidos = config['file_options']['pedidos_csv']['header']
  separator_pedidos = config['file_options']['pedidos_csv']['sep']
  print(f"""
  Obtidos os seguintes parâmetros de pedidos: 
  - path: {path_pedidos}
  - compression_pedidos: {compression_pedidos}
  - header_pedidos: {header_pedidos}
  - separator_pedidos: {separator_pedidos}
  """)
  pedidos = dh.load_pedidos(path = path_pedidos, compression=compression_pedidos, header=header_pedidos, sep=separator_pedidos)

  ```

- Substituir a escrita de dados parquet pelo seguinte trecho:
  ```python
  print("Escrevendo o resultado em parquet")
  path_output = config['paths']['output']
  print(f"Obtido o path de saída: {path_output}")
  dh.write_parquet(df=pedidos_clientes, path=path_output)

  ```

---

## Passo 5: Isolando a Lógica de Negócio

Esta etapa é semelhante à anterior, mas vamos garantir que o arquivo esteja no lugar certo.

1. Crie o diretório e o arquivo de inicialização:

```sh
mkdir -p ./data-engineering-pyspark/src/processing

```

```sh
touch ./data-engineering-pyspark/src/processing/__init__.py

```

2. Crie o arquivo `src/processing/transformations.py`:
```bash
touch ./data-engineering-pyspark/src/processing/transformations.py

```

3. Adicione o seguinte código a ele:

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

4. Faça os seguintes ajustes em `main.py` :
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
    spark-submit ./data-engineering-pyspark/src/main.py

    ```
  
---

## Passo 6: Refatoração de `main.py`
Nesse momento nosso script `main.py` está bastante sujo. As linhas comentadas é tudo que mexemos até aqui mas que não precisamos mais.
```python
# src/main.py
from pyspark.sql import SparkSession
# from pyspark.sql import functions as F
# from pyspark.sql.types import (StructType, StructField, StringType, LongType, ArrayType, DateType, FloatType, TimestampType)
# from config.settings import CLIENTES_PATH, PEDIDOS_PATH, OUTPUT_PATH
from config.settings import carregar_config
from session.spark_session import SparkSessionManager
from io_utils.data_handler import DataHandler
from processing.transformations import Transformation

config = carregar_config()
app_name = config['spark']['app_name']
print(f"Obtido o app name: {app_name}")

# spark = SparkSession.builder.appName("Analise de Pedidos").getOrCreate()
# spark = SparkSession.builder.appName(app_name).getOrCreate()
spark = SparkSessionManager.get_spark_session(app_name=app_name)

dh = DataHandler(spark)
transformer = Transformation()

# print("Definindo schema do dataframe de clientes")
# schema_clientes = StructType([
#     StructField("id", LongType(), True),
#     StructField("nome", StringType(), True),
#     StructField("data_nasc", DateType(), True),
#     StructField("cpf", StringType(), True),
#     StructField("email", StringType(), True),
#     StructField("interesses", ArrayType(StringType()), True)
# ])
print("Abrindo o dataframe de clientes")
# clientes = spark.read.option("compression", "gzip").json("./data-engineering-pyspark/data/input/dataset-json-clientes/data/clientes.json.gz", schema=schema_clientes)
# clientes = spark.read.option("compression", "gzip").json(CLIENTES_PATH, schema=schema_clientes)
path_clientes = config['paths']['clientes']
print(f"Obtido o path de clientes: {path_clientes}")
# clientes = spark.read.option("compression", "gzip").json(path_clientes, schema=schema_clientes)
clientes = dh.load_clientes(path = path_clientes)

clientes.show(5, truncate=False)

# print("Definindo schema do dataframe de pedidos")
# schema_pedidos = StructType([
#     StructField("id_pedido", StringType(), True),
#     StructField("produto", StringType(), True),
#     StructField("valor_unitario", FloatType(), True),
#     StructField("quantidade", LongType(), True),
#     StructField("data_criacao", TimestampType(), True),
#     StructField("uf", StringType(), True),
#     StructField("id_cliente", LongType(), True)
# ])

print("Abrindo o dataframe de pedidos")
# pedidos = spark.read.option("compression", "gzip").csv("./data-engineering-pyspark/data/input/datasets-csv-pedidos/data/pedidos/", header=True, schema=schema_pedidos, sep=";")
# pedidos = spark.read.option("compression", "gzip").csv(PEDIDOS_PATH, header=True, schema=schema_pedidos, sep=";")

path_pedidos = config['paths']['pedidos']
compression_pedidos = config['file_options']['pedidos_csv']['compression']
header_pedidos = config['file_options']['pedidos_csv']['header']
separator_pedidos = config['file_options']['pedidos_csv']['sep']

print(f"""
Obtidos os seguintes parâmetros de pedidos: 
- path: {path_pedidos}
- compression: {compression_pedidos}
- header: {header_pedidos}
- separator: {separator_pedidos}
""")

# pedidos = spark.read.option("compression", compression_pedidos).csv(path_pedidos, header=True, schema=schema_pedidos, sep=separator_pedidos)
pedidos = dh.load_pedidos(path = path_pedidos, compression=compression_pedidos, header=header_pedidos, sep=separator_pedidos)

print("Adicionando a coluna valor_total")
# pedidos = pedidos.withColumn("valor_total", F.col("valor_unitario") * F.col("quantidade"))
pedidos = transformer.add_valor_total_pedidos(pedidos)
pedidos.show(5, truncate=False)

print("Calculando o valor total de pedidos por cliente e filtrar os 10 maiores")
# calculado = pedidos.groupBy("id_cliente") \
#     .agg(F.sum("valor_total").alias("valor_total")) \
#     .orderBy(F.desc("valor_total")) \
#     .limit(10)
calculado = transformer.get_top_10_clientes(pedidos)

calculado.show(10, truncate=False)

print("Fazendo a junção dos dataframes")
# pedidos_clientes = calculado.join(clientes, clientes.id == calculado.id_cliente, "inner") \
    # .select(calculado.id_cliente, clientes.nome, clientes.email, calculado.valor_total)
pedidos_clientes = transformer.join_pedidos_clientes(calculado, clientes)
pedidos_clientes.show(20, truncate=False)

print("Escrevendo o resultado em parquet")
# pedidos_clientes.write.mode("overwrite").parquet("./data-engineering-pyspark/data/output/pedidos_por_cliente")
# pedidos_clientes.write.mode("overwrite").parquet(OUTPUT_PATH)
path_output = config['paths']['output']
print(f"Obtido o path de saída: {path_output}")
#pedidos_clientes.write.mode("overwrite").parquet(path_output)
dh.write_parquet(df=pedidos_clientes, path=path_output)

spark.stop()
```

### Pontos de refatoração
Vamos promover algumas alterações pra que o nosso `main.py` fique mais limpo e organizado.
- Remoção de imports desnecessários
- Nomes de variáveis mais claras 
- Encapsulamento da lógica na função `main()`


1. Substitua todo o conteúdo do `src/main.py` pelo código abaixo:

```python
# src/main.py
from config.settings import carregar_config
from session.spark_session import SparkSessionManager
from io_utils.data_handler import DataHandler
from processing.transformations import Transformation

def main():
  
  config = carregar_config()
  app_name = config['spark']['app_name']
  print(f"Obtido o app name: {app_name}")

  spark = SparkSessionManager.get_spark_session(app_name=app_name)

  data_handler = DataHandler(spark)
  transformer = Transformation()

  print("Abrindo o dataframe de clientes")
  path_clientes = config['paths']['clientes']
  print(f"Obtido o path de clientes: {path_clientes}")
  clientes_df = data_handler.load_clientes(path = path_clientes)
  clientes_df.show(5, truncate=False)

  print("Abrindo o dataframe de pedidos")
  path_pedidos = config['paths']['pedidos']
  compression_pedidos = config['file_options']['pedidos_csv']['compression']
  header_pedidos = config['file_options']['pedidos_csv']['header']
  separator_pedidos = config['file_options']['pedidos_csv']['sep']

  print(f"""
  Obtidos os seguintes parâmetros de pedidos: 
  - path: {path_pedidos}
  - compression: {compression_pedidos}
  - header: {header_pedidos}
  - separator: {separator_pedidos}
  """)

  pedidos_df = data_handler.load_pedidos(path = path_pedidos, compression=compression_pedidos, header=header_pedidos, sep=separator_pedidos)

  print("Adicionando a coluna valor_total")
  pedidos_df = transformer.add_valor_total_pedidos(pedidos_df)
  pedidos_df.show(5, truncate=False)

  print("Calculando o valor total de pedidos por cliente e filtrar os 10 maiores")
  top_10_clientes_df = transformer.get_top_10_clientes(pedidos_df)

  top_10_clientes_df.show(10, truncate=False)

  print("Fazendo a junção dos dataframes")
  relatorio_top_10_cliente_df = transformer.join_pedidos_clientes(top_10_clientes_df, clientes_df)
  relatorio_top_10_cliente_df.show(20, truncate=False)

  print("Escrevendo o resultado em parquet")
  path_output = config['paths']['output']
  print(f"Obtido o path de saída: {path_output}")
  data_handler.write_parquet(df=relatorio_top_10_cliente_df, path=path_output)

  spark.stop()

if __name__ == "__main__":
  main()


```

2. Faça o teste:
```sh
spark-submit ./data-engineering-pyspark/src/main.py

```

3. Conferindo o arquivo parquet:
```sh
parquet-tools show ./data-engineering-pyspark/data/output/pedidos_por_cliente

```

```sh
parquet-tools show ./data-engineering-pyspark/data/output/pedidos_por_cliente/part*.parquet

```

```sh
ls -latr ./data-engineering-pyspark/data/output/pedidos_por_cliente/

```

---

## O que ganhamos com esta nova estrutura?

-   **Organização mais clara:** Cada parte da aplicação tem seu lugar. Se precisar alterar algo sobre a sessão Spark, você sabe que deve ir em `src/session`. Se a forma de ler um arquivo mudar, o lugar é `src/io_utils`.
-   **Configuração Centralizada:** Mudar os caminhos dos arquivos de entrada ou saída agora é trivial e seguro, sem risco de quebrar a lógica da aplicação.
-   **Reuso de Componentes:** Cada componente (`DataHandler`, `Transformation`, `SparkSessionManager`) pode ser facilmente importado e reutilizado em outros projetos ou notebooks.
-   **Testabilidade Aprimorada:** A lógica de negócio em `Transformation` continua pura e fácil de testar. Agora, também podemos testar o `DataHandler` de forma isolada, se necessário.

---

## Passo 7: Injeção de Dependências

Até agora, nossa função `main` está fazendo duas coisas: criando os objetos (`DataHandler`, `Transformation`) e orquestrando as chamadas dos métodos. Vamos dar um passo adiante na organização do código usando um padrão chamado **Injeção de Dependências (DI)**.

A ideia é simples: em vez de uma classe ou função criar os objetos de que precisa (suas "dependências"), ela os recebe de fora, geralmente em seu construtor. Isso desacopla o código e, mais importante, torna-o muito mais fácil de testar.

Vamos criar uma classe `Pipeline` que conterá toda a lógica de orquestração. O `main.py` se tornará a **"Raiz de Composição"** (`Composition Root`), o único lugar responsável por montar e "ligar" os componentes da nossa aplicação.

1. Crie o arquivo `src/pipeline/pipeline.py`:

Este arquivo irá abrigar nossa nova classe orquestradora.

  ```bash
  mkdir -p ./data-engineering-pyspark/src/pipeline

  ```

  ```bash
  touch ./data-engineering-pyspark/src/pipeline/__init__.py

  ```

  ```bash
  touch ./data-engineering-pyspark/src/pipeline/pipeline.py

  ```

2. Adicione o seguinte código ao `src/pipeline/pipeline.py`:

A classe `Pipeline` **não cria** as suas dependências: ela as **recebe prontas** no construtor. Em vez de instanciar `DataHandler` e `Transformation` internamente, o `Pipeline` apenas declara *que precisa* desses colaboradores e confia que alguém os fornecerá. Esse "alguém" será o `main.py` (a Raiz de Composição).

> **Por que injetar `DataHandler` e `Transformation`, e não a `SparkSession`?**
> Se o `Pipeline` recebesse apenas o `spark` e criasse `DataHandler(spark)` lá dentro, você **não conseguiria** substituir o `DataHandler` por um objeto falso (*mock*) durante os testes — ele estaria "soldado" ao código. Injetando o `DataHandler` já construído, no teste podemos passar um *mock* que retorna DataFrames fixos, sem tocar no disco. É exatamente isso que torna o `Pipeline` testável (veremos no [Passo 13](#passo-13-testes-automatizados)).

```python
# src/pipeline/pipeline.py
from io_utils.data_handler import DataHandler
from processing.transformations import Transformation

class Pipeline:
    """
    Encapsula a lógica de execução do pipeline de dados.
    """
    def __init__(self, data_handler: DataHandler, transformer: Transformation):
        self.data_handler = data_handler
        self.transformer = transformer

    def run(self, config):
        """
        Executa o pipeline completo: carga, transformação, e salvamento.
        """
        print("Pipeline iniciado...")        
        
        print("Abrindo o dataframe de clientes")
        path_clientes = config['paths']['clientes']
        print(f"Obtido o path de clientes: {path_clientes}")
        clientes_df = self.data_handler.load_clientes(path = path_clientes)
        clientes_df.show(5, truncate=False)
        
        print("Abrindo o dataframe de pedidos")
        path_pedidos = config['paths']['pedidos']
        compression_pedidos = config['file_options']['pedidos_csv']['compression']
        header_pedidos = config['file_options']['pedidos_csv']['header']
        separator_pedidos = config['file_options']['pedidos_csv']['sep']
        
        print(f"""
        Obtidos os seguintes parâmetros de pedidos: 
        - path: {path_pedidos}
        - compression: {compression_pedidos}
        - header: {header_pedidos}
        - separator: {separator_pedidos}
        """)
        
        pedidos_df = self.data_handler.load_pedidos(path = path_pedidos, compression=compression_pedidos, header=header_pedidos, sep=separator_pedidos)
        
        print("Adicionando a coluna valor_total")
        pedidos_df = self.transformer.add_valor_total_pedidos(pedidos_df)
        pedidos_df.show(5, truncate=False)
        
        print("Calculando o valor total de pedidos por cliente e filtrar os 10 maiores")
        top_10_clientes_df = self.transformer.get_top_10_clientes(pedidos_df)
        
        top_10_clientes_df.show(10, truncate=False)
        
        print("Fazendo a junção dos dataframes")
        relatorio_top_10_cliente_df = self.transformer.join_pedidos_clientes(top_10_clientes_df, clientes_df)
        relatorio_top_10_cliente_df.show(20, truncate=False)
        
        print("Escrevendo o resultado em parquet")
        path_output = config['paths']['output']
        print(f"Obtido o path de saída: {path_output}")
        self.data_handler.write_parquet(df=relatorio_top_10_cliente_df, path=path_output)

        print("Pipeline concluído com sucesso!")
      
```

3. Refatore o `src/main.py` para ser a Raiz de Composição:

Agora, o `main.py` fica muito mais limpo. Sua única responsabilidade é inicializar os objetos e iniciar o processo.

Substitua todo o conteúdo do `src/main.py` por este código:

```python
# src/main.py
from config.settings import carregar_config
from session.spark_session import SparkSessionManager
from io_utils.data_handler import DataHandler
from processing.transformations import Transformation
from pipeline.pipeline import Pipeline

def main():
  
  config = carregar_config()
  app_name = config['spark']['app_name']
  print(f"Obtido o app name: {app_name}")

  spark = SparkSessionManager.get_spark_session(app_name=app_name)

  # Raiz de Composição (Composition Root):
  # este é o ÚNICO lugar que monta as dependências concretas e as injeta.
  data_handler = DataHandler(spark)
  transformer = Transformation()
  pipeline = Pipeline(data_handler, transformer)
  pipeline.run(config=config)


  spark.stop()

if __name__ == "__main__":
  main()

```

4. Faça o teste:

```bash
spark-submit ./data-engineering-pyspark/src/main.py

```

### Ingestão de dependencias e a problemática da **testabilidade**

Por que fizemos tudo isso? **Para facilitar os testes.**

Imagine que você queira testar a classe `Pipeline` sem ler arquivos reais do disco. Com a injeção de dependências, você poderia criar um `DataHandler` "falso" (um *mock*) que retorna DataFrames de teste pré-definidos e injetá-lo no `Pipeline`. O `Pipeline` executaria sua lógica sem saber que está usando dados falsos, permitindo que você verifique o resultado de forma rápida e isolada.

---

## Passo 8: Logging

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

1. Importe o pacote `Logging` em `src/main.py`:

  ```python
  import logging
  ```

2. Crie uma função para configurar o logging:

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

3. Antes de chamar a função `main()`, chame `configurar_logging()`:

  ```python
  if __name__ == "__main__":
      configurar_logging()
      main()

  ```

4. Agora que o *Root Logger* está configurado, crie um `logger` local em `main()`:

Essa será a primeira linha do método `main()`.
```python
logger = logging.getLogger(__name__)

```

5. Em todas as classes, adicione a configuração do logger no início do arquivo e substitua todos os `print()` por chamadas ao `logging`.<br>

  Abaixo está um exemplo na classe `src/pipeline.py`:

  ```python
  # src/pipeline/pipeline.py
  import logging
  from io_utils.data_handler import DataHandler
  from processing.transformations import Transformation

  logger = logging.getLogger(__name__)

  class Pipeline:
      # ... (o construtor __init__ permanece o mesmo) ...

      def run(self, config):
          logger.info("Pipeline iniciado...")
          # ... (substitua os prints por logging.info) ...
          logger.info("Pipeline concluído com sucesso!")
  ```

---

## Passo 9: Tratamento de Erros
Ao trabalhar com processamento de dados em grande escala, é inevitável que nos deparemos com imprevistos, como dados ausentes ou malformados, falhas de conexão com fontes de dados ou erros de lógica em nossas transformações.
Ignorar essas possíveis falhas pode levar à interrupção de pipelines, resultados incorretos e perda de tempo valioso.

### Cenário 1: Integridade dos Dados (Read Modes)

O Spark, por padrão, é "permissivo". Se você definir que uma coluna é `Integer` mas chegar um texto "abc", o Spark converte silenciosamente para `null`. Em sistemas financeiros ou críticos, isso é inaceitável. Queremos que o processo falhe imediatamente (`FAILFAST`) se o dado estiver sujo.

Vamos alterar o `src/io_utils/data_handler.py`.

**1. Ajuste o método `load_pedidos`:**
Adicione a opção `.option("mode", "FAILFAST")`.

```python
    # src/io_utils/data_handler.py
    # ...
    def load_pedidos(self, path: str, compression: str, header:bool, sep:str) -> DataFrame:
        """Carrega o dataframe de pedidos com modo FAILFAST."""
        schema = self._get_schema_pedidos()
        return self.spark.read \
            .option("compression", compression) \
            .option("mode", "FAILFAST") \
            .csv(path, header=header, schema=schema, sep=sep)
    # ...

```

### Cenário 2: Arquivos Vazios ou Inexistentes

Às vezes o arquivo existe, mas está vazio (0 bytes ou apenas cabeçalho). Processar um dataframe vazio pode gerar erros em etapas seguintes ou relatórios em branco sem aviso prévio. Além disso, se o arquivo não existir, o Spark lança uma `AnalysisException`.

Vamos capturar esse erro e verificar se o dataframe tem dados.

**1. Inclua os imports necessários em `src/io_utils/data_handler.py`:**
Precisamos importar a exceção do Spark e o módulo de logging.

```python
# src/io_utils/data_handler.py
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.utils import AnalysisException # <-- Importante
# ... imports de types ...

logger = logging.getLogger(__name__) # <-- Inicializa o logger

```

**2. Ajuste o método `load_pedidos` com try/except e verificação de vazio:**

```python
    # src/io_utils/data_handler.py
    # ...
    def load_pedidos(self, path: str, compression: str, header:bool, sep:str) -> DataFrame:
        try:
            schema = self._get_schema_pedidos()
            df = self.spark.read \
                .option("compression", compression) \
                .option("mode", "FAILFAST") \
                .csv(path, header=header, schema=schema, sep=sep)
            
            # Verificação de Dataframe Vazio
            if df.isEmpty():
                logger.warning(f"ATENÇÃO: O arquivo em '{path}' foi lido mas não contém registros.")
            
            return df

        except AnalysisException as e:
            logger.error(f"Erro ao ler arquivo: {e}")
            raise e # Relança o erro para parar o pipeline

```

### Cenário 3: Erros da JVM (Java Virtual Machine)

Como o PySpark roda em cima da JVM (Java), alguns erros críticos (como falta de memória ou arquivo corrompido fisicamente) chegam como `Py4JJavaError`. Se não tratarmos isso, o log fica ilegível para quem só sabe Python.

**1. Inclua o import do Py4J em `src/io_utils/data_handler.py`:**

```python
# src/io_utils/data_handler.py
import logging
from py4j.protocol import Py4JJavaError # <-- Importante para erros da JVM
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.utils import AnalysisException
# ...

```

**2. Adicione o novo bloco `except` ao método `load_pedidos`:**

```python
    # src/io_utils/data_handler.py
    # ... dentro do load_pedidos ...
        except AnalysisException as e:
            logger.error(f"Erro de IO/Spark: {e}")
            raise e
        
        except Py4JJavaError as e:
            logger.critical(f"Erro Crítico na JVM (possível arquivo corrompido ou erro de memória): {e}")
            raise e

```

### Cenário 4: Blindando `main.py`

Agora que nosso `DataHandler` sabe reportar quando algo dá errado, precisamos garantir que o nosso `main.py` saiba lidar com isso.

**1. Atualize o `src/main.py` para capturar falhas no pipeline:**

```python
# src/main.py
# ... imports ...

def main():
    # ... carregamento de config ...
    
    spark = None # Inicializa como None para segurança no finally
    try:
        spark = SparkSessionManager.get_spark_session(app_name=app_name)
        data_handler = DataHandler(spark)
        transformer = Transformation()
        pipeline = Pipeline(data_handler, transformer)
        pipeline.run(config=config)

    except Exception as e:
        logging.error(f"FALHA CRÍTICA NO PIPELINE: {e}")
        # Aqui poderíamos adicionar envio de notificação (Slack, Email, PagerDuty)
        
    finally:
        if spark:
            spark.stop()
            logging.info("Sessão Spark finalizada.")

if __name__ == "__main__":
    configurar_logging()
    main()

```

### Testando os Erros

Para ver isso funcionando, vamos quebrar nossa aplicação de propósito.

1. **Teste de Arquivo Inexistente:**
Abra o arquivo `config/settings.yaml` e altere a chave `paths.pedidos` para apontar para um arquivo que não existe.
```yaml
pedidos : "./PATH-INVALIDO/data/input/datasets-csv-pedidos/data/pedidos"

```

*Observe o log de erro tratado.*


2. Para voltar a configuração original, faça o ajuste em `config/settings.yaml`:
```
pedidos: "./data-engineering-pyspark/data/input/datasets-csv-pedidos/data/pedidos/"

```

#### Conclusão

Com essas mudanças, se um arquivo não for encontrado, a aplicação não vai mais quebrar com um stack trace gigante. Em vez disso, ela registrará uma mensagem de erro clara e finalizará a sessão Spark de forma segura.

---

## Passo 10: Gestão de Dependências

Para garantir que nossa aplicação funcione da mesma forma em qualquer máquina, precisamos fixar as versões das bibliotecas que usamos.

1. Crie o arquivo `requirements.txt`:

Na raiz do seu projeto, crie um arquivo chamado `requirements.txt`.

  ```bash
  touch ./data-engineering-pyspark/requirements.txt

  ```

2. Adicione a dependência do PySpark:

  Abra o `requirements.txt` e adicione a versão exata do PySpark que você está usando. Você pode descobrir a versão com o comando `pip show pyspark`.

  ```
  # requirements.txt
  pyspark==4.1.1
  pyyaml==6.0.3

  ```
  
*(Nota: use a versão que estiver instalada no seu ambiente)*

3. Atualize as instruções de instalação:

  A partir de agora, a forma correta de instalar as dependências do projeto é:

  ```bash
  pip install -r ./data-engineering-pyspark/requirements.txt

  ```
  Isso garante que qualquer pessoa que execute seu projeto usará exatamente a mesma versão do PySpark.

---

## Passo 11: Qualidade do Código com Linter e Formatador

Para manter nosso código limpo, legível e livre de erros comuns, vamos usar duas ferramentas padrão da indústria: `ruff` (linter) e `black` (formatador).

1. Adicione as ferramentas ao `requirements.txt`:

  ```
  # requirements.txt
  pyspark==4.1.1
  pyyaml==6.0.3
  ruff==0.12.9
  black==25.1.0
  ```

*(Nota: você pode usar versões mais recentes se desejar)*

2. Instale as novas dependências:

  ```bash
  pip install -r ./data-engineering-pyspark/requirements.txt

  ```

3. Como usar as ferramentas:

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

## Passo 12: Empacotamento da Aplicação para Distribuição

O passo final da jornada de um engenheiro de software é tornar sua aplicação distribuível. Em vez de pedir para alguém clonar seu repositório e executar um script, vamos empacotar nosso pipeline em um formato que pode ser instalado com `pip` e executado com um simples comando no terminal.

**1. Crie o arquivo `pyproject.toml`:**

Este é o arquivo de configuração padrão para projetos Python modernos. Crie-o na raiz do seu projeto.

  ```bash
  touch ./data-engineering-pyspark/pyproject.toml

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
  version = "0.1.0"
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
      "pyspark==4.1.1",
      "pyyaml==6.0.3"
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

3. Crie um arquivo `MANIFEST.in`:

  - O arquivo:
  ```bash
  touch ./data-engineering-pyspark/MANIFEST.in

  ```

  - O conteúdo:
  ```
  include requirements.txt
  include README.md

  ```

4. Crie o arquivo `README.md`:

Este é o arquivo que será exibido quando alguém acessar o repositório.
  ```bash
  echo "[DATAENG] Meu projeto bem estruturado de dados com PySpark" > ./data-engineering-pyspark/README.md

  ```

5. Adicione o pacote `build` a `requirements.txt`:
  - Configurando o arquivo:

    ```
    # requirements.txt
    pyspark==4.1.1
    pyyaml==6.0.3
    ruff==0.12.9
    black==25.1.0
    build==1.3.0
    ```

  - Instalando:
    ```bash
    pip install -r ./data-engineering-pyspark/requirements.txt

    ```

6. Construa o pacote:

  ```bash
  python -m build

  ```

Você verá que um novo diretório `dist/` foi criado, contendo o arquivo `.whl` (Wheel).

7. Instale e execute sua aplicação:

Agora, para testar, você pode instalar sua própria aplicação como se fosse qualquer outra biblioteca.

  - Desinstalando a versão anterior se existir
    ```bash
    # Desinstale a versão de desenvolvimento se já existir
    pip uninstall dataeng_pyspark_data_pipeline -y

    ```

  - Instalando a versão distribuída
    ```bash
    # Instala o pacote que acabamos de criar
    pip install ./data-engineering-pyspark/dist/*.whl

    ```

    [OPCIONAL] - Caso tenha instado antes e precise forçar a reinstalação:
    ```
    pip install --force-reinstall ./data-engineering-pyspark/dist/dataeng_pyspark_data_pipeline-0.1.0-py3-none-any.whl

    ```

  - Executando a aplicação
    ```bash
    spark-submit --master "local[*]" \
      --py-files ./data-engineering-pyspark/dist/dataeng_pyspark_data_pipeline-0.1.0-py3-none-any.whl \
      ./data-engineering-pyspark/src/main.py

    ```

## Passo 13: Testes Automatizados

Até agora, construímos uma aplicação robusta, bem estruturada e distribuível. Mas como garantir que a lógica de negócio — o coração da aplicação — está correta e **continuará** correta conforme o projeto evolui? A resposta é: **testes automatizados**.

Uma boa suíte de testes nos dá:
- **Validação da Correção:** garante que cálculos e regras de negócio se comportam exatamente como o esperado.
- **Proteção contra Regressões:** se uma alteração futura quebrar algo, o teste falha e avisa imediatamente.
- **Confiança para Refatorar:** você melhora o código sabendo que não introduziu bugs.
- **Documentação Viva:** um bom teste descreve, em código executável, qual o comportamento esperado de cada componente.

### A Pirâmide de Testes

Nem todo teste é igual. Vamos organizar nossa suíte em duas camadas:

- **Testes Unitários** — verificam **uma unidade isolada** (um método, uma função), sem I/O externo. São muitos, rápidos e baratos. Ex.: a classe `Transformation`, que contém lógica pura.
- **Testes de Integração** — verificam se os componentes **cooperam corretamente** (a orquestração do `Pipeline`, a leitura/escrita real em disco). São menos numerosos e mais lentos.

> A base da pirâmide é larga (muitos testes unitários, rápidos) e o topo é estreito (poucos testes de integração, lentos). Essa proporção mantém a suíte ágil sem abrir mão da confiança de que "as peças se encaixam".

Diferente da versão anterior deste tutorial — que cobria apenas a `Transformation` — vamos testar **todas** as classes do projeto: `Transformation`, `DataHandler`, `SparkSessionManager`, `carregar_config` e o `Pipeline`.

### 1. Adicione as dependências de teste

`pytest` é o framework de testes mais popular do Python, e o `pytest-cov` mede a **cobertura** (quanto do código é exercitado pelos testes).

- Atualize o `requirements.txt`:
  ```
  # requirements.txt
  pyspark==4.1.1
  pyyaml==6.0.3
  ruff==0.12.9
  black==25.1.0
  build==1.3.0
  pytest==8.4.1       # Framework de testes
  pytest-cov==6.0.0   # Relatório de cobertura
  ```
  *(Você pode usar versões mais recentes se desejar.)*

- Instale:
  ```bash
  pip install -r ./data-engineering-pyspark/requirements.txt

  ```

### 2. Crie a estrutura de testes

Convenção: um diretório `tests/` na raiz do projeto, **separado** do `src/` e subdividido por tipo de teste. Os arquivos e funções de teste devem começar com `test_`.

  ```bash
  mkdir -p ./data-engineering-pyspark/tests/unit
  mkdir -p ./data-engineering-pyspark/tests/integration

  touch ./data-engineering-pyspark/tests/__init__.py
  touch ./data-engineering-pyspark/tests/unit/__init__.py
  touch ./data-engineering-pyspark/tests/integration/__init__.py

  ```

Ao final, a árvore ficará assim:

  ```
  data-engineering-pyspark/
  ├── pytest.ini
  ├── src/
  │   └── ...
  └── tests/
      ├── __init__.py
      ├── conftest.py              # fixtures compartilhadas (ex.: SparkSession)
      ├── unit/
      │   ├── __init__.py
      │   ├── test_transformations.py
      │   ├── test_data_handler.py
      │   ├── test_settings.py
      │   └── test_spark_session.py
      └── integration/
          ├── __init__.py
          └── test_pipeline.py
  ```

### 3. Configure o pytest (`pytest.ini`)

Sem configuração, o `import` das nossas classes (`from processing.transformations import ...`) falharia, porque o código fica em `src/`. O `pytest.ini` resolve isso e centraliza as opções da suíte.

- Crie o arquivo `./data-engineering-pyspark/pytest.ini`:
  ```ini
  [pytest]
  pythonpath = src
  testpaths = tests
  markers =
      unit: Testes unitários isolados (sem I/O externo)
      integration: Testes de integração (orquestração entre componentes)
  addopts = -v
  ```

O que cada opção faz:
- **`pythonpath = src`** — adiciona `src/` ao caminho de import. É por isso que escrevemos `from processing.transformations import Transformation` (e **não** `from src.processing...`).
- **`testpaths = tests`** — onde o pytest procura testes.
- **`markers`** — rótulos para categorizar testes (ex.: rodar só os unitários com `pytest -m unit`).
- **`addopts = -v`** — opções sempre aplicadas (aqui, saída detalhada).

### 4. Centralize a `SparkSession` no `conftest.py`

Criar uma `SparkSession` é **caro**. Não queremos pagar esse custo em cada teste. O pytest tem um arquivo especial, o `conftest.py`, cujas *fixtures* ficam disponíveis automaticamente para **todos** os testes — sem precisar importar.

- Crie o arquivo `./data-engineering-pyspark/tests/conftest.py`:
  ```python
  # tests/conftest.py
  import pytest
  from pyspark.sql import SparkSession


  @pytest.fixture(scope="session")
  def spark():
      """
      SparkSession compartilhada por toda a suíte de testes.

      scope="session" garante que a sessão seja criada uma única vez e
      reutilizada, evitando o overhead de inicialização do Spark em cada teste.
      """
      session = (
          SparkSession.builder
          .appName("test-pipeline-session")
          .master("local[2]")
          .config("spark.ui.enabled", "false")
          .config("spark.sql.shuffle.partitions", "2")
          .getOrCreate()
      )
      yield session
      session.stop()
  ```

Pontos-chave:
- **`scope="session"`** — uma única sessão para toda a execução (em vez de `scope="function"`, que a recriaria a cada teste).
- **`yield session`** — tudo antes do `yield` é a preparação; o que vem depois (`session.stop()`) é a limpeza, executada ao final.
- **`spark.ui.enabled=false`** e **`shuffle.partitions=2`** — desligam a UI e reduzem o número de partições para deixar os testes rápidos e silenciosos.
- Qualquer teste que declare um parâmetro chamado `spark` recebe essa sessão automaticamente.

### 5. A anatomia de um teste: Arrange, Act, Assert

Todo teste que escreveremos segue três passos:

1. **Arrange (Preparar):** monte os dados de entrada e o resultado esperado.
2. **Act (Agir):** execute a função/método sob teste.
3. **Assert (Verificar):** compare o resultado obtido com o esperado.

Com a fundação pronta, vamos escrever os testes camada por camada.

### 6. Testes unitários da `Transformation` (a lógica de negócio)

Este é o arquivo **mais crítico**: as transformações contêm as regras de negócio. Um erro aqui corromperia silenciosamente todos os resultados. Como é lógica pura, criamos os DataFrames *inline* (sem I/O) — máxima velocidade e isolamento.

Repare em dois pontos importantes em relação à versão anterior do tutorial:
- Agrupamos os testes em **classes** (`TestAddValorTotalPedidos`, ...) para organizar por método testado.
- Cada teste cobre **um comportamento específico**, incluindo **casos de borda** (nulos, zero, menos de 10 clientes), e a docstring explica *por que* aquele caso importa.

- Crie o arquivo `./data-engineering-pyspark/tests/unit/test_transformations.py`:
  ```python
  # tests/unit/test_transformations.py
  import pytest
  from pyspark.sql.types import (
      ArrayType, DateType, FloatType, LongType, StringType,
      StructField, StructType, TimestampType,
  )

  from processing.transformations import Transformation


  # --- Schemas reutilizáveis ---

  SCHEMA_PEDIDOS = StructType([
      StructField("id_pedido", StringType(), True),
      StructField("produto", StringType(), True),
      StructField("valor_unitario", FloatType(), True),
      StructField("quantidade", LongType(), True),
      StructField("data_criacao", TimestampType(), True),
      StructField("uf", StringType(), True),
      StructField("id_cliente", LongType(), True),
  ])

  SCHEMA_PEDIDOS_COM_TOTAL = StructType([
      StructField("id_cliente", LongType(), True),
      StructField("valor_total", FloatType(), True),
  ])

  SCHEMA_CLIENTES = StructType([
      StructField("id", LongType(), True),
      StructField("nome", StringType(), True),
      StructField("data_nasc", DateType(), True),
      StructField("cpf", StringType(), True),
      StructField("email", StringType(), True),
      StructField("interesses", ArrayType(StringType()), True),
  ])


  class TestAddValorTotalPedidos:

      def test_calcula_valor_unitario_por_quantidade(self, spark):
          """valor_total deve ser valor_unitario × quantidade."""
          df = spark.createDataFrame(
              [("p1", "TV", 1500.0, 2, None, "SP", 1)], SCHEMA_PEDIDOS,
          )
          resultado = Transformation().add_valor_total_pedidos(df)
          assert resultado.collect()[0].valor_total == pytest.approx(3000.0)

      def test_adiciona_coluna_valor_total(self, spark):
          """A coluna 'valor_total' deve existir no resultado (etapas seguintes dependem dela)."""
          df = spark.createDataFrame(
              [("p1", "TV", 100.0, 1, None, "SP", 1)], SCHEMA_PEDIDOS,
          )
          resultado = Transformation().add_valor_total_pedidos(df)
          assert "valor_total" in resultado.columns

      def test_valor_total_zero_quando_quantidade_e_zero(self, spark):
          """Item devolvido (quantidade=0) deve gerar valor_total=0, não erro nem NULL."""
          df = spark.createDataFrame(
              [("p1", "TV", 500.0, 0, None, "SP", 1)], SCHEMA_PEDIDOS,
          )
          resultado = Transformation().add_valor_total_pedidos(df)
          assert resultado.collect()[0].valor_total == pytest.approx(0.0)

      def test_valor_total_nulo_quando_valor_unitario_e_nulo(self, spark):
          """NULL se propaga em operações aritméticas — comportamento esperado do Spark."""
          df = spark.createDataFrame(
              [("p1", "TV", None, 2, None, "SP", 1)], SCHEMA_PEDIDOS,
          )
          resultado = Transformation().add_valor_total_pedidos(df)
          assert resultado.collect()[0].valor_total is None


  class TestGetTop10Clientes:

      def test_retorna_exatamente_10_quando_ha_mais_de_10(self, spark):
          """Com 15 clientes, o resultado deve conter exatamente 10 linhas."""
          dados = [(i, float(i * 100)) for i in range(1, 16)]
          df = spark.createDataFrame(dados, SCHEMA_PEDIDOS_COM_TOTAL)
          resultado = Transformation().get_top_10_clientes(df)
          assert resultado.count() == 10

      def test_ordena_por_valor_total_decrescente(self, spark):
          """O maior valor_total deve vir primeiro. Ordem ascendente devolveria os 10 piores — bug silencioso."""
          dados = [(3, 500.0), (1, 1500.0), (2, 300.0)]
          df = spark.createDataFrame(dados, SCHEMA_PEDIDOS_COM_TOTAL)
          linhas = Transformation().get_top_10_clientes(df).collect()
          assert linhas[0].id_cliente == 1   # maior valor
          assert linhas[2].id_cliente == 2   # menor valor

      def test_retorna_todos_quando_ha_menos_de_10(self, spark):
          """Com apenas 3 clientes, todos devem retornar (sem erro de limite)."""
          dados = [(1, 100.0), (2, 200.0), (3, 300.0)]
          df = spark.createDataFrame(dados, SCHEMA_PEDIDOS_COM_TOTAL)
          assert Transformation().get_top_10_clientes(df).count() == 3

      def test_agrega_multiplos_pedidos_do_mesmo_cliente(self, spark):
          """Um cliente com vários pedidos deve ter os valores SOMADOS, não contados."""
          dados = [(1, 100.0), (1, 200.0), (2, 500.0)]
          df = spark.createDataFrame(dados, SCHEMA_PEDIDOS_COM_TOTAL)
          linhas = {r.id_cliente: r.valor_total
                    for r in Transformation().get_top_10_clientes(df).collect()}
          assert linhas[1] == pytest.approx(300.0)   # 100 + 200


  class TestJoinPedidosClientes:

      @pytest.fixture
      def pedidos_df(self, spark):
          return spark.createDataFrame([(1, 1500.0), (2, 300.0)], SCHEMA_PEDIDOS_COM_TOTAL)

      @pytest.fixture
      def clientes_df(self, spark):
          dados = [
              (1, "Ana Lima", None, "000.000.000-00", "ana@test.com", None),
              (2, "Carlos Melo", None, "111.111.111-11", "carlos@test.com", None),
          ]
          return spark.createDataFrame(dados, SCHEMA_CLIENTES)

      def test_resultado_contem_apenas_as_colunas_esperadas(self, spark, pedidos_df, clientes_df):
          """O relatório deve expor só id_cliente, nome, email e valor_total — nada de CPF/data_nasc."""
          resultado = Transformation().join_pedidos_clientes(pedidos_df, clientes_df)
          assert set(resultado.columns) == {"id_cliente", "nome", "email", "valor_total"}

      def test_associa_cliente_correto_ao_pedido(self, spark, pedidos_df, clientes_df):
          """Cada id_cliente deve ser ligado ao nome e email corretos."""
          resultado = Transformation().join_pedidos_clientes(pedidos_df, clientes_df)
          linhas = {r.id_cliente: r for r in resultado.collect()}
          assert linhas[1].nome == "Ana Lima"
          assert linhas[1].email == "ana@test.com"

      def test_inner_join_exclui_cliente_sem_pedido(self, spark):
          """Cliente sem pedido não deve aparecer. Um LEFT JOIN poluiria o relatório com valor_total NULL."""
          pedidos = spark.createDataFrame([(1, 1500.0)], SCHEMA_PEDIDOS_COM_TOTAL)
          clientes = spark.createDataFrame(
              [
                  (1, "Ana Lima", None, "000.000.000-00", "ana@test.com", None),
                  (99, "Sem Pedido", None, "999.999.999-99", "x@test.com", None),
              ],
              SCHEMA_CLIENTES,
          )
          resultado = Transformation().join_pedidos_clientes(pedidos, clientes)
          assert resultado.count() == 1
          assert resultado.collect()[0].nome == "Ana Lima"
  ```

**Conceitos importantes deste arquivo:**
- **Organização em classes** (`Test...`): agrupa os testes por método sob teste, deixando a saída do pytest legível e a intenção clara.
- **Casos de borda**: além do "caminho feliz", testamos `quantidade=0`, `valor_unitario=NULL`, menos de 10 clientes e a exclusão de clientes sem pedido. São justamente esses casos que costumam esconder bugs.
- **`pytest.approx`**: números de ponto flutuante (`FloatType`) raramente são exatamente iguais por causa de arredondamento binário. `pytest.approx(3000.0)` compara com uma tolerância, evitando falhas espúrias.
- **Docstrings que explicam o "porquê"**: cada teste documenta qual regra de negócio protege — o teste vira documentação executável.

### 7. Testes unitários do `DataHandler` (I/O com arquivos temporários)

O `DataHandler` lê e escreve arquivos. Mas **não** queremos depender dos datasets reais (grandes e externos). A fixture `tmp_path` do pytest cria um diretório temporário, único por teste e apagado automaticamente — nele geramos arquivos minúsculos de propósito.

- Crie o arquivo `./data-engineering-pyspark/tests/unit/test_data_handler.py`:
  ```python
  # tests/unit/test_data_handler.py
  import gzip
  import json
  import os
  import pytest
  from pyspark.sql.types import (
      ArrayType, FloatType, LongType, StringType, StructField, StructType,
  )

  from io_utils.data_handler import DataHandler


  @pytest.fixture
  def arquivo_clientes_gz(tmp_path):
      """Arquivo JSON gzipado com dois clientes de exemplo."""
      clientes = [
          {"id": 1, "nome": "Ana Lima", "data_nasc": "1985-03-10",
           "cpf": "000.000.000-00", "email": "ana@test.com", "interesses": ["Tech"]},
          {"id": 2, "nome": "Carlos Melo", "data_nasc": "1990-07-22",
           "cpf": "111.111.111-11", "email": "carlos@test.com", "interesses": []},
      ]
      gz_path = tmp_path / "clientes.json.gz"
      with gzip.open(gz_path, "wt", encoding="utf-8") as f:
          for c in clientes:
              f.write(json.dumps(c) + "\n")
      return str(gz_path)


  @pytest.fixture
  def arquivo_pedidos_gz(tmp_path):
      """Arquivo CSV gzipado com três pedidos de exemplo."""
      linhas = [
          "id_pedido;produto;valor_unitario;quantidade;data_criacao;uf;id_cliente",
          "abc-001;TV;1500.0;2;2024-01-01T10:00:00;SP;1",
          "abc-002;PC;3000.0;1;2024-01-02T11:00:00;RJ;2",
          "abc-003;MONITOR;800.0;3;2024-01-03T12:00:00;MG;1",
      ]
      gz_path = tmp_path / "pedidos.csv.gz"
      with gzip.open(gz_path, "wt", encoding="utf-8") as f:
          f.write("\n".join(linhas))
      return str(gz_path)


  class TestLoadClientes:

      def test_le_json_gz_e_retorna_dataframe(self, spark, arquivo_clientes_gz):
          df = DataHandler(spark).load_clientes(arquivo_clientes_gz)
          assert df.count() == 2

      def test_schema_aplica_tipos_corretos(self, spark, arquivo_clientes_gz):
          """Schema explícito evita type coercion: sem ele, 'id' viria como String e quebraria o JOIN."""
          df = DataHandler(spark).load_clientes(arquivo_clientes_gz)
          tipos = {f.name: f.dataType for f in df.schema.fields}
          assert isinstance(tipos["id"], LongType)
          assert isinstance(tipos["interesses"], ArrayType)


  class TestLoadPedidos:

      def test_le_csv_gz_com_separador_ponto_e_virgula(self, spark, arquivo_pedidos_gz):
          df = DataHandler(spark).load_pedidos(
              arquivo_pedidos_gz, compression="gzip", header=True, sep=";",
          )
          assert df.count() == 3

      def test_schema_pedidos_tem_tipos_numericos(self, spark, arquivo_pedidos_gz):
          """Sem schema, valor_unitario e quantidade viriam como String e a multiplicação falharia."""
          df = DataHandler(spark).load_pedidos(
              arquivo_pedidos_gz, compression="gzip", header=True, sep=";",
          )
          tipos = {f.name: f.dataType for f in df.schema.fields}
          assert isinstance(tipos["valor_unitario"], FloatType)
          assert isinstance(tipos["quantidade"], LongType)


  class TestWriteParquet:

      def test_dados_gravados_podem_ser_relidos(self, spark, tmp_path):
          """Verificar só a criação do diretório não basta: relemos para garantir integridade."""
          schema = StructType([
              StructField("id_cliente", LongType(), True),
              StructField("valor_total", FloatType(), True),
          ])
          df = spark.createDataFrame([(1, 3000.0), (2, 300.0)], schema)
          output_path = str(tmp_path / "saida_parquet")

          DataHandler(spark).write_parquet(df, output_path)

          assert os.path.exists(output_path)
          assert spark.read.parquet(output_path).count() == 2
  ```

> **`tmp_path`** é uma fixture nativa do pytest que entrega um `pathlib.Path` para um diretório temporário isolado. Cada teste recebe o seu, e o pytest limpa tudo automaticamente — testes que não deixam lixo são testes confiáveis.

### 8. Testes unitários de `carregar_config` (sem Spark)

Estes são os testes **mais rápidos** da suíte: validam apenas a leitura do YAML e nem precisam de Spark. Aqui também testamos o **caminho de erro** (arquivo inexistente).

- Crie o arquivo `./data-engineering-pyspark/tests/unit/test_settings.py`:
  ```python
  # tests/unit/test_settings.py
  import pytest
  import yaml

  from config.settings import carregar_config


  @pytest.fixture
  def arquivo_config_valido(tmp_path):
      """Cria um settings.yaml mínimo e válido em diretório temporário."""
      config_data = {
          "spark": {"app_name": "TestApp"},
          "paths": {
              "clientes": "/dados/clientes.json.gz",
              "pedidos": "/dados/pedidos/",
              "output": "/dados/output/",
          },
          "file_options": {
              "pedidos_csv": {"compression": "gzip", "header": True, "sep": ";"}
          },
      }
      config_file = tmp_path / "settings.yaml"
      config_file.write_text(yaml.dump(config_data))
      return str(config_file)


  class TestCarregarConfig:

      def test_carrega_yaml_valido_como_dicionario(self, arquivo_config_valido):
          assert isinstance(carregar_config(arquivo_config_valido), dict)

      def test_valores_sao_lidos_sem_distorcao(self, arquivo_config_valido):
          resultado = carregar_config(arquivo_config_valido)
          assert resultado["spark"]["app_name"] == "TestApp"
          assert resultado["file_options"]["pedidos_csv"]["sep"] == ";"

      def test_arquivo_inexistente_lanca_excecao(self):
          """O pipeline deve falhar rápido e com clareza, não silenciosamente com None."""
          with pytest.raises(FileNotFoundError):
              carregar_config("/caminho/que/nao/existe/settings.yaml")
  ```

> **`pytest.raises`** verifica que um bloco **lança** a exceção esperada. O teste passa se — e somente se — `FileNotFoundError` for levantada. Testar o caminho de erro é tão importante quanto testar o caminho feliz.

### 9. Testes unitários do `SparkSessionManager` (contrato de Singleton)

Aqui verificamos o **contrato público** da classe: retornar uma `SparkSession` válida e **reutilizar** a sessão existente (comportamento de Singleton via `getOrCreate`).

- Crie o arquivo `./data-engineering-pyspark/tests/unit/test_spark_session.py`:
  ```python
  # tests/unit/test_spark_session.py
  from pyspark.sql import SparkSession

  from session.spark_session import SparkSessionManager


  class TestSparkSessionManager:

      def test_retorna_instancia_de_spark_session(self, spark):
          sessao = SparkSessionManager.get_spark_session(app_name="test-contrato")
          assert isinstance(sessao, SparkSession)

      def test_getorcreate_reutiliza_a_mesma_sessao(self, spark):
          """Chamadas subsequentes devem devolver a MESMA instância (Singleton via getOrCreate)."""
          sessao_a = SparkSessionManager.get_spark_session(app_name="test-a")
          sessao_b = SparkSessionManager.get_spark_session(app_name="test-b")
          assert sessao_a is sessao_b
  ```

### 10. Testes de integração do `Pipeline`

Lembra do [Passo 7](#passo-7-injeção-de-dependências), onde injetamos `DataHandler` e `Transformation` no `Pipeline`? **Agora colhemos o benefício.** Faremos dois estilos complementares:

1. **Orquestração (com *mock*):** substituímos o `DataHandler` por um objeto falso (`MagicMock`) e verificamos *se* e *como* o `Pipeline` chama suas dependências — sem tocar no disco.
2. **End-to-end (sem *mock*):** rodamos o pipeline inteiro com dados reais pequenos e conferimos o Parquet de saída.

- Crie o arquivo `./data-engineering-pyspark/tests/integration/test_pipeline.py`:
  ```python
  # tests/integration/test_pipeline.py
  import gzip
  import json
  import pytest
  from unittest.mock import MagicMock
  from pyspark.sql.types import (
      ArrayType, DateType, FloatType, LongType, StringType,
      StructField, StructType, TimestampType,
  )

  from io_utils.data_handler import DataHandler
  from pipeline.pipeline import Pipeline
  from processing.transformations import Transformation


  SCHEMA_PEDIDOS = StructType([
      StructField("id_pedido", StringType(), True),
      StructField("produto", StringType(), True),
      StructField("valor_unitario", FloatType(), True),
      StructField("quantidade", LongType(), True),
      StructField("data_criacao", TimestampType(), True),
      StructField("uf", StringType(), True),
      StructField("id_cliente", LongType(), True),
  ])

  SCHEMA_CLIENTES = StructType([
      StructField("id", LongType(), True),
      StructField("nome", StringType(), True),
      StructField("data_nasc", DateType(), True),
      StructField("cpf", StringType(), True),
      StructField("email", StringType(), True),
      StructField("interesses", ArrayType(StringType()), True),
  ])


  @pytest.fixture
  def config_teste():
      return {
          "paths": {
              "clientes": "/mock/clientes.json.gz",
              "pedidos": "/mock/pedidos/",
              "output": "/mock/output/",
          },
          "file_options": {
              "pedidos_csv": {"compression": "gzip", "header": True, "sep": ";"}
          },
      }


  @pytest.fixture
  def dataframes_mock(spark):
      pedidos_df = spark.createDataFrame(
          [("p1", "TV", 1500.0, 2, None, "SP", 1),
           ("p2", "PC", 3000.0, 1, None, "RJ", 2)],
          SCHEMA_PEDIDOS,
      )
      clientes_df = spark.createDataFrame(
          [(1, "Ana Lima", None, "000.000.000-00", "ana@test.com", None),
           (2, "Carlos Melo", None, "111.111.111-11", "carlos@test.com", None)],
          SCHEMA_CLIENTES,
      )
      return pedidos_df, clientes_df


  def _handler_mock(pedidos_df, clientes_df):
      """DataHandler falso que devolve DataFrames pré-definidos, sem ler disco."""
      handler = MagicMock(spec=DataHandler)
      handler.load_clientes.return_value = clientes_df
      handler.load_pedidos.return_value = pedidos_df
      return handler


  class TestPipelineOrquestracao:
      """Verifica SE e COMO o Pipeline chama suas dependências, usando um DataHandler mockado."""

      def test_le_clientes_com_path_da_config(self, spark, config_teste, dataframes_mock):
          handler = _handler_mock(*dataframes_mock)
          Pipeline(handler, Transformation()).run(config_teste)
          handler.load_clientes.assert_called_once_with(path="/mock/clientes.json.gz")

      def test_le_pedidos_com_parametros_da_config(self, spark, config_teste, dataframes_mock):
          """Um separador errado faria o CSV ser lido como uma coluna só — sem erro, mas com dados errados."""
          handler = _handler_mock(*dataframes_mock)
          Pipeline(handler, Transformation()).run(config_teste)
          handler.load_pedidos.assert_called_once_with(
              path="/mock/pedidos/", compression="gzip", header=True, sep=";",
          )

      def test_grava_no_path_de_output(self, spark, config_teste, dataframes_mock):
          handler = _handler_mock(*dataframes_mock)
          Pipeline(handler, Transformation()).run(config_teste)
          handler.write_parquet.assert_called_once()
          assert handler.write_parquet.call_args.kwargs["path"] == "/mock/output/"


  class TestPipelineEndToEnd:
      """Dados reais pequenos percorrem TODO o pipeline e verificamos o Parquet final."""

      def test_pipeline_completo_gera_parquet_valido(self, spark, tmp_path):
          clientes = [
              {"id": 1, "nome": "Ana Lima", "data_nasc": "1985-03-10",
               "cpf": "000.000.000-00", "email": "ana@test.com", "interesses": ["Tech"]},
              {"id": 2, "nome": "Carlos Melo", "data_nasc": "1990-07-22",
               "cpf": "111.111.111-11", "email": "carlos@test.com", "interesses": []},
          ]
          clientes_path = tmp_path / "clientes.json.gz"
          with gzip.open(clientes_path, "wt", encoding="utf-8") as f:
              for c in clientes:
                  f.write(json.dumps(c) + "\n")

          pedidos_lines = [
              "id_pedido;produto;valor_unitario;quantidade;data_criacao;uf;id_cliente",
              "abc-001;TV;1500.0;2;2024-01-01T10:00:00;SP;1",
              "abc-002;PC;3000.0;1;2024-01-02T11:00:00;RJ;2",
              "abc-003;MONITOR;800.0;1;2024-01-03T12:00:00;MG;1",
          ]
          pedidos_path = tmp_path / "pedidos.csv.gz"
          with gzip.open(pedidos_path, "wt", encoding="utf-8") as f:
              f.write("\n".join(pedidos_lines))

          output_path = str(tmp_path / "output")
          config = {
              "paths": {
                  "clientes": str(clientes_path),
                  "pedidos": str(pedidos_path),
                  "output": output_path,
              },
              "file_options": {
                  "pedidos_csv": {"compression": "gzip", "header": True, "sep": ";"}
              },
          }

          Pipeline(DataHandler(spark), Transformation()).run(config)

          resultado = spark.read.parquet(output_path)
          assert set(resultado.columns) == {"id_cliente", "nome", "email", "valor_total"}
          # Ana Lima: pedidos abc-001 (1500×2=3000) + abc-003 (800×1=800) = 3800
          ana = resultado.where("nome = 'Ana Lima'").collect()
          assert ana[0].valor_total == pytest.approx(3800.0)
  ```

> **O que é um `MagicMock(spec=DataHandler)`?** Um objeto falso que tem a mesma "cara" do `DataHandler` (os mesmos métodos), mas cujo comportamento nós controlamos. `assert_called_once_with(...)` verifica que o método foi chamado **exatamente uma vez** e **com os argumentos esperados**. Assim testamos a *orquestração* do `Pipeline` sem ler um único arquivo — só possível porque o `DataHandler` é **injetado** no construtor.

### 11. Executando os testes

A partir da raiz do projeto:

  ```bash
  cd ./data-engineering-pyspark
  pytest

  ```

A saída lista cada teste (graças ao `-v` do `addopts`):

  ```
  ============================= test session starts ==============================
  collected 18 items

  tests/integration/test_pipeline.py::TestPipelineOrquestracao::test_le_clientes_com_path_da_config PASSED
  ...
  tests/unit/test_transformations.py::TestAddValorTotalPedidos::test_calcula_valor_unitario_por_quantidade PASSED
  ...
  ============================== 18 passed in 12.34s =============================
  ```

Para rodar **apenas** uma camada, selecione pelo diretório:

  ```bash
  pytest tests/unit          # só os testes unitários (rápidos)
  pytest tests/integration   # só os testes de integração

  ```

> Os marcadores declarados no `pytest.ini` permitem filtrar com `pytest -m unit`. Para usá-los, marque os testes — por exemplo, adicionando no topo de cada arquivo unitário a linha `pytestmark = pytest.mark.unit` (e `pytestmark = pytest.mark.integration` no arquivo de integração).

### 12. Medindo a cobertura de código

Cobertura indica **quais linhas do código foram exercitadas** pelos testes. É um termômetro útil: embora 100% de cobertura não garanta ausência de bugs, áreas com cobertura baixa são pontos cegos.

Rode com o `pytest-cov`, apontando para o pacote `src`:

  ```bash
  pytest --cov=src --cov-report=term-missing

  ```

A saída mostra a porcentagem por arquivo e **quais linhas faltam** (`Missing`):

  ```
  ---------- coverage: ... ----------
  Name                                 Stmts   Miss  Cover   Missing
  ------------------------------------------------------------------
  src/config/settings.py                   3      0   100%
  src/io_utils/data_handler.py            18      1    94%   42
  src/pipeline/pipeline.py                25      0   100%
  src/processing/transformations.py        9      0   100%
  src/session/spark_session.py             4      0   100%
  ------------------------------------------------------------------
  TOTAL                                   59      2    97%
  ```

Para um relatório navegável em HTML:

  ```bash
  pytest --cov=src --cov-report=html
  # abra htmlcov/index.html no navegador

  ```

> **Cuidado com a métrica:** busque cobrir os **caminhos críticos e os casos de borda** (foi o que fizemos), não perseguir 100% a qualquer custo. Um teste que executa o código mas não verifica nada (sem `assert`) aumenta a cobertura sem proteger contra nada.

### Recapitulando

Saímos de 2 testes para uma suíte completa que cobre todas as camadas da aplicação:

| Camada | Arquivo | O que protege |
|---|---|---|
| Unitário | `test_transformations.py` | Regras de negócio + casos de borda (nulo, zero, agregação, ordenação, join) |
| Unitário | `test_data_handler.py` | Leitura/escrita e aplicação correta dos schemas |
| Unitário | `test_settings.py` | Carga de configuração e falha explícita em arquivo ausente |
| Unitário | `test_spark_session.py` | Contrato de Singleton da sessão Spark |
| Integração | `test_pipeline.py` | Orquestração (mock) + fluxo end-to-end (Parquet final) |

Essa rede de segurança permite refatorar e evoluir o projeto com confiança — exatamente o objetivo de toda a jornada de engenharia de software deste tutorial.

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


