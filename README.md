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
  curl -L --output-dir /tmp -O https://raw.githubusercontent.com/infobarbosa/pyspark-poo/main/assets/data/data.csv

  ```

2. Baixe o script `infer-schema.py`:

  ```bash
  curl -L --output-dir /tmp -O https://raw.githubusercontent.com/infobarbosa/pyspark-poo/main/assets/scripts/infer-schema.py

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
df_bonus = df.withColumn(
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
  curl -L --output-dir /tmp -O https://raw.githubusercontent.com/infobarbosa/pyspark-poo/main/assets/scripts/schema-definido.py

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

**1. Atualize o `src/main.py` para usar os Schemas:**

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


**2. Execute o projeto e confira os resultados:**

```sh
spark-submit ./data-engineering-pyspark/src/main.py

```

```sh
parquet-tools show ./data-engineering-pyspark/data/output/pedidos_por_cliente

```

```sh
ls -la ./data-engineering-pyspark/data/output/pedidos_por_cliente/

```

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

**5. Execute o projeto:**

```sh
spark-submit ./data-engineering-pyspark/src/main.py

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

12. Execute o projeto e confira os resultados:

```sh
spark-submit ./data-engineering-pyspark/src/main.py

```

```sh
parquet-tools show ./data-engineering-pyspark/data/output/pedidos_por_cliente

```

```sh
ls -la ./data-engineering-pyspark/data/output/pedidos_por_cliente/

```

---

## Passo 3: Gerenciando a Sessão Spark

A criação da `SparkSession` também pode ser isolada para ser mais reutilizável e fácil de configurar.

1. Crie o diretório:

```bash
mkdir -p ./data-engineering-pyspark/src/session
touch ./data-engineering-pyspark/src/session/__init__.py
touch ./data-engineering-pyspark/src/session/spark_session.py

```

2. Adicione o seguinte código a ele:

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

5. Execute o projeto e confira os resultados:

```sh
spark-submit ./data-engineering-pyspark/src/main.py

```

```sh
parquet-tools show ./data-engineering-pyspark/data/output/pedidos_por_cliente

```

```sh
ls -la ./data-engineering-pyspark/data/output/pedidos_por_cliente/

```

---

## Passo 4: Pacote de Leitura e Escrita de Dados (I/O)

Vamos criar uma classe que lida com todas as operações de entrada (leitura) e saída (escrita) de dados.

1. Crie o diretório e o arquivo de inicialização:

```bash
mkdir -p ./data-engineering-pyspark/src/io_utils
touch ./data-engineering-pyspark/src/io_utils/__init__.py
touch ./data-engineering-pyspark/src/io_utils/data_handler.py

```

2. Adicione o seguinte código a ele:

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

3. Faça os ajustes em `main.py`:

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

6. Execute o projeto e confira os resultados:

```sh
spark-submit ./data-engineering-pyspark/src/main.py

```

```sh
parquet-tools show ./data-engineering-pyspark/data/output/pedidos_por_cliente

```

```sh
ls -la ./data-engineering-pyspark/data/output/pedidos_por_cliente/

```

---

## Passo 5: Isolando a Lógica de Negócio

Esta etapa é semelhante à anterior, mas vamos garantir que o arquivo esteja no lugar certo.

1. Crie o diretório e o arquivo de inicialização:

```sh
mkdir -p ./data-engineering-pyspark/src/processing
touch ./data-engineering-pyspark/src/processing/__init__.py
touch ./data-engineering-pyspark/src/processing/transformations.py

```

2. Adicione o seguinte código a ele:

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

3. Faça os seguintes ajustes em `main.py` :
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

4. Execute o projeto e confira os resultados:

```sh
spark-submit ./data-engineering-pyspark/src/main.py

```

```sh
parquet-tools show ./data-engineering-pyspark/data/output/pedidos_por_cliente

```

```sh
ls -la ./data-engineering-pyspark/data/output/pedidos_por_cliente/

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

2. Execute o projeto:
```sh
spark-submit ./data-engineering-pyspark/src/main.py

```

```sh
parquet-tools show ./data-engineering-pyspark/data/output/pedidos_por_cliente

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
  touch ./data-engineering-pyspark/src/pipeline/__init__.py
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

4. Execute o projeto:

```bash
spark-submit ./data-engineering-pyspark/src/main.py

```

```sh
parquet-tools show ./data-engineering-pyspark/data/output/pedidos_por_cliente

```

```sh
ls -la ./data-engineering-pyspark/data/output/pedidos_por_cliente/

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

#### Os Níveis de Log (Severity Levels)

O sistema de logging do Python classifica as mensagens em 5 níveis padrão de severidade. Em engenharia de dados, entender quando usar cada um é essencial para não transformar seus logs em um "mar de ruído" ou, pior, em um "silêncio perigoso":

| Nível | Valor | Quando Usar em Pipelines de Dados | Exemplo no Nosso Projeto |
| :--- | :---: | :--- | :--- |
| **`DEBUG`** | 10 | Diagnóstico minucioso para desenvolvimento. Inspecionar DataFrames intermediários, planos de execução física ou variáveis de loop. *(Desativado em produção)* | `logger.debug(f"Plano Catalyst: {df._jdf.queryExecution()}")` |
| **`INFO`** | 20 | Marcos normais e esperados do fluxo. Confirmação de início/fim de jobs, quantidade de linhas lidas, caminhos de saída salvos. | `logger.info("Pipeline finalizado com sucesso.")` |
| **`WARNING`** | 30 | Alerta sobre algo inesperado, mas que **não impediu** a continuidade do pipeline. | `logger.warning("Arquivo lido está vazio.")` |
| **`ERROR`** | 40 | Uma falha impediu a conclusão de uma operação importante, mas o sistema como um todo pode tentar continuar ou tratar o erro. | `logger.error("Falha ao salvar partição no Parquet.")` |
| **`CRITICAL`** | 50 | Falha catastrófica que inviabiliza todo o ambiente. O processo precisa ser abortado imediatamente. | `logger.critical("Sem memória no cluster (OOM) ou storage inacessível.")` |

> [!TIP]
> **Como o nível mínimo funciona?**  
> Se configuramos `level: INFO` no `settings.yaml`, o logger exibirá mensagens `INFO`, `WARNING`, `ERROR` e `CRITICAL`. Todas as mensagens `DEBUG` serão silenciosamente ignoradas pelo Spark/Python, economizando espaço em disco e I/O.

#### A Hierarquia de Loggers
O módulo `logging` do Python organiza os loggers em uma hierarquia baseada em nomes separados por pontos. Por exemplo, um logger chamado pacote1.modulo1 é filho do logger pacote1, que por sua vez é filho do logger raiz (root).

A grande vantagem é que, por padrão, as mensagens de um logger filho são propagadas para os "handlers" (manipuladores) do seu logger pai. É por isso que podemos configurar o logger raiz uma única vez e todos os outros loggers do projeto enviarão suas mensagens para os handlers configurados nele.

A melhor prática é obter um logger em cada módulo usando a variável especial __name__:

```python
import logging
logger = logging.getLogger(__name__)
```

1. Substitua completamente o conteúdo de `settings.py`:

  ```python
  # src/config/settings.py
  import yaml
  import logging.config

  def carregar_config(path: str = "./data-engineering-pyspark/config/settings.yaml") -> dict:
      """Carrega o arquivo YAML."""
      with open(path, 'r') as file:
          return yaml.safe_load(file)

  def configurar_logging(config_logging: dict):
      """Aplica a configuração de logging lida do YAML."""
      logging.config.dictConfig(config_logging)
      logging.getLogger(__name__).info("Logging configurado com sucesso via YAML.")
  ```

2. Acrescente o seguinte conteúdo ao arquivo `settings.yaml`:
  ```yaml
  # Configuração Padrão de Mercado para Logging
  logging:
    version: 1
    disable_existing_loggers: False
    formatters:
      padrao:
        format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        datefmt: "%Y-%m-%d %H:%M:%S"
    handlers:
      console:
        class: logging.StreamHandler
        level: INFO
        formatter: padrao
        stream: ext://sys.stdout
      file:
        class: logging.FileHandler
        level: INFO
        formatter: padrao
        filename: "dataeng-pyspark-poo.log"
    root:
      level: INFO
      handlers: [console, file]
  ```

3. Substitua o conteúdo completo de `main.py` pelo código abaixo:

  ```python
  # src/main.py
  from config.settings import carregar_config, configurar_logging
  from session.spark_session import SparkSessionManager
  from io_utils.data_handler import DataHandler
  from processing.transformations import Transformation
  from pipeline.pipeline import Pipeline
  import logging

  def main():
      # 1. Carrega configurações do YAML
      config = carregar_config()

      # 2. Ativa o log instantaneamente
      configurar_logging(config['logging'])

      # 3. Inicia a aplicação
      logger = logging.getLogger(__name__)
      logger.info(f"Iniciando job: {config['spark']['app_name']}")

      spark = SparkSessionManager.get_spark_session(app_name=config['spark']['app_name'])

      # Composition Root
      data_handler = DataHandler(spark)
      transformer = Transformation()
      pipeline = Pipeline(data_handler, transformer)
      
      pipeline.run(config=config)

      spark.stop()
      logger.info("Pipeline finalizado com sucesso.")

  if __name__ == "__main__":
      main()
  ```

4. Em todas as classes, adicione a configuração do logger no início do arquivo e substitua todos os `print()` por chamadas ao `logging`.<br>

  Por exemplo, a seguir está o código completo da classe Pipeline (`src/pipeline.py`) instancia e utiliza o objeto `logger`:

  ```python
  # src/pipeline/pipeline.py
  from io_utils.data_handler import DataHandler
  from processing.transformations import Transformation
  import logging

  logger = logging.getLogger(__name__)

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
          logger.info("Pipeline iniciado...")

          logger.info("Abrindo o dataframe de clientes")
          path_clientes = config['paths']['clientes']
          logger.info(f"Obtido o path de clientes: {path_clientes}")
          clientes_df = self.data_handler.load_clientes(path = path_clientes)
          clientes_df.show(5, truncate=False)

          logger.info("Abrindo o dataframe de pedidos")
          path_pedidos = config['paths']['pedidos']
          compression_pedidos = config['file_options']['pedidos_csv']['compression']
          header_pedidos = config['file_options']['pedidos_csv']['header']
          separator_pedidos = config['file_options']['pedidos_csv']['sep']
          
          logger.info(f"""Obtidos os seguintes parâmetros de pedidos:
          - path: {path_pedidos}
          - compression: {compression_pedidos}
          - header: {header_pedidos}
          - separator: {separator_pedidos}
          """)
          
          pedidos_df = self.data_handler.load_pedidos(path = path_pedidos, compression=compression_pedidos, header=header_pedidos, sep=separator_pedidos)
          
          logger.info("Adicionando a coluna valor_total")
          pedidos_df = self.transformer.add_valor_total_pedidos(pedidos_df)
          pedidos_df.show(5, truncate=False)
          
          logger.info("Calculando o valor total de pedidos por cliente e filtrar os 10 maiores")
          top_10_clientes_df = self.transformer.get_top_10_clientes(pedidos_df)
          
          top_10_clientes_df.show(10, truncate=False)
          
          logger.info("Fazendo a junção dos dataframes")
          relatorio_top_10_cliente_df = self.transformer.join_pedidos_clientes(top_10_clientes_df, clientes_df)
          relatorio_top_10_cliente_df.show(20, truncate=False)
          
          logger.info("Escrevendo o resultado em parquet")
          path_output = config['paths']['output']
          logger.info(f"Obtido o path de saída: {path_output}")
          self.data_handler.write_parquet(df=relatorio_top_10_cliente_df, path=path_output)

          logger.info("Pipeline concluído com sucesso!")
      
  ```

6. Execute novamente e confira o arquivo gerado `dataeng-pyspark-poo.log`:

```sh
spark-submit ./data-engineering-pyspark/src/main.py

```

```sh
tail -100 dataeng-pyspark-poo.log

```

---

## Passo 9: Tratamento de Erros

Ao trabalhar com processamento de dados em grande escala, é inevitável que nos deparemos com imprevistos, como dados ausentes ou malformados, falhas de conexão com fontes de dados ou erros de lógica em nossas transformações. Ignorar essas possíveis falhas pode levar à interrupção de pipelines, corrupção silenciosa de dados e diagnósticos demorados em produção.

---

### A Evolução do Tratamento de Erros no PySpark: O Pacote `pyspark.errors`

Compreender onde e como os erros são disparados no PySpark depende de entender a evolução da própria biblioteca:

#### Como era antes do PySpark 4 (versões legadas e Spark 2.x / 3.x inicial)
* **Exceções dispersas:** Exceções do Spark SQL eram importadas de módulos secundários, principalmente `pyspark.sql.utils` (por exemplo, `from pyspark.sql.utils import AnalysisException`).
* **Dependência da ponte Py4J (`Py4JJavaError`):** Como o PySpark é uma camada Python sobre a JVM, boa parte dos erros ocorridos na execução física não eram traduzidos. Eles chegavam ao Python empacotados como `py4j.protocol.Py4JJavaError`, exibindo *stack traces* Java quilométricos e forçando o desenvolvedor a capturar erros genéricos de infraestrutura em vez de exceções semânticas de negócio.
* **Diagnóstico frágil:** Não existia uma padronização formal de códigos de erro; identificar a causa raiz muitas vezes exigia fazer *parsing* de strings na mensagem de erro do Java.

#### Como é agora (PySpark 4 e consolidação do `pyspark.errors`)
A partir do PySpark 3.4 e consolidado de forma definitiva no **PySpark 4**, o Apache Spark introduziu uma hierarquia unificada e idiomática de erros no pacote nativo [**`pyspark.errors`**](https://spark.apache.org/docs/latest/api/python/reference/pyspark.errors.html):

* **Módulo Oficial Centralizado:** Todas as exceções do PySpark (Core, SQL, Streaming e Connect) estão centralizadas em `pyspark.errors`. Agora importamos diretamente:
  ```python
  from pyspark.errors import PySparkException, AnalysisException, ParseException
  ```
* **Hierarquia Unificada (`PySparkException`):** Todas as exceções de usuário herdam da classe base `PySparkException`, permitindo capturar tanto falhas específicas quanto qualquer erro originado no ecossistema Spark de forma limpa.
* **Error Classes e SQLSTATE:** Os erros agora contam com classes de erro padronizadas e códigos SQLSTATE (padrão ANSI SQL). Métodos como `e.getErrorClass()`, `e.getSqlState()` e `e.getMessageParameters()` permitem criar regras de monitoramento, métricas e retentativas programáticas sem depender de expressões regulares na mensagem de erro.
* **Menos atrito com a JVM:** Grande parte dos erros que antes estouravam como `Py4JJavaError` agora são interceptados e mapeados para subclasses nativas de `PySparkException`, tornando o código muito mais legível e manutenível.

---

### Estrutura Básica de Tratamento de Erros (`try / except`)

Antes de vermos os cenários práticos no PySpark, vale recapitular como o Python gerencia exceções através dos blocos `try`, `except`, `else` e `finally`, e quais são as boas práticas recomendadas para pipelines de dados:

```python
try:
    # 1. Bloco protegido: onde ocorrem operações com risco de falha
    logger.info("Iniciando leitura e processamento...")
    df = spark.read.csv("caminho/para/dados.csv", header=True)
    df.show(5)

except PySparkException as e:
    # 2. Captura específica: trata ou registra erros nativos do PySpark
    logger.error(f"Falha de execução no PySpark: {e}")
    raise  # Relança o erro para que o orquestrador (ex: Airflow) detecte a falha

except Exception as e:
    # 3. Captura genérica: última linha de defesa para erros inesperados
    logger.exception("Erro inesperado no pipeline.")
    raise

else:
    # 4. Executado APENAS se o bloco 'try' foi concluído sem lançar nenhuma exceção
    logger.info("Etapa concluída com sucesso.")

finally:
    # 5. Executado SEMPRE, ocorrendo erro ou não
    # Ideal para encerramento de conexões, limpeza de temporários ou auditoria
    logger.info("Finalizando rotina e liberando recursos.")
```

#### Boas Práticas em Pipelines de Dados:
1. **Nunca "engula" erros em silêncio:** Evite `except: pass`. Silenciar exceções mascara problemas graves, gera perda de integridade nos dados e dificulta a auditoria em produção.
2. **Ordene do mais específico ao mais genérico:** Capture primeiro as exceções especializadas do PySpark (ex: `AnalysisException`, `ParseException`), depois a classe base do framework (`PySparkException`) e, por último, a classe genérica do Python (`Exception`).
3. **Registre com `logger.exception` ou `logger.error` (e saiba a diferença):** Não use `print()`. Dentro de blocos `except`, escolha conscientemente se você quer ou não anexar o stack trace completo do erro (veja a explicação aprofundada logo abaixo).
4. **Relance (`raise`) quando necessário:** Se a integridade dos dados for violada ou uma etapa indispensável quebrar, pare o fluxo imediatamente para não propagar dados corrompidos.

#### 💡 `logger.error` vs `logger.exception`: Qual é a Melhor Prática?

Uma dúvida muito frequente em engenharia de software e de dados é: **quando usar `logger.error` e quando usar `logger.exception`?**

Ambos registram a mensagem com o nível de severidade **ERROR (40)**, mas o comportamento de diagnóstico é bem diferente:

* **`logger.exception("Mensagem")`**:
  * **O que faz:** Registra a mensagem no nível `ERROR` e **anexa automaticamente o *traceback* (stack trace)** completo da exceção ativa.
  * **Equivalência técnica:** Chamar `logger.error("Mensagem", exc_info=True)`.
  * **Regra de Linters modernos (Ruff / Flake8):** As regras [G201 / LOG007](https://docs.astral.sh/ruff/rules/error-with-exc-info/) consideram `logger.error(..., exc_info=True)` um anti-pattern (*code smell*) redundante e recomendam explicitamente o uso de `logger.exception(...)`.
  * **Quando usar:** 
    1. No ponto de entrada da aplicação (como no `main.py`), onde a exceção é capturada e encerra o job (`sys.exit(1)`). Sem o `logger.exception`, o histórico detalhado da falha seria perdido.
    2. Em falhas técnicas graves, erros inesperados ou bugs de código em que os engenheiros que receberem o alerta (PagerDuty, CloudWatch, Datadog) precisarão ver exatamente em qual linha e módulo o erro estourou.
  * **Atenção:** Só deve ser chamado **dentro de um bloco `except`**. Se chamado fora dele, o Python registrará `NoneType: None` no traceback.

* **`logger.error("Mensagem")`**:
  * **O que faz:** Registra uma mensagem no nível `ERROR` em **uma única linha de texto limpa**, sem stack trace.
  * **Quando usar:**
    1. Erros conhecidos de negócio ou validações funcionais onde você quer sinalizar uma falha, mas onde o stack trace do Python não agrega valor e apenas poluiria os arquivos de log (ex: *"Arquivo de clientes não encontrado no bucket. Abortando etapa."*).
    2. Em camadas internas (como faremos no `data_handler.py`), quando você apenas registra um log contextual antes de relançar a exceção (`raise LoadPedidosException(...) from e`). Nesse caso, o stack trace será preservado pela própria exceção chained e registrado na borda da aplicação.
    3. Fora de blocos `except`, para sinalizar qualquer condição de erro lógica.

| Aspecto | `logger.exception(...)` | `logger.error(...)` |
| :--- | :--- | :--- |
| **Nível de Severidade** | `ERROR` (40) | `ERROR` (40) |
| **Gera Traceback (Stack Trace)?** | **Sim**, automaticamente (`exc_info=True`) | **Não** por padrão (apenas o texto) |
| **Onde pode ser chamado?** | **Apenas dentro de blocos `except`** | Em qualquer lugar do código |
| **Caso de Uso Típico** | Borda da aplicação (`main.py`), falhas técnicas e exceções inesperadas | Validações de negócio, camadas intermediárias com `raise ... from` ou logs de erro simples |

> [!TIP]
> **Evite mensagens redundantes com `logger.exception`!**  
> Como o `logger.exception` já anexa o traceback completo (e a última linha do traceback sempre contém a mensagem da exceção original), evite concatenar `{e}` na string:
> ```python
> # ❌ Redundante (a mensagem de erro aparecerá duas vezes no log):
> logger.exception(f"Erro capturado no pipeline: {e}")
>
> # ✅ Idiomático e objetivo (o detalhe técnico já estará no traceback):
> logger.exception("Falha técnica durante a execução do pipeline de pedidos.")
> ```

---

### Cenário 1: A Exceção Base do Ecossistema - `PySparkException`

#### O que é a `PySparkException`?

A `PySparkException` é a classe base de todas as exceções nativas do PySpark, disponível diretamente no pacote [`pyspark.errors`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.errors.html). 

Antes de sua formalização, capturar erros do Spark no Python era uma tarefa ingrata: ou capturava-se a genérica `Exception` do Python (perdendo o contexto do framework), ou lidava-se com a ruidosa `Py4JJavaError`. Com a `PySparkException`, você ganha uma **rede de segurança padronizada e idiomática** para qualquer falha interna originada no ecossistema Spark (seja no Core, SQL, Streaming ou Connect).

#### Identificação Estruturada de Erros

A maior vantagem da hierarquia moderna é eliminar o *parsing* manual de strings de erro. Toda exceção derivada de `PySparkException` fornece métodos nativos para diagnóstico:

* **`e.getErrorClass()`**: Retorna a classe textual padronizada do erro (ex: `"PATH_NOT_FOUND"`, `"COLUMN_NOT_FOUND"`, `"CANNOT_PARSE_INTERVAL"`). Ideal para condicionais e métricas.
* **`e.getSqlState()`**: Retorna o código padrão ANSI SQL associado (ex: `"42000"` para erros de sintaxe ou semântica).
* **`e.getMessageParameters()`**: Retorna um dicionário com os valores reais que causaram a falha (ex: nome da tabela ou caminho que não foi encontrado).

#### Exemplo de `PySparkException`

Vamos fazer alguns ajustes em `data_handler.py` para capturar exceções PySpark.

1. Importações
```python
from pyspark.errors import PySparkException
import logging
```

2. Ajuste do logger
```python
logger = logging.getLogger(__name__)
```

3. Aplicação no método `load_pedidos`
```python

    def load_pedidos(self, path: str, compression: str, header:bool, sep:str) -> DataFrame:
        try:
            schema = self._get_schema_pedidos()
            df = self.spark.read \
                .option("compression", compression) \
                .csv(path, header=header, schema=schema, sep=sep)
            
            if df.isEmpty():
                logger.warning(f"ATENÇÃO: O arquivo em '{path}' foi lido mas não contém registros.")
            
            return df

        except PySparkException as e:
            logger.error(f"Erro no PySpark [Classe: {e.getErrorClass()} | SQLSTATE: {e.getSqlState()}]: {e}")
            raise e
```

---

### Cenário 2: Falhas Estruturais e Metadados - `AnalysisException`

#### O que é a `AnalysisException`?

A `AnalysisException` é uma **subclasse especializada de `PySparkException`**.

O PySpark utiliza um modelo de **avaliação preguiçosa (lazy evaluation)**. Quando você escreve uma operação (selecionar colunas, fazer filtros ou joins), o Spark não executa imediatamente — ele constrói um "plano lógico".

Antes de transformar esse plano lógico em execução física nos nós do cluster, o componente central do Spark chamado **Catalyst Optimizer** analisa o seu código para validar se ele faz sentido com relação aos metadados. A `AnalysisException` é o erro que o Catalyst lança quando **o seu plano lógico é inválido**.

#### Principais causas desse erro

Na prática, a `AnalysisException` é o Spark dizendo: *"Eu entendi o seu código Python, mas a lógica de banco de dados ou a estrutura dos dados está errada"*. Exemplos frequentes em pipelines:

* **Coluna Inexistente (UNRESOLVED_COLUMN):** Você tentou selecionar, filtrar ou agrupar por uma coluna que não existe no DataFrame ou foi digitada com erro de digitação.
* **Ambiguidade de Colunas (AMBIGUOUS_REFERENCE):** Muito comum após um `join` entre duas tabelas que possuem colunas com o mesmo nome sem aliases.
* **Incompatibilidade de Tipos (DATATYPE_MISMATCH):** Você tentou realizar uma operação matemática em uma coluna de texto (String) ou comparar tipos de dados incompatíveis.
* **Caminho Inexistente (`PATH_NOT_FOUND`):** Você tentou ler um arquivo ou pasta que não existe na origem.
* **Erro de Sintaxe SQL:** Quando você usa `spark.sql("SELECT * FRM tabela")` e comete um erro de sintaxe na query.

#### Como evitar a AnalysisException

A melhor forma de lidar com esse erro é adotar práticas defensivas:

1. **Verifique a existência da coluna:** Antes de operar, cheque a lista de colunas com `if "coluna" in df.columns:`.
2. **Resolva ambiguidades no Join:** Use aliases (apelidos) para os DataFrames antes de juntá-los e chame as colunas pelo alias (`df_a["id"]`).
3. **Conheça seus dados:** Use `df.printSchema()` frequentemente durante o desenvolvimento para garantir que a tipagem e os nomes estão corretos.

#### Exemplo de `AnalysisException`

Como a `AnalysisException` é mais específica que a `PySparkException`, nós a capturamos **primeiro**:

1. Importe `AnalysisException`
```python
from pyspark.errors import AnalysisException, PySparkException
```

2. Aplique o tratamento de erro:
```python
    def load_pedidos(self, path: str, compression: str, header:bool, sep:str) -> DataFrame:
        try:
            schema = self._get_schema_pedidos()
            df = self.spark.read \
                .option("compression", compression) \
                .csv(path, header=header, schema=schema, sep=sep)
            
            if df.isEmpty():
                logger.warning(f"ATENÇÃO: O arquivo em '{path}' foi lido mas não contém registros.")
            
            return df

        except AnalysisException as e:
            logger.error(f"Erro de análise/metadados no Spark [Classe: {e.getErrorClass()}]: {e}")
            raise e
        except PySparkException as e:
            logger.error(f"Erro no PySpark [Classe: {e.getErrorClass()} | SQLSTATE: {e.getSqlState()}]: {e}")
            raise e
```

---

> [!NOTE]
> ### 💡 Por que não capturamos `Py4JJavaError`? (Legado vs. Spark Connect)
> 
> Em tutoriais mais antigos ou discussões no StackOverflow, você verá com frequência códigos fazendo `from py4j.protocol import Py4JJavaError`. No PySpark moderno (3.4+ e 4.0), **isso se tornou um anti-pattern** por duas razões principais:
> 
> 1. **Vazamento de Abstração:** O Py4J é apenas a biblioteca que implementa a ponte de comunicação entre o interpretador Python e o processo Java no modo clássico. Depender diretamente dele acopla o seu código de dados a detalhes internos de infraestrutura.
> 2. **Incompatibilidade com o Spark Connect:** A nova arquitetura padrão do PySpark (Spark Connect) comunica-se com clusters via **gRPC**, sem utilizar Py4J no lado cliente. Códigos que dependem de `Py4JJavaError` tornam-se frágeis e incompatíveis com ambientes modernos como Databricks Serverless.
> 
> A criação do módulo [`pyspark.errors`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.errors.html) veio justamente para resolver esse problema: todas as falhas de execução e da JVM agora são traduzidas para exceções nativas (`PySparkException` e suas especializações).

---

### Blindando `DataHandler` com Exceções Customizadas (POO)

Até aqui, vimos como capturar as exceções técnicas do Spark (`AnalysisException` e `PySparkException`). No entanto, em um projeto orientado a objetos e bem arquitetado, **o chamador (como o `Pipeline` ou o `main.py`) não deve depender de exceções internas do framework**.

Como boa prática de engenharia de software e para evitar acoplamento desnecessário ou *circular imports*, as exceções de uma camada devem residir em um módulo isolado (`exceptions.py`), e não misturadas dentro do arquivo da classe executável (`data_handler.py`).

1. Crie o arquivo `src/io_utils/exceptions.py`:

```bash
touch ./data-engineering-pyspark/src/io_utils/exceptions.py

```

2. Defina a hierarquia de exceções da camada de I/O em `src/io_utils/exceptions.py`:

```python
# src/io_utils/exceptions.py

class DataHandlerException(Exception):
    """Exceção base para qualquer falha na camada de I/O."""
    pass

class LoadPedidosException(DataHandlerException):
    """Lançada especificamente ao falhar o carregamento do dataset de pedidos."""
    pass

```

3. Em `src/io_utils/data_handler.py` importe a exceção do módulo recém-criado :

```python
from io_utils.exceptions import LoadPedidosException

```

4. Relance os erros capturados do Spark usando **Exception Chaining** (`raise ... from e`, da PEP 3134):

> [!NOTE]
> No `src/io_utils/data_handler.py`, atualize apenas o método `load_pedidos` com o tratamento de exceções abaixo, mantendo os demais métodos existentes (`load_clientes`, `write_parquet`, etc.) inalterados na classe.

```python

    def load_pedidos(self, path: str, compression: str, header:bool, sep:str) -> DataFrame:
        try:
            schema = self._get_schema_pedidos()
            df = self.spark.read \
                .option("compression", compression) \
                .csv(path, header=header, schema=schema, sep=sep)
            
            # Verificação de Dataframe Vazio
            if df.isEmpty():
                logger.warning(f"ATENÇÃO: O arquivo em '{path}' foi lido mas não contém registros.")
            
            return df

        except AnalysisException as e:
            logger.error(f"Erro de análise/metadados no Spark [Classe: {e.getErrorClass()}]: {e}")
            # Encapsula o erro técnico na exceção de negócio mantendo o traceback original (from e)
            raise LoadPedidosException(f"Falha ao carregar pedidos a partir de '{path}'") from e

        except PySparkException as e:
            logger.error(f"Erro de processamento no PySpark [Classe: {e.getErrorClass()}]: {e}")
            raise LoadPedidosException(f"Erro no motor Spark ao carregar pedidos em '{path}'") from e

```

### Blindando `main.py`

Agora que nosso pacote `io_utils` possui seu próprio módulo de exceções, o `main.py` pode tratar falhas em camadas sem acoplar-se aos detalhes internos da engine:

1. Atualize o `src/main.py` para capturar as falhas do pipeline:

```python
# src/main.py
from config.settings import carregar_config, configurar_logging
from session.spark_session import SparkSessionManager
from io_utils.data_handler import DataHandler
from io_utils.exceptions import DataHandlerException, LoadPedidosException
from processing.transformations import Transformation
from pipeline.pipeline import Pipeline
from pyspark.errors import PySparkException
import logging
import sys

def main():
    config = carregar_config()
    configurar_logging(config['logging'])
    logger = logging.getLogger(__name__)
    logger.info(f"Iniciando job: {config['spark']['app_name']}")

    spark = SparkSessionManager.get_spark_session(app_name=config['spark']['app_name'])

    try:
        data_handler = DataHandler(spark)
        transformer = Transformation()
        pipeline = Pipeline(data_handler, transformer)
        
        pipeline.run(config=config)

        logger.info("Pipeline finalizado com sucesso.")

    except LoadPedidosException as e:
        # 1. Tratamento específico para o dataset crítico de pedidos
        logger.exception(f"Falha no carregamento de pedidos: {e}")
        sys.exit(1)

    except DataHandlerException as e:
        # 2. Tratamento genérico para qualquer outra falha de I/O
        logger.exception(f"Erro na camada de leitura/escrita de dados: {e}")
        sys.exit(1)

    except PySparkException as e:
        # 3. Falhas do Spark ocorridas fora da leitura (ex: ações nas transformações)
        logger.exception(f"Erro originado no PySpark [Classe: {e.getErrorClass()}]: {e}")
        sys.exit(1)

    except Exception as e:
        # 4. Última linha de defesa para erros inesperados
        logger.exception(f"Erro inesperado durante a execução do job: {e}")
        sys.exit(1)

    finally:
        spark.stop()
        logger.info("Spark session encerrada.")
        
if __name__ == "__main__":
    main()

```

### Testando os Erros

Para ver isso funcionando, vamos quebrar nossa aplicação de propósito.

1. **Teste de Arquivo Inexistente:**
Abra o arquivo `config/settings.yaml` e altere a chave `paths.pedidos` para apontar para um arquivo que não existe.
```yaml
pedidos: "./PATH-INVALIDO/data/input/datasets-csv-pedidos/data/pedidos"

```

Execute o pipeline:
```bash
spark-submit ./data-engineering-pyspark/src/main.py

```

*Observe o log e o encadeamento gracioso de exceções:*

1. **No arquivo de log (`dataeng-pyspark-poo.log`)**, repare como as duas camadas se comunicam:
   * O `DataHandler` registra o erro técnico com a classe do Spark:
     ```text
     ERROR - Erro de análise/metadados no Spark [Classe: PATH_NOT_FOUND]: [PATH_NOT_FOUND] Path does not exist...
     ```
   * O `main.py` captura a exceção de domínio `LoadPedidosException`, e no *traceback* o Python exibe a causa original preservada:
     ```text
     pyspark.errors.exceptions.captured.AnalysisException: [PATH_NOT_FOUND] Path does not exist...

     The above exception was the direct cause of the following exception:

     io_utils.exceptions.LoadPedidosException: Falha ao carregar pedidos a partir de './PATH-INVALIDO/...'
     ```

2. **No terminal**, confira o código de saída retornado ao sistema operacional imediatamente após o comando:
   ```bash
   echo $?

   ```
   > **Saída esperada:** `1`<br>
   > O valor `1` (diferente de zero) comprova que o `sys.exit(1)` sinalizou corretamente ao sistema operacional (e a qualquer orquestrador como Airflow ou Dagster) que o pipeline falhou.

Após o teste, volte a configuração original em `config/settings.yaml`:
```yaml
pedidos: "./data-engineering-pyspark/data/input/datasets-csv-pedidos/data/pedidos/"

```

2. **Teste de Arquivo Corrompido**
```sh
echo "meu arquivo" > ./data-engineering-pyspark/data/input/datasets-csv-pedidos/data/pedidos/corrompido.csv.gz

```

```sh
spark-submit ./data-engineering-pyspark/src/main.py

```

**Atenção!**<br>
Repare onde o erro ocorre. Após as nossas alterações ele **não** acontece em `DataHandler`. Sabe dizer por quê?

Após o teste remova o arquivo corrompido:
```sh
rm ./data-engineering-pyspark/data/input/datasets-csv-pedidos/data/pedidos/corrompido.csv.gz

```


#### Respondendo à pergunta do teste 2

Por que o erro do arquivo corrompido **não** apareceu no `DataHandler`, mesmo com o `try/except` lá dentro?

Por causa da **avaliação preguiçosa**. O `spark.read.csv(...)` não lê nada: ele apenas registra o plano de leitura. O arquivo só é fisicamente aberto quando uma **ação** é disparada — e as ações do nosso pipeline (`show`, `write`, `count`) acontecem depois, dentro de `Transformation` e `Pipeline`. Quando o Spark finalmente tropeça no arquivo corrompido durante a execução física, o `try` do `load_pedidos` já foi encerrado há muito tempo, e quem captura o erro é o `except PySparkException` do `main.py`.

> E o `df.isEmpty()`? Ele *é* uma ação, mas o Spark o resolve com um `take(1)`: lê o mínimo necessário para achar uma linha e para. Se o arquivo corrompido não for o primeiro da lista, ele nem chega a ser tocado.

Lição prática: **`try/except` só protege o que for executado dentro dele**. Em Spark, isso raramente é a leitura — é a ação.

#### Conclusão

Saímos de um pipeline que falhava de forma silenciosa ou ilegível e chegamos a um que falha de forma **previsível, rastreável e limpa**. Duas camadas centrais de defesa foram adicionadas:

| Camada | Onde | O que garante |
|---|---|---|
| `DataHandlerException` / `LoadPedidosException` | `io_utils/exceptions.py` | Desacopla o chamador do Spark; traduz falhas técnicas em exceções de negócio preservando o *traceback* original (`from e`). |
| `try/except/finally` | `main.py` | Última linha de defesa: captura exceções de domínio e do Spark, avisa o orquestrador (`sys.exit(1)`) e garante o `spark.stop()` no `finally`. |

Repare no padrão que usamos no `DataHandler`: **logar e relançar encapsulado em exceção de domínio** (`raise ... from e`).

```python
except AnalysisException as e:
    logger.error(f"Erro de análise/metadados no Spark [Classe: {e.getErrorClass()}]: {e}")
    raise LoadPedidosException(f"Falha ao carregar pedidos em '{path}'") from e

```

Isso não é redundância. Tratar um erro **não** significa escondê-lo: significa registrá-lo com contexto técnico, traduzi-lo para o domínio da aplicação e deixá-lo subir para quem tem autoridade para decidir o que fazer. Um `except` que apenas loga e segue em frente é pior do que nenhum `except` — ele transforma uma falha ruidosa em dado corrompido silencioso, que só será descoberto semanas depois pelo time de negócio.

##### O que levar deste passo

- Encapsule erros de biblioteca em **exceções customizadas de domínio** (`LoadPedidosException`) com `raise ... from e` para manter o código desacoplado e orientado a objetos.
- Capture primeiro exceções **específicas** (`AnalysisException`), depois a base do framework (`PySparkException`) — evite acoplar com exceções de infraestrutura legadas como `Py4JJavaError`.
- Logue **e relance**. `except` não é sinônimo de "ignorar".
- Um job que falhou precisa **terminar com código de saída diferente de zero**, senão o orquestrador acha que deu tudo certo.
- Use `finally` para liberar recursos (a sessão Spark) aconteça o que acontecer.
- Lembre que, em Spark, o erro aparece na **ação**, não na declaração da leitura.

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
  pyspark==4.2.0
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
  pyspark==4.2.0
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

O passo final da jornada de um engenheiro de software é tornar sua aplicação distribuível. Em vez de pedir para que outro engenheiro ou o orquestrador (Airflow, Dagster, Databricks) clone seu repositório Git e rode scripts soltos, vamos empacotar nosso pipeline em um formato padronizado de mercado: o **Wheel (`.whl`)**.

---

### 💡 Por que empacotamos aplicações PySpark em `.whl`?

No desenvolvimento local, seu código roda no mesmo processo. Mas em um ambiente de produção real (AWS EMR, Google Cloud Dataproc, Kubernetes ou Databricks), o Spark opera em uma **arquitetura distribuída**:
* **Driver:** A máquina que orquestra a aplicação e executa o script inicial (`main.py`).
* **Workers (Executores):** Dezenas ou centenas de nós que realizam o processamento pesado e paralelo das partições de dados.

Os nós executores **não possuem seu código instalado localmente nem compartilham seu disco**. Quando passamos nosso código empacotado via `--py-files pacote.whl` no comando `spark-submit`, o Spark distribui automaticamente o pacote binário para todos os executores via rede (*broadcast*), injetando seus módulos no `sys.path` de cada JVM/Python Worker sem a necessidade de instalar nada com `pip` nó por nó.

> [!NOTE]
> **No mercado atual (Databricks Workflows):**  
> Plataformas modernas de dados utilizam diretamente o conceito de **Python Wheel Task**. Você faz o upload do `.whl` gerado pelo seu pipeline de CI/CD para o storage e o orquestrador da nuvem instancia o job diretamente a partir do entrypoint do pacote.

---

### 1. Organizando o Namespace do Pacote (Evitando *Namespace Pollution*)

Até o Passo 11, organizamos nossos módulos (`config`, `io_utils`, `processing`, `session`, `pipeline`) diretamente dentro da pasta `src/`. No desenvolvimento diário, o Python encontra tudo sem problemas.

Contudo, ao construir um pacote distribuível (`.whl`), se deixarmos essas pastas soltas na raiz de `src/`, o instalador do Python (`pip`) as colocaria diretamente na raiz do `site-packages` global. Se outra biblioteca qualquer também possuir um módulo chamado `config` ou `session`, haverá uma colisão de nomes catastrófica (**namespace collision**).

A boa prática consolidada de Engenharia de Software é agrupar todos os módulos sob uma pasta raiz que represente o pacote: **`data_engineering_pyspark`**.

Execute os comandos abaixo para organizar os diretórios:

```bash
# 1. Cria a pasta raiz do pacote com seu arquivo de inicialização
mkdir -p ./data-engineering-pyspark/src/data_engineering_pyspark
touch ./data-engineering-pyspark/src/data_engineering_pyspark/__init__.py

# 2. Move os módulos do projeto para dentro do namespace do pacote
mv ./data-engineering-pyspark/src/config ./data-engineering-pyspark/src/data_engineering_pyspark/
mv ./data-engineering-pyspark/src/io_utils ./data-engineering-pyspark/src/data_engineering_pyspark/
mv ./data-engineering-pyspark/src/processing ./data-engineering-pyspark/src/data_engineering_pyspark/
mv ./data-engineering-pyspark/src/session ./data-engineering-pyspark/src/data_engineering_pyspark/
mv ./data-engineering-pyspark/src/pipeline ./data-engineering-pyspark/src/data_engineering_pyspark/
```

Agora, atualize os imports em `src/main.py` para refletir o novo namespace:

```python
# src/main.py
import sys
import logging
from data_engineering_pyspark.config.settings import carregar_config, configurar_logging
from data_engineering_pyspark.session.spark_session import SparkSessionManager
from data_engineering_pyspark.io_utils.data_handler import DataHandler
from data_engineering_pyspark.processing.transformations import Transformation
from data_engineering_pyspark.pipeline.pipeline import Pipeline
from data_engineering_pyspark.io_utils.exceptions import DataHandlerException, LoadPedidosException
from pyspark.errors import PySparkException

logger = logging.getLogger(__name__)

def main():
    config = carregar_config()
    configurar_logging(config["logging"])

    logger.info("Iniciando a aplicação Spark...")
    spark = SparkSessionManager.get_spark_session(config["spark"]["app_name"])

    try:
        data_handler = DataHandler(spark=spark)
        transformer = Transformation()
        pipeline = Pipeline(data_handler=data_handler, transformer=transformer)
        pipeline.run(config=config)
        logger.info("Pipeline finalizado com sucesso.")

    except LoadPedidosException as e:
        logger.exception(f"Falha no carregamento de pedidos: {e}")
        sys.exit(1)

    except DataHandlerException as e:
        logger.exception(f"Erro na camada de leitura/escrita de dados: {e}")
        sys.exit(1)

    except PySparkException as e:
        logger.exception(f"Erro originado no PySpark [Classe: {e.getErrorClass()}]: {e}")
        sys.exit(1)

    except Exception as e:
        logger.exception(f"Erro inesperado durante a execução do job: {e}")
        sys.exit(1)

    finally:
        spark.stop()
        logger.info("Spark session encerrada.")

if __name__ == "__main__":
    main()
```

Atualize também os imports nos arquivos internos que referenciam outros módulos do projeto:

* Em `src/data_engineering_pyspark/pipeline/pipeline.py`, atualize os imports do `DataHandler` e `Transformation`:
  ```python
  from data_engineering_pyspark.io_utils.data_handler import DataHandler
  from data_engineering_pyspark.processing.transformations import Transformation
  ```
* Em `src/data_engineering_pyspark/io_utils/data_handler.py`, atualize o import da exceção:
  ```python
  from data_engineering_pyspark.io_utils.exceptions import LoadPedidosException
  ```

---

### 2. O Arquivo de Configuração do Pacote: `pyproject.toml` (PEP 517 / 518 / 621)

O `pyproject.toml` é o padrão canônico da comunidade Python para metadados e empacotamento, substituindo os antigos `setup.py` e `setup.cfg`.

Crie ou atualize o arquivo `./data-engineering-pyspark/pyproject.toml`:

```toml
# pyproject.toml
[build-system]
requires = ["setuptools>=61.0"]
build-backend = "setuptools.build_meta"

[project]
name = "data_engineering_pyspark"
version = "0.1.0"
authors = [
  { name="Barbosa", email="infobarbosa@yahoo.com.br" },
]
description = "Pipeline de Engenharia de Dados com PySpark estruturado com boas práticas de Engenharia de Software."
readme = "README.md"
requires-python = ">=3.10"
license = { text = "MIT" }
classifiers = [
    "Programming Language :: Python :: 3",
    "Operating System :: OS Independent",
]
dependencies = [
    "pyspark>=4.2.0,<5.0.0",
    "pyyaml>=6.0.2",
]

[project.optional-dependencies]
dev = [
    "ruff==0.12.9",
    "black==25.1.0",
    "build==1.3.0",
    "pytest==8.4.1",
    "pytest-cov==6.0.0",
]

[project.scripts]
run-data-pipeline = "main:main"

[tool.setuptools.packages.find]
where = ["src"]

[tool.setuptools.package-data]
"*" = ["*.yaml"]

[tool.pytest.ini_options]
pythonpath = ["src", "src/data_engineering_pyspark"]
testpaths = ["tests"]
addopts = "-v"
```

> [!TIP]
> **Por que usar ranges (`>=4.2.0,<5.0.0`) em vez de fixar com `==` no `pyproject.toml`?**  
> Em pacotes distribuíveis, fixar com `==` é uma má prática porque impede que o pacote seja instalado em ambientes que possuam uma versão patch compatível (ex: 4.2.1) e quebra a compatibilidade com ambientes de nuvem. Para congelar versões exatas em ambientes de desenvolvimento e CI, utilizamos o `requirements.txt`.

---

### 3. Crie o arquivo `README.md` do Pacote

Este arquivo documenta o pacote gerado e é exigido pelo build:

```bash
echo "# Data Engineering PySpark" > ./data-engineering-pyspark/README.md
```

---

### 4. Adicione o pacote `build` a `requirements.txt`

Certifique-se de que a ferramenta `build` está instalada no seu ambiente virtual:

```bash
pip install build==1.3.0
```

---

### 5. Construindo o Pacote (`python -m build`)

Com a ferramenta canônica `build` instalada no seu `.venv`, gere a distribuição:

```bash
python -m build ./data-engineering-pyspark
```

Você verá que um diretório `dist/` foi gerado dentro de `data-engineering-pyspark/` contendo:
* **`.whl` (Wheel):** O binário pré-construído pronto para distribuição.
* **`.tar.gz` (Source Distribution - sdist):** O código-fonte compactado com seus metadados.

Verifique os arquivos gerados:
```bash
ls -lh ./data-engineering-pyspark/dist/
```

---

### 6. Executando Diretamente no PySpark via `--py-files`

Agora vamos submeter nossa aplicação ao Spark, fornecendo o pacote Wheel diretamente através da flag `--py-files`:

```bash
spark-submit --master "local[*]" \
  --py-files ./data-engineering-pyspark/dist/data_engineering_pyspark-0.1.0-py3-none-any.whl \
  ./data-engineering-pyspark/src/main.py
```

> [!TIP]
> **Cadê o `pip install`?**  
> Repare que **não** precisamos executar `pip install` no ambiente local antes de rodar o `spark-submit`! Essa é justamente a vantagem do `--py-files`: em vez de depender de instalações locais prévias, o Spark injeta o arquivo `.whl` dinamicamente no Driver e em todos os nós Executores do cluster.

---

> [!NOTE]
> **🚀 No Radar do Mercado: `uv` (Astral)**  
> Nos times mais modernos de engenharia de dados (2024–2026), a ferramenta **`uv`** (escrita em Rust pela Astral) tem se tornado o padrão do ecossistema Python. Ela substitui `pip`, `virtualenv`, `pip-tools` e `build` com velocidade até 100x superior. Em pipelines CI/CD com `uv`, o build é tão simples quanto executar `uv build`.

## Passo 13: Testes Automatizados

Até agora, construímos uma aplicação robusta, bem estruturada e distribuível. Mas como garantir que a lógica de negócio — o coração da aplicação — está correta e **continuará** correta conforme o projeto evolui? A resposta é: **testes automatizados**.

Uma boa suíte de testes nos dá:
- **Validação da Correção:** garante que cálculos e regras de negócio se comportam exatamente como o esperado.
- **Proteção contra Regressões:** se uma alteração futura quebrar algo, o teste falha e avisa imediatamente.
- **Confiança para Refatorar:** você melhora o código sabendo que não introduziu bugs.
- **Documentação Viva:** um bom teste descreve, em código executável, qual o comportamento esperado de cada componente.

### 13-A. A Pirâmide de Testes

Nem todo teste é igual. Vamos organizar nossa suíte em duas camadas:

- **Testes Unitários** — verificam **uma unidade isolada** (um método, uma função), sem I/O externo. São muitos, rápidos e baratos. Ex.: a classe `Transformation`, que contém lógica pura.
- **Testes de Integração** — verificam se os componentes **cooperam corretamente** (a orquestração do `Pipeline`, a leitura/escrita real em disco). São menos numerosos e mais lentos.

> A base da pirâmide é larga (muitos testes unitários, rápidos) e o topo é estreito (poucos testes de integração, lentos). Essa proporção mantém a suíte ágil sem abrir mão da confiança de que "as peças se encaixam".

### 13-B. Adicione as dependências de teste

`pytest` é o framework de testes mais popular do Python, e o `pytest-cov` mede a **cobertura** (quanto do código é exercitado pelos testes).

- Atualize o `requirements.txt`:
  ```
  # requirements.txt
  pyspark==4.2.0
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

### 13-C. Crie a estrutura de testes

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
  ├── pyproject.toml              # config do projeto e do pytest
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

### 13-D. Configure o pytest (no `pyproject.toml`)

Sem configuração, o `import` das nossas classes falharia, porque o código fica em `src/`. Em vez de criar um novo arquivo, centralizamos a configuração no `pyproject.toml` que você configurou no Passo 12.

- Se ainda não adicionou no Passo 12, edite o `pyproject.toml` e adicione ao final a seção `[tool.pytest.ini_options]`:

  ```toml
  # pyproject.toml (adicione ao final se ainda não estiver presente)
  [tool.pytest.ini_options]
  pythonpath = ["src", "src/data_engineering_pyspark"]
  testpaths = ["tests"]
  markers = [
      "unit: Testes unitários isolados (sem I/O externo)",
      "integration: Testes de integração (orquestração entre componentes)",
  ]
  addopts = "-v"
  ```

O que cada opção faz:
- **`pythonpath`** — adiciona `src/` e `src/data_engineering_pyspark/` ao caminho de import. Isso permite flexibilidade total: você pode importar tanto pelo namespace completo (`from data_engineering_pyspark.processing.transformations import Transformation`) quanto pelo formato direto (`from processing.transformations import Transformation`), garantindo compatibilidade contínua.
- **`testpaths`** — onde o pytest procura testes.
- **`markers`** — rótulos para categorizar testes (ex.: rodar só os unitários com `pytest -m unit`).
- **`addopts`** — opções sempre aplicadas (aqui, saída detalhada).

### 13-E. Centralize a `SparkSession` no `conftest.py`

Criar uma `SparkSession` é **caro**. Não queremos pagar esse custo em cada teste. O pytest tem um arquivo especial, o `conftest.py`, cujas *fixtures* ficam disponíveis automaticamente para **todos** os testes — sem precisar importar.

- Crie o arquivo `./data-engineering-pyspark/tests/conftest.py`:

  ```bash
  touch ./data-engineering-pyspark/tests/conftest.py

  ```

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

### 13-F. A anatomia de um teste: Arrange, Act, Assert

Todo teste que escreveremos segue três passos:

1. **Arrange (Preparar):** monte os dados de entrada e o resultado esperado.
2. **Act (Agir):** execute a função/método sob teste.
3. **Assert (Verificar):** compare o resultado obtido com o esperado.

Com a fundação pronta, vamos escrever os testes camada por camada.

### 13-G. Testes unitários da `Transformation` (a lógica de negócio)

Este é o arquivo **mais crítico**: as transformações contêm as regras de negócio. Um erro aqui corromperia silenciosamente todos os resultados. Como é lógica pura, criamos os DataFrames *inline* (sem I/O) — máxima velocidade e isolamento.

Repare em dois pontos importantes:
- Agrupamos os testes em **classes** (`TestAddValorTotalPedidos`, ...) para organizar por método testado.
- Cada teste cobre **um comportamento específico**, incluindo **casos de borda** (nulos, zero, menos de 10 clientes), e a docstring explica *por que* aquele caso importa.

- Crie o arquivo `./data-engineering-pyspark/tests/unit/test_transformations.py`:

  ```bash
  touch ./data-engineering-pyspark/tests/unit/test_transformations.py

  ```

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

### 13-H. Testes unitários do `DataHandler` (I/O com arquivos temporários)

O `DataHandler` lê e escreve arquivos. Mas **não** queremos depender dos datasets reais (grandes e externos). A fixture `tmp_path` do pytest cria um diretório temporário, único por teste e apagado automaticamente — nele geramos arquivos minúsculos de propósito.

- Crie o arquivo `./data-engineering-pyspark/tests/unit/test_data_handler.py`:

  ```bash
  touch ./data-engineering-pyspark/tests/unit/test_data_handler.py

  ```

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

### 13-I. Testes unitários de `carregar_config` (sem Spark)

Estes são os testes **mais rápidos** da suíte: validam apenas a leitura do YAML e nem precisam de Spark. Aqui também testamos o **caminho de erro** (arquivo inexistente).

- Crie o arquivo `./data-engineering-pyspark/tests/unit/test_settings.py`:

  ```bash
  touch ./data-engineering-pyspark/tests/unit/test_settings.py

  ```

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

### 13-J. Testes unitários do `SparkSessionManager` (contrato de Singleton)

Aqui verificamos o **contrato público** da classe: retornar uma `SparkSession` válida e **reutilizar** a sessão existente (comportamento de Singleton via `getOrCreate`).

- Crie o arquivo `./data-engineering-pyspark/tests/unit/test_spark_session.py`:

  ```bash
  touch ./data-engineering-pyspark/tests/unit/test_spark_session.py

  ```

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

### 13-K. Testes de integração do `Pipeline`

Lembra do [Passo 7](#passo-7-injeção-de-dependências), onde injetamos `DataHandler` e `Transformation` no `Pipeline`? **Agora colhemos o benefício.** Faremos dois estilos complementares:

1. **Orquestração (com *mock*):** substituímos o `DataHandler` por um objeto falso (`MagicMock`) e verificamos *se* e *como* o `Pipeline` chama suas dependências — sem tocar no disco.
2. **End-to-end (sem *mock*):** rodamos o pipeline inteiro com dados reais pequenos e conferimos o Parquet de saída.

- Crie o arquivo `./data-engineering-pyspark/tests/integration/test_pipeline.py`:

  ```bash
  touch ./data-engineering-pyspark/tests/integration/test_pipeline.py

  ```

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

### 13-L. Executando os testes

A partir da pasta anterior (a que contém o diretório `data-engineering-pyspark/`), sem precisar entrar nele:

  ```bash
  pytest ./data-engineering-pyspark

  ```

Passamos o caminho do projeto como argumento para que o pytest **encontre o `pyproject.toml`** e aplique o `pythonpath`. Rodar `pytest` sozinho, a partir da pasta anterior, faria o pytest procurar a configuração apenas "para cima" e não a encontraria — causando erros de import.

A saída lista cada teste (graças ao `-v` do `addopts`):

  ```
  ============================= test session starts ==============================
  collected 25 items

  tests/integration/test_pipeline.py::TestPipelineOrquestracao::test_le_clientes_com_path_da_config PASSED
  ...
  tests/unit/test_transformations.py::TestAddValorTotalPedidos::test_calcula_valor_unitario_por_quantidade PASSED
  ...
  ============================== 25 passed in 9.87s ==============================
  ```

Para rodar **apenas** uma camada, selecione pelo diretório:

  ```bash
  pytest ./data-engineering-pyspark/tests/unit          # só os testes unitários (rápidos)
  pytest ./data-engineering-pyspark/tests/integration   # só os testes de integração

  ```

> Os marcadores declarados no `pyproject.toml` (seção `[tool.pytest.ini_options]`) permitem filtrar com `pytest -m unit`. Para usá-los, marque os testes — por exemplo, adicionando no topo de cada arquivo unitário a linha `pytestmark = pytest.mark.unit` (e `pytestmark = pytest.mark.integration` no arquivo de integração).

### 13-M. Medindo a cobertura de código

Cobertura indica **quais linhas do código foram exercitadas** pelos testes. É um termômetro útil: embora 100% de cobertura não garanta ausência de bugs, áreas com cobertura baixa são pontos cegos.

Rode com o `pytest-cov`, apontando para o código em `./data-engineering-pyspark/src`:

  ```bash
  pytest ./data-engineering-pyspark --cov=./data-engineering-pyspark/src --cov-report=term-missing

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
  pytest ./data-engineering-pyspark --cov=./data-engineering-pyspark/src --cov-report=html
  # abra htmlcov/index.html no navegador

  ```

> **Cuidado com a métrica:** busque cobrir os **caminhos críticos e os casos de borda** (foi o que fizemos), não perseguir 100% a qualquer custo. Um teste que executa o código mas não verifica nada (sem `assert`) aumenta a cobertura sem proteger contra nada.

### 13-N. Recapitulando

Agora temos uma suíte completa que cobre todas as camadas da aplicação:

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

---

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
  - UTILIZAR a estrutura `try/except` para tratamento de erros na classe de lógica de negócios.
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

## Referências

### Livros
- **Spark: The Definitive Guide** (Bill Chambers & Matei Zaharia): O guia definitivo para entender a fundo o motor do Apache Spark.
- **Learning Spark: Lightning-Fast Data Analytics** (Jules S. Damji et al.): Ótimo para quem está começando e foca nas APIs mais modernas.
- **Clean Code: A Handbook of Agile Software Craftsmanship** (Robert C. Martin): Leitura fundamental para as partes de refatoração, organização e qualidade de código.
- **Engenharia de Software Moderna** (Marco Tulio Valente): Referência excelente em português sobre princípios de engenharia de software e POO.

### Documentação Oficial
- [Apache Spark - Documentação Oficial](https://spark.apache.org/docs/latest/): Referência primária para configurações e APIs.
- [PySpark - Referência da API](https://spark.apache.org/docs/latest/api/python/): Detalhes sobre módulos, classes e funções do PySpark.
- [Documentação Oficial do Python](https://docs.python.org/3/): Para guias sobre POO, Exceptions, Logging, entre outros.
- [Documentação do Pytest](https://docs.pytest.org/): Guia completo para a criação e organização de testes automatizados em Python.

### Ferramentas e Bibliotecas
- [Black (Formatador de Código)](https://black.readthedocs.io/): O formatador de código Python rigoroso (uncompromising code formatter).
- [Flake8 (Linter)](https://flake8.pycqa.org/): Ferramenta para verificação de estilo de código e identificação de erros de sintaxe.
- [Poetry (Gestão de Dependências)](https://python-poetry.org/): Excelente para gestão de pacotes, dependências e ambientes virtuais em Python.

### Artigos e Tutoriais
- [Databricks Engineering Blog](https://www.databricks.com/blog/category/engineering): Artigos aprofundados sobre arquitetura de dados, boas práticas e novidades do mundo Spark.
- [PEP 8 – Style Guide for Python Code](https://peps.python.org/pep-0008/): O guia de estilo de código Python, muito útil na etapa de refatoração e qualidade de código.
