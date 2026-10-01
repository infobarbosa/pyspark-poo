# Exemplo: Spark + Kafka (streaming)

> Exemplo de apoio ao tutorial [Engenharia de Software com PySpark](../README.md).
> **Não faz parte dos artefatos que o aluno constrói** — serve para demonstrar que a
> arquitetura do tutorial sobrevive até quando os dados chegam em movimento.

---

## O que este exemplo mostra

Compare `src/main.py` deste exemplo com o do [exemplo JDBC](../exemplo-jdbc/src/main.py).
São praticamente o mesmo arquivo: carrega config, configura logging, cria a sessão, monta
as dependências, trata a falha, encerra no `finally`.

Só duas coisas mudaram:

1. A classe de fronteira agora é `KafkaHandler`.
2. O job **não termina** — ele fica no ar (`awaitTermination`).

A regra de negócio, `adicionar_valor_total()`, é literalmente a mesma função do tutorial.
Ela **não sabe** se o DataFrame veio de um `.csv.gz`, de uma tabela Postgres ou de um
tópico Kafka. Esse é o ponto do capítulo inteiro.

---

## Anatomia

A estrutura é a **mesma do tutorial** e a mesma do [exemplo JDBC](../exemplo-jdbc/README.md) —
o que é justamente o argumento do capítulo.

```
exemplo-kafka/
├── docker-compose.yml         # broker único em modo KRaft (sem Zookeeper)
├── requirements.txt           # pyspark e pyyaml — o conector NÃO está aqui
├── settings.yaml              # broker, tópico, offsets, checkpoint, trigger
└── src/
    ├── settings.py            # carrega o YAML
    ├── spark_session.py       # SparkSessionManager — SÓ cria a sessão
    ├── kafka_handler.py       # KafkaHandler — a única classe que fala com o broker
    ├── transformations.py     # Transformation — regra de negócio pura
    ├── pipeline.py            # Pipeline — orquestra, devolve a StreamingQuery
    └── main.py                # Composition Root — só monta e dispara
```

| Tutorial | Aqui | Responsabilidade |
|---|---|---|
| `config/settings.py` | `settings.py` | configuração |
| `session/spark_session.py` | `spark_session.py` | **criar a sessão** |
| `io_utils/data_handler.py` | `kafka_handler.py` | **manipular os dados** |
| `processing/transformations.py` | `transformations.py` | regra de negócio |
| `pipeline/pipeline.py` | `pipeline.py` | orquestração |
| `main.py` | `main.py` | raiz de composição |

**Exercício de leitura para a turma:** abra `spark_session.py` deste exemplo e o do exemplo
JDBC lado a lado. São o mesmo arquivo — muda só a coordenada Maven que chega por parâmetro.
Depois faça o mesmo com `transformations.py`: o `add_valor_total_pedidos` é idêntico ao do
tutorial, linha por linha. **A fronteira externa mudou inteira; o miolo não mudou nada.**

> Repare também na divisão de responsabilidades dentro do Kafka: carregar o **conector** é
> assunto da *sessão* (classpath da JVM); assinar o **tópico** é assunto do *handler*.

---

## Como executar

**1. Suba o broker:**

```bash
docker compose -f exemplo-kafka/docker-compose.yml up -d
```

**2. Crie o tópico:**

```bash
docker exec exemplo-kafka-broker /opt/kafka/bin/kafka-topics.sh \
  --create --topic pedidos --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1
```

> Três partições de propósito: mostra o paralelismo do consumo no Spark.

**3. Rode a aplicação** (em um terminal, e deixe rodando):

```bash
pip install -r exemplo-kafka/requirements.txt
spark-submit exemplo-kafka/src/main.py
```

**4. Produza mensagens** (em outro terminal):

```bash
docker exec -i exemplo-kafka-broker /opt/kafka/bin/kafka-console-producer.sh \
  --topic pedidos --bootstrap-server localhost:9092 <<'JSON'
{"id_pedido":"a1","produto":"NOTEBOOK","valor_unitario":1500.0,"quantidade":2,"data_criacao":"2026-01-17T15:28:57","uf":"MG","id_cliente":5872}
{"id_pedido":"a2","produto":"CELULAR","valor_unitario":1000.0,"quantidade":3,"data_criacao":"2026-01-01T11:58:48","uf":"DF","id_cliente":934}
{"id_pedido":"a3","produto":"GELADEIRA","valor_unitario":2000.0,"quantidade":1,"data_criacao":"2026-01-27T13:37:31","uf":"MA","id_cliente":174}
isto-aqui-nao-e-json
JSON
```

> A última linha é proposital — veja a consideração nº 4.

**5. Confira a saída** (após o próximo trigger, ~10s):

```bash
ls exemplo-kafka/saida/pedidos/
```

**6. Derrube tudo:**

```bash
docker compose -f exemplo-kafka/docker-compose.yml down -v
rm -rf exemplo-kafka/saida
```

---

## Considerações para discutir em aula

### 1. O conector do Kafka também é dependência da JVM

Mesma lição do exemplo JDBC, e vale repetir porque é onde a turma mais trava:

```yaml
jars_packages: "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"
```

Leia essa coordenada em voz alta com os alunos, ela tem três informações:

| Parte | Significado |
|---|---|
| `spark-sql-kafka-0-10` | conector para Kafka 0.10+ (praticamente qualquer versão atual) |
| `_2.13` | versão do **Scala** com que seu Spark foi compilado |
| `:4.1.1` | versão do **Spark** — precisa bater exatamente |

Errar qualquer um dos três produz o erro mais famoso do Spark Streaming:
`Failed to find data source: kafka`. Não é bug: é dependência ausente ou incompatível.

### 2. O Kafka não entrega JSON — entrega bytes

O DataFrame que sai do `readStream` **nunca** tem o formato da sua mensagem. Ele sempre
tem este schema fixo:

```
key (binary) | value (binary) | topic | partition | offset | timestamp | timestampType
```

Sua mensagem está em `value`, como **binário**. Por isso o handler faz:

```python
F.from_json(F.col("value").cast("string"), self._schema_pedido())
```

Vale mostrar na tela um `.printSchema()` antes e depois — o "antes" costuma surpreender.

E repare no que veio junto: `offset`, `partition` e `timestamp`. Esses metadados são ouro
para depuração e rastreabilidade, e o exemplo carrega `kafka_offset` e `kafka_timestamp`
até a saída de propósito.

### 3. Em streaming, schema explícito não é boa prática — é obrigação

Não existe `inferSchema` em streaming, e o motivo é óbvio quando dito em voz alta:
**o Spark não pode inspecionar dados que ainda não chegaram.**

O Passo 1 do tutorial (aquele do `cod_bonus` "0101" que virou 101) deixa de ser um
conselho e vira requisito técnico. O schema é o **contrato** entre quem produz e quem
consome — e mudanças nele quebram o consumidor silenciosamente. Bom momento para
mencionar *Schema Registry*, mesmo sem entrar em detalhe.

### 4. Mensagem malformada não explode: ela vira `null`

Essa é a armadilha mais perigosa do exemplo, e por isso a linha `isto-aqui-nao-e-json`
está no roteiro de execução.

O `from_json` **não lança exceção** com JSON inválido: ele devolve um struct nulo. Se
ninguém tratar, você grava linhas vazias no destino e o job segue reportando sucesso.

```python
.filter(F.col("pedido").isNotNull())
```

Em produção, esse filtro não jogaria a mensagem fora — ela iria para uma **dead letter
queue** (outro tópico, ou uma pasta de quarentena), com o offset original, para análise
posterior. Descartar dado ruim em silêncio é pior do que falhar.

> **Gancho com o Passo 9 (Tratamento de Erros):** nem toda falha vem como exceção.
> Às vezes ela chega como um `null` bem-comportado.

### 5. O checkpoint é o estado da sua aplicação

```yaml
checkpoint: "./exemplo-kafka/saida/_checkpoint/pedidos"
```

É o que separa um script de uma aplicação resiliente. Nessa pasta o Spark guarda quais
offsets já foram processados. Se o job cair e subir de novo, ele **retoma exatamente de
onde parou** — não reprocessa, não perde.

Demonstração que vale ouro em aula:

1. Rode, produza 3 mensagens, veja a saída.
2. Derrube com `Ctrl+C`.
3. Produza mais 2 mensagens com o job **fora do ar**.
4. Suba de novo — só as 2 novas são processadas.

Consequências práticas que a turma precisa levar:

- Apagar o checkpoint = reprocessar o tópico inteiro (ou perder dados, dependendo de
  `startingOffsets`). **Não é uma pasta temporária.**
- `startingOffsets: earliest` só vale na **primeira** execução. Depois, quem manda é o
  checkpoint — e isso confunde muita gente ("mudei o YAML e nada aconteceu").
- Cada query precisa do **seu próprio** checkpoint. Duas queries apontando para a mesma
  pasta corrompem o estado das duas.

### 6. Controle o tamanho do micro-batch

```yaml
max_offsets_per_trigger: 1000
```

Sem isso, o primeiro batch de um tópico com backlog de milhões de mensagens tenta engolir
tudo de uma vez — e o executor morre por falta de memória. Esse parâmetro é o
equivalente, no mundo do streaming, a paginar uma consulta.

### 7. Nem tudo que funciona em batch funciona em streaming

O exemplo faz só um `withColumn` — e isso é proposital. Um `groupBy().agg()` como o do
tutorial **não roda** com sink de arquivo em modo `append`, porque um agregado sobre um
stream infinito nunca fica "pronto".

Para agregar em streaming você precisa de duas peças a mais:

- **watermark** — a promessa de até quanto tempo você espera por dados atrasados;
- um **sink que suporte atualização** (`complete`/`update`), como console, memória, Kafka
  ou uma tabela transacional (Delta/Iceberg).

Não precisa aprofundar em aula. Basta plantar a ideia: **tempo é uma dimensão nova**, e
ela muda o que é possível calcular.

### 8. Batch e streaming, a mesma fronteira

O `KafkaHandler` traz um método `ler_batch()` no final, de propósito. Ele lê uma faixa
fechada de offsets (`earliest` → `latest`), como se o tópico fosse um arquivo — útil para
reprocessamento histórico.

Compare os dois métodos lado a lado: muda `readStream` por `read` e acrescenta
`endingOffsets`. **A transformação de negócio não muda uma vírgula.** É o argumento mais
forte a favor de isolar I/O em uma classe própria.

---

## O que isso tem a ver com o tutorial

| Tutorial (arquivos) | Este exemplo (Kafka) |
|---|---|
| `SparkSessionManager` (Passo 3) | idêntico, + o conector no classpath |
| `DataHandler` (Passo 4) | `KafkaHandler` |
| `Transformation` (Passo 5) | **idêntica, o mesmo método** |
| `Pipeline` com DI (Passo 7) | idêntico, mas `run()` devolve a query |
| Composition Root em `main.py` | idêntico, + `awaitTermination()` |
| Logging (Passo 8) | idêntico |
| `try/except/finally` (Passo 9) | idêntico, + `KeyboardInterrupt` |
| Schema explícito (Passo 1) | obrigatório, não opcional |

Arquivo, banco ou tópico: **configuração externalizada, credencial fora do código, uma
classe só para I/O, dependências injetadas e falha tratada na fronteira.** Só muda o
`format()`.
