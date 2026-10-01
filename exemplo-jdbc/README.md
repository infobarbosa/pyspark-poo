# Exemplo: Spark + JDBC (banco relacional)

> Exemplo de apoio ao tutorial [Engenharia de Software com PySpark](../README.md).
> **Não faz parte dos artefatos que o aluno constrói** — serve para demonstrar que a
> arquitetura do tutorial não muda quando a fonte de dados deixa de ser um arquivo.

---

## O que este exemplo mostra

No tutorial, a classe `DataHandler` isola a aplicação do sistema de arquivos.
Aqui, a classe `JDBCHandler` isola a aplicação de um **banco relacional**.

O restante — configuração externalizada, `SparkSession` gerenciada, injeção de
dependência, logging, `try/except/finally` — é **idêntico**. Essa é a mensagem:

> A arquitetura não muda com a tecnologia. O que muda é apenas o `format()`.

A diferença real é que um banco é um **recurso compartilhado e vivo**: ele tem dono,
tem limite de conexões, tem senha e pode sair do ar no meio do job. É disso que trata
este exemplo.

---

## Anatomia

A estrutura é a **mesma do tutorial**, módulo a módulo — só os nomes dos arquivos são
planos em vez de pacotes, para manter o exemplo curto.

```
exemplo-jdbc/
├── docker-compose.yml         # Postgres de apoio para a demonstração
├── requirements.txt           # pyspark e pyyaml — o driver JDBC NÃO está aqui
├── settings.yaml              # tudo sobre a conexão, menos a senha
└── src/
    ├── settings.py            # carrega o YAML e resolve o segredo
    ├── spark_session.py       # SparkSessionManager — SÓ cria a sessão
    ├── jdbc_handler.py        # JDBCHandler — a única classe que sabe o que é JDBC
    ├── transformations.py     # Transformation — regra de negócio pura
    ├── pipeline.py            # Pipeline — orquestra, recebe as dependências prontas
    └── main.py                # Composition Root — só monta e dispara
```

| Tutorial | Aqui | Responsabilidade |
|---|---|---|
| `config/settings.py` | `settings.py` | configuração e segredos |
| `session/spark_session.py` | `spark_session.py` | **criar a sessão** |
| `io_utils/data_handler.py` | `jdbc_handler.py` | **manipular os dados** |
| `processing/transformations.py` | `transformations.py` | regra de negócio |
| `pipeline/pipeline.py` | `pipeline.py` | orquestração |
| `main.py` | `main.py` | raiz de composição |

> A separação entre `spark_session.py` e `jdbc_handler.py` é intencional e vale apontar:
> carregar o **driver JDBC** é assunto da *sessão* (é classpath da JVM); abrir a
> **conexão** é assunto do *handler*. Duas responsabilidades, dois arquivos.

---

## Como executar

**1. Defina a senha no ambiente** (o container e a aplicação leem a mesma variável):

```bash
export JDBC_PASSWORD='senha-da-aula'
```

**2. Suba o banco:**

```bash
docker compose -f exemplo-jdbc/docker-compose.yml up -d
```

**3. Crie e popule a tabela de origem:**

```bash
docker exec -i exemplo-jdbc-postgres psql -U loja_app -d loja <<'SQL'
CREATE TABLE pedidos (
    id_pedido      TEXT PRIMARY KEY,
    id_cliente     BIGINT,
    produto        TEXT,
    valor_unitario NUMERIC(10,2),
    quantidade     BIGINT,
    uf             CHAR(2)
);

INSERT INTO pedidos
SELECT
    md5(g::text),
    (random() * 9999 + 1)::bigint,
    (ARRAY['NOTEBOOK','CELULAR','GELADEIRA','LIQUIDIFICADOR'])[floor(random()*4+1)],
    (random() * 2000 + 100)::numeric(10,2),
    (random() * 5 + 1)::bigint,
    (ARRAY['MG','SP','RJ','DF'])[floor(random()*4+1)]
FROM generate_series(1, 50000) g;
SQL
```

**4. Instale as dependências e execute:**

```bash
pip install -r exemplo-jdbc/requirements.txt
spark-submit exemplo-jdbc/src/main.py
```

> Na primeira execução o Spark baixa o driver do Maven Central — pode demorar alguns
> segundos. Se a máquina estiver sem internet, use `--jars caminho/postgresql.jar`.

**5. Confira o resultado no banco:**

```bash
docker exec -it exemplo-jdbc-postgres \
  psql -U loja_app -d loja -c 'SELECT * FROM relatorio_top_10_clientes ORDER BY valor_total DESC;'
```

**6. Derrube tudo:**

```bash
docker compose -f exemplo-jdbc/docker-compose.yml down -v
```

---

## Considerações para discutir em aula

### 1. O driver JDBC não é uma dependência Python

Este é o mal-entendido mais comum da turma. `pip install psycopg2` **não resolve nada**
aqui: quem abre a conexão é a **JVM**, não o Python. O driver é um `.jar`.

No exemplo ele está declarado em `settings.yaml` e aplicado na construção da sessão:

```python
.config("spark.jars.packages", "org.postgresql:postgresql:42.7.4")
```

Equivale a `spark-submit --packages org.postgresql:postgresql:42.7.4`, mas fica
**versionado junto da configuração** em vez de escondido em um script de deploy.

> **Gancho com o Passo 10 (Gestão de Dependências):** uma aplicação Spark tem *duas*
> árvores de dependência — a do Python (`requirements.txt`) e a da JVM (`--packages`).
> As duas precisam ser fixadas e versionadas.

### 2. A senha nunca está no código, nem no YAML, nem no git

O `settings.yaml` guarda apenas o **nome da variável de ambiente**:

```yaml
senha_env: "JDBC_PASSWORD"
```

E `main.py` falha imediatamente, com mensagem clara, se ela não existir. Duas coisas
importantes nesse desenho:

- O erro acontece **antes** de subir a sessão Spark — falhar cedo e barato.
- Trocar variável de ambiente por um cofre de segredos (Secrets Manager, Vault) mexe em
  **uma única função**, `obter_senha()`. O resto da aplicação não muda.

> **Pergunte à turma:** onde essa senha apareceria se ela estivesse hardcoded? No git, no
> log do `spark-submit`, no histórico do shell e na UI do Spark. Quatro vazamentos.

### 3. Sem particionamento, seu cluster inteiro vira um computador só

Este é o ponto de maior impacto prático do exemplo.

```python
spark.read.format("jdbc").option("dbtable", "pedidos").load()
```

Esse código **funciona** — e é péssimo. O Spark abre **uma** conexão, traz tudo em
**uma** partição, para **um** executor. Os outros 19 nós do cluster ficam assistindo.

A correção está em `settings.yaml`:

```yaml
particionamento:
  partition_column: "id_cliente"
  lower_bound: 1
  upper_bound: 10000
  num_partitions: 4
```

O Spark quebra a consulta em 4 faixas de `id_cliente` e dispara 4 `SELECT` simultâneos.
O `main.py` imprime `pedidos.rdd.getNumPartitions()` justamente para a turma **ver** o
número mudar de 1 para 4.

Três armadilhas que valem o alerta:

- `num_partitions` são **conexões reais e simultâneas** ao banco. Colocar 200 ali é uma
  forma eficiente de derrubar o banco de produção — e de receber uma ligação do DBA.
- A coluna precisa ser **bem distribuída**. Se 90% dos `id_cliente` estiverem na primeira
  faixa, você trocou uma partição gigante por uma partição gigante e três vazias (*skew*).
- `lower_bound` e `upper_bound` **não filtram nada** — servem só para calcular as faixas.
  Valores fora desse intervalo continuam sendo lidos, nas partições das pontas.

### 4. Deixe o banco fazer o que o banco faz bem (pushdown)

Repare que não lemos a tabela: mandamos uma **subquery**.

```yaml
dbtable: "(SELECT id_pedido, id_cliente, ... FROM pedidos WHERE uf = 'MG') AS pedidos_mg"
```

O filtro e a seleção de colunas rodam **dentro do Postgres**, que tem índice e estatística
para isso. Trafega pela rede só o que interessa. A alternativa ingênua — ler tudo e fazer
`.filter()` no Spark — transporta a tabela inteira para depois jogar fora 75% dela.

> Regra prática: **em JDBC, filtre o mais cedo possível — e o mais cedo possível é no banco.**

### 5. `mode("overwrite")` faz `DROP TABLE` por padrão

Aqui vale parar a aula. O comportamento padrão do Spark com `overwrite` é
`DROP TABLE` + `CREATE TABLE`. Você perde índices, constraints, *foreign keys*,
permissões e comentários — e a tabela é recriada com os tipos que o Spark achar melhor.

Por isso o exemplo usa:

```yaml
truncate: true    # apaga as linhas, preserva a estrutura
```

### 6. A falha é tratada na fronteira, não espalhada pelo código

O `JDBCHandler` separa dois tipos de erro, seguindo o Passo 9 do tutorial:

| Exceção | O que costuma ser | Nível |
|---|---|---|
| `AnalysisException` | tabela/coluna inexistente, schema incompatível | `error` |
| `Py4JJavaError` | banco fora do ar, senha errada, driver ausente, deadlock | `critical` |

O handler **loga com contexto e relança**. Quem decide o destino do processo é o
`main.py`, com `sys.exit(1)` — e o `finally` garante que a sessão Spark morre nos dois
caminhos. Engolir exceção em pipeline de dados é como desligar o alarme de incêndio:
o job "passa" e entrega dado errado.

---

## O que isso tem a ver com o tutorial

| Tutorial (arquivos) | Este exemplo (JDBC) |
|---|---|
| `SparkSessionManager` (Passo 3) | idêntico, + o driver JDBC no classpath |
| `DataHandler` (Passo 4) | `JDBCHandler` |
| `Transformation` (Passo 5) | idêntica — os métodos têm os **mesmos nomes** |
| `Pipeline` com DI (Passo 7) | idêntico |
| Composition Root em `main.py` | idêntico |
| Logging (Passo 8) | idêntico |
| `try/except/finally` (Passo 9) | idêntico |
| Schema explícito (Passo 1) | vem do catálogo do banco |

A **única** peça realmente nova é a natureza do recurso: um banco tem credencial, limite
de conexões e um DBA do outro lado. Tudo o mais que você aprendeu continua valendo.
