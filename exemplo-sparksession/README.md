# Exemplo: anatomia de uma SparkSession

> Exemplo de apoio ao tutorial [Engenharia de Software com PySpark](../README.md).
> **Não faz parte dos artefatos que o aluno constrói.** É a versão "sob o capô" do
> [Passo 3](../README.md#passo-3-gerenciando-a-sessão-spark), onde o `SparkSessionManager`
> aparece com três linhas.

---

## O que este exemplo mostra

No tutorial, a sessão nasce assim:

```python
SparkSession.builder.appName(app_name).master("local[*]").getOrCreate()
```

Isso está **correto** — e esconde umas quarenta decisões que alguém já tomou por você.
Este exemplo abre a caixa: quais parâmetros existem, o que cada um resolve, e quais
deles **mudam o resultado do seu job**, não só a velocidade.

Três ideias para a turma levar daqui:

1. **`getOrCreate()` não é uma formalidade.** É onde você declara quanta máquina usar,
   como paralelizar, quais dependências carregar e em que fuso horário interpretar datas.
2. **Nem todo parâmetro tem efeito no lugar onde você o escreveu.** Alguns são aceitos
   e ignorados em silêncio — o exemplo prova isso com número na tela.
3. **Configuração não é código.** O mesmo job roda no laptop e no cluster; o que muda
   é o perfil no YAML.

---

## Anatomia

```
exemplo-sparksession/
├── requirements.txt
├── settings.yaml          # dois perfis: `local` (roda) e `cluster` (referência)
└── src/
    ├── settings.py        # carrega o YAML e escolhe o perfil
    ├── spark_session.py   # SÓ a criação da sessão — em duas versões, ver abaixo
    ├── inspecao.py        # o trabalho sobre a sessão: relatórios e armadilhas
    ├── main.py            # Composition Root — só monta e dispara
    └── demo_shuffle.py    # demonstração isolada, com cronômetro
```

| Tutorial | Aqui | Responsabilidade |
|---|---|---|
| `config/settings.py` | `settings.py` | configuração |
| `session/spark_session.py` | `spark_session.py` | **criar a sessão** |
| `io_utils/data_handler.py` | `inspecao.py` | **trabalhar sobre a sessão** |
| `main.py` | `main.py` | raiz de composição |

O `main.py` tem 60 linhas e não sabe fazer nada sozinho — é o objetivo. Quem cria a
sessão é `spark_session.py`; quem trabalha sobre ela é `inspecao.py`.

### Por que `spark_session.py` tem dois builders

| Função | Para quê |
|---|---|
| `criar_sessao_didatica()` | O builder escrito na mão, ~20 `.config()` com um comentário cada. **É o arquivo que você projeta na tela.** |
| `SparkSessionManager.criar(perfil)` | O builder alimentado pelo YAML. **É o que vai para produção** — e é a evolução natural do Passo 3. |

Os dois produzem a mesma sessão. A diferença é **onde mora a configuração** — e essa é
a discussão de engenharia de software, não de Spark.

---

## Como executar

```bash
pip install -r exemplo-sparksession/requirements.txt

# 1) Relatório da configuração efetiva + as armadilhas
spark-submit exemplo-sparksession/src/main.py

# 2) Lista o perfil de cluster (não tenta conectar: não há YARN no laptop)
spark-submit exemplo-sparksession/src/main.py cluster

# 3) A demonstração medida do shuffle
spark-submit exemplo-sparksession/src/demo_shuffle.py
```

Limpeza: `rm -rf exemplo-sparksession/saida`

---

## Os parâmetros, por tema

### 1. Identidade — quem é este job e onde ele roda

| Parâmetro | Por que importa |
|---|---|
| `spark.app.name` | Aparece na Spark UI, no History Server e nos alertas. Um job chamado `"app"` custa caro às 3h da manhã. |
| `spark.master` | `local[*]`, `yarn`, `k8s://...`. O `--master` do `spark-submit` **sobrescreve** o que está no código. |

### 2. Recursos — quanta máquina este job pode usar

| Parâmetro | Por que importa |
|---|---|
| `spark.driver.memory` | Heap do driver. **Não pode ser definido no builder** — veja a armadilha nº 1. |
| `spark.driver.maxResultSize` | Teto para `collect()`/`toPandas()` (default 1g). Existe para o job falhar com mensagem clara em vez de travar a JVM. |
| `spark.executor.memory` | Heap de **cada** executor. Em modo `local` não tem efeito: não existe executor separado. |
| `spark.executor.cores` | Tarefas simultâneas por executor. Mais cores dividindo a mesma memória = mais risco de OOM. |
| `spark.executor.memoryOverhead` | Fora do heap: buffers de shuffle e os **processos Python** do PySpark. Causa nº 1 de `Container killed by YARN`. |
| `spark.dynamicAllocation.enabled` | Pede e devolve executores conforme a demanda. Em cluster compartilhado, é o que evita segurar 20 máquinas ociosas. |
| `spark.dynamicAllocation.shuffleTracking.enabled` | Obrigatório com alocação dinâmica **sem** external shuffle service (o caso típico no Kubernetes). |

### 3. Paralelismo e shuffle — onde a performance se ganha ou se perde

| Parâmetro | Por que importa |
|---|---|
| `spark.sql.shuffle.partitions` | **O parâmetro mais impactante do Spark.** Default 200, *sempre* — para 1 KB ou para 1 TB. |
| `spark.sql.adaptive.enabled` | AQE: replaneja a consulta em execução, com estatísticas reais. Ligado por padrão desde o 3.2. |
| `spark.sql.adaptive.coalescePartitions.enabled` | Junta partições pequenas **depois** do shuffle. Conserta um `shuffle.partitions` exagerado sem você acertar o número. |
| `spark.sql.adaptive.skewJoin.enabled` | Quebra sozinho a partição gigante de um join. A cura do clássico "199 tarefas prontas, 1 roda há 40 minutos". |
| `spark.sql.autoBroadcastJoinThreshold` | Até esse tamanho (default 10 MB) a tabela menor é transmitida e o join acontece **sem shuffle**. |
| `spark.sql.files.maxPartitionBytes` | Tamanho alvo de partição na **leitura** (default 128 MB). |
| `spark.default.parallelism` | Só vale para RDD. Confundir com `shuffle.partitions` é erro comum — vale citar para desfazer. |

### 4. Dependências da JVM

| Parâmetro | Por que importa |
|---|---|
| `spark.jars.packages` | Driver JDBC, conector Kafka, Delta, Iceberg. **Nada disso é `pip`.** |
| `spark.jars` | Mesma coisa, com o `.jar` local — útil quando não há internet na aula. |

> Mesma lição dos exemplos [JDBC](../exemplo-jdbc/README.md) e [Kafka](../exemplo-kafka/README.md):
> uma aplicação Spark tem **duas** árvores de dependência, a do Python e a da JVM.

### 5. Comportamento SQL — estes mudam o RESULTADO

Este é o grupo que a turma subestima. Os outros mudam quanto tempo o job leva; **estes
mudam o número que sai no relatório.**

| Parâmetro | Por que importa |
|---|---|
| `spark.sql.session.timeZone` | Sem fixar, vale o fuso da JVM: o resultado depende da **máquina** onde rodou. Veja a armadilha nº 3. |
| `spark.sql.ansi.enabled` | Overflow e divisão por zero levantam erro em vez de devolver `null`. Passou a ser o padrão no Spark 4 — quem migrou do 3.x precisa saber disso. |
| `spark.sql.sources.partitionOverwriteMode` | Com `dynamic`, o `overwrite` apaga só as partições tocadas em vez de limpar o diretório inteiro da tabela. |
| `spark.sql.execution.arrow.pyspark.enabled` | Acelera muito `toPandas()` e pandas UDFs (troca serialização linha a linha por transferência colunar). |

### 6. Observabilidade

| Parâmetro | Por que importa |
|---|---|
| `spark.eventLog.enabled` / `.dir` | Sem isso, **quando o job termina a Spark UI morre junto** e não sobra nada para investigar. |
| `spark.log.level` | Nível de log da JVM. Sem ajustar, centenas de linhas `INFO` enterram a saída da sua aplicação. |

**Deliberadamente fora:** `spark.serializer` (Kryo). Ele importa para RDD; com DataFrame,
o Tungsten cuida da serialização internamente. Citar Kryo em 2026 costuma ser conselho
copiado de blog de 2016.

---

## As três armadilhas

### 1. A configuração que o Spark aceita e ignora

Rode `main.py` e olhe este trecho da saída:

```
 spark.driver.memory declarado na configuracao ... 2g
 heap que a JVM do driver realmente recebeu ..... 1024 MiB
```

O `settings.yaml` pede 2g. O `spark.conf.get()` **confirma** 2g. E a JVM recebeu 1g.

Ninguém errou: quando o seu código Python executa, **a JVM do driver já subiu**. Não dá
mais para mudar o heap dela. O valor foi gravado no `SparkConf` e não serviu para nada.

A forma certa é definir antes do processo nascer:

```bash
spark-submit --driver-memory 2g exemplo-sparksession/src/main.py
```

Rode com e sem a flag e compare a segunda linha — ela vira `2048 MiB`. É a demonstração
mais valiosa da pasta, porque ensina uma atitude: **não confie que a configuração pegou;
verifique.**

> O mesmo vale para `spark.driver.extraJavaOptions` e, em `client mode`, `spark.driver.cores`.

### 2. Ordem de precedência (e o `getOrCreate()` silencioso)

Da maior para a menor prioridade:

1. `.config()` no código / `SparkConf`
2. flags do `spark-submit` (`--conf`, `--driver-memory`, `--master`)
3. `spark-defaults.conf`
4. default do Spark

Com a ressalva da armadilha nº 1: o item 1 **vence na leitura**, mas chega tarde demais
para o que já foi decidido ao subir a JVM.

E tem o caso do `getOrCreate()`: se **já existe** uma sessão ativa — notebook, `pyspark`
shell, um teste que não fechou a sessão anterior — ele devolve a sessão existente. Configs
de runtime são aplicadas; as **estáticas e de cluster são descartadas sem erro**. O
`SparkSessionManager` deste exemplo detecta e avisa:

```python
if SparkSession.getActiveSession() is not None:
    logger.warning("Ja existe uma SparkSession ativa. Configuracoes estaticas ... IGNORADAS")
```

> Explica um sintoma clássico: *"mudei a config no notebook e nada aconteceu"*. Não
> aconteceu mesmo — era preciso reiniciar o kernel.

### 3. Fuso horário: o job que dá resultado diferente por máquina

`main.py` termina com esta consulta:

```sql
SELECT current_timezone(), to_timestamp('2026-01-17T15:28:57') AS ts, unix_timestamp(...) AS epoch
```

Troque `spark.sql.session.timeZone` para `"UTC"` no `settings.yaml` e rode de novo. O
texto de entrada é **exatamente o mesmo** e a coluna `epoch` muda em 3 horas.

Agora imagine esse job agregando pedidos por dia: o laptop do aluno (America/Sao_Paulo) e
o cluster (quase sempre UTC) vão colocar os pedidos da madrugada em **dias diferentes**. O
relatório fecha com números distintos e ninguém consegue reproduzir o bug.

> Gancho direto com o `data_criacao` (`TimestampType`) do tutorial. **Fixe o fuso sempre.**

---

## A demonstração medida

`demo_shuffle.py` roda a **mesma** agregação três vezes — 2 milhões de linhas, 50 chaves
distintas — mudando só a configuração. Saída real de uma execução:

```
 1. Default do Spark, AQE desligado | tarefas de shuffle: 200 | AQE=false | arquivos gerados:  44 |   1.58s
 2. Default do Spark, AQE ligado    | tarefas de shuffle: 200 | AQE=true  | arquivos gerados:   1 |   0.12s
 3. Ajustado a mao, AQE desligado   | tarefas de shuffle:   8 | AQE=false | arquivos gerados:   8 |   0.12s
```

O resultado é **idêntico** nos três: 50 linhas. O que muda é o custo de chegar lá — **13x**
no tempo e dezenas de arquivos minúsculos no destino.

Pontos a explorar com a turma:

- O cenário 1 não tem bug. Ele usa o **default**. Defaults não são recomendações: são
  chutes razoáveis feitos sem conhecer o seu dado.
- 200 tarefas geraram 44 arquivos, não 200 — partição vazia não vira arquivo. Mas as **200
  tarefas foram agendadas e pagas** do mesmo jeito.
- O cenário 2 mantém a configuração ruim e chega ao melhor resultado: o AQE mediu o dado
  real depois do shuffle. **É por isso que ele existe** — o cenário 3 acerta o número hoje
  e erra quando o volume triplicar.
- O script faz um *aquecimento* antes de medir, para que a diferença não seja JIT da JVM.
  Medir direito faz parte da lição.

---

## Roteiro sugerido de aula (~20 min)

1. Abra `spark_session.py` em `criar_sessao_didatica()` e leia os seis blocos. *"Vocês
   escreveram três linhas no Passo 3. Estas são as decisões que vinham junto."*
2. Rode `main.py`. Mostre o relatório e a coluna `[*]`: **o que é seu e o que é default**.
3. Pare na armadilha nº 1 (2g declarado, 1024 MiB reais). Rode de novo com
   `--driver-memory 2g` e mostre virar 2048.
4. Rode `demo_shuffle.py`. Deixe a tabela na tela enquanto explica o AQE.
5. Rode `main.py cluster` — ele **lista** o perfil de produção em vez de tentar conectar.
   Compare com o `local`: *"Mesmo código. Só o YAML mudou."*
6. Feche na armadilha nº 3: configuração que muda **resultado**, não performance.

---

## O que isso tem a ver com o tutorial

| Tutorial | Este exemplo |
|---|---|
| `SparkSessionManager` (Passo 3) | o mesmo, agora dirigido por configuração |
| `settings.yaml` (Passo 2) | dois perfis, um por ambiente |
| Separação sessão × dados (Passos 3 e 4) | `spark_session.py` × `inspecao.py` |
| Logging (Passo 8) | `spark.log.level` e o silenciamento do `py4j` |
| Schema explícito (Passo 1) | `spark.sql.ansi.enabled`: rigor com o dado, agora em runtime |
| Dependências (Passo 10) | `spark.jars.packages`: a árvore de dependências da JVM |

A sessão Spark é **a fronteira mais externa da sua aplicação** — a que define o ambiente em
que todo o resto vai rodar. Tratá-la como configuração versionada, e não como três linhas
copiadas, é a mesma disciplina que o tutorial aplica a caminhos de arquivo e credenciais.
