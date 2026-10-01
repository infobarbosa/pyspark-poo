# exemplo-sparksession/src/inspecao.py
"""Inspecao da sessao: relatorios e demonstracoes sobre a configuracao ativa.

Este modulo tem o papel que o `io_utils/data_handler.py` tem no tutorial --
nao o de ler dados, mas o de ser a CAMADA DE TRABALHO do exemplo. O `main.py`
fica so com a montagem; o que este exemplo tem de conteudo mora aqui.
"""

from pyspark.sql import SparkSession

LARGURA = 78

# Os parametros que valem a pena olhar, agrupados pelo tema que resolvem.
PARAMETROS_POR_TEMA = {
    "1. Identidade": [
        "spark.app.name",
        "spark.master",
    ],
    "2. Recursos": [
        "spark.driver.memory",
        "spark.driver.maxResultSize",
        "spark.executor.memory",
        "spark.executor.cores",
        "spark.executor.memoryOverhead",
        "spark.dynamicAllocation.enabled",
        "spark.dynamicAllocation.maxExecutors",
    ],
    "3. Paralelismo e shuffle": [
        "spark.default.parallelism",
        "spark.sql.shuffle.partitions",
        "spark.sql.adaptive.enabled",
        "spark.sql.adaptive.coalescePartitions.enabled",
        "spark.sql.adaptive.skewJoin.enabled",
        "spark.sql.autoBroadcastJoinThreshold",
        "spark.sql.files.maxPartitionBytes",
    ],
    "4. Dependencias da JVM": [
        "spark.jars.packages",
        "spark.jars",
    ],
    "5. Comportamento SQL (muda o RESULTADO)": [
        "spark.sql.session.timeZone",
        "spark.sql.ansi.enabled",
        "spark.sql.sources.partitionOverwriteMode",
        "spark.sql.execution.arrow.pyspark.enabled",
    ],
    "6. Observabilidade": [
        "spark.eventLog.enabled",
        "spark.eventLog.dir",
        "spark.ui.showConsoleProgress",
    ],
}


def valor_efetivo(spark: SparkSession, chave: str) -> str:
    """Le o valor que esta REALMENTE valendo na sessao."""
    try:
        return str(spark.conf.get(chave))
    except Exception:
        return "-"


def exibir_perfil_de_referencia(nome: str, perfil: dict) -> None:
    """Apenas lista o perfil, sem tentar conectar em um cluster inexistente."""
    print("\n" + "=" * LARGURA)
    print(f" PERFIL '{nome}' -- referencia de producao (master={perfil['master']})")
    print(" Nao ha cluster aqui para conectar; abaixo, o que este perfil declara.")
    print("=" * LARGURA)
    for chave, valor in perfil.get("conf", {}).items():
        print(f" {chave:<56} {valor}")
    print("=" * LARGURA)
    print(" Compare com o perfil 'local': mesmo codigo, configuracao diferente.\n")


def relatorio_de_configuracao(spark: SparkSession, definidos_por_nos: set) -> None:
    """Imprime a configuracao efetiva, marcando o que veio do nosso YAML.

    Repare que NAO afirmamos aqui qual e o default do Spark: perguntamos a
    propria sessao. Defaults mudam de versao para versao -- a sessao nao mente.
    """
    print("\n" + "=" * LARGURA)
    print(f" CONFIGURACAO EFETIVA DA SESSAO  |  Spark {spark.version}")
    print(" [*] = definido por nos   |   [ ] = default da sua versao do Spark")
    print("=" * LARGURA)

    for tema, chaves in PARAMETROS_POR_TEMA.items():
        print(f"\n{tema}")
        print("-" * LARGURA)
        for chave in chaves:
            marca = "*" if chave in definidos_por_nos else " "
            print(f" [{marca}] {chave:<52} {valor_efetivo(spark, chave)}")

    print("\n" + "=" * LARGURA)
    print(f" Spark UI: {spark.sparkContext.uiWebUrl}")
    print("=" * LARGURA + "\n")


def conferir_driver_memory(spark: SparkSession) -> None:
    """A configuracao que o Spark ACEITA e IGNORA -- em silencio.

    `spark.driver.memory` definido no builder (ou no settings.yaml) e gravado
    no SparkConf e lido de volta certinho. Mas a JVM do driver JA SUBIU quando
    este codigo Python executa: nao da mais para mudar o heap dela.

    O unico jeito de conferir e perguntar para a propria JVM.
    """
    declarado = valor_efetivo(spark, "spark.driver.memory")

    # API interna do PySpark (_jvm), usada aqui so para a demonstracao.
    heap_bytes = spark.sparkContext._jvm.java.lang.Runtime.getRuntime().maxMemory()
    heap_mb = round(heap_bytes / 1024 / 1024)

    print("-" * LARGURA)
    print(" ARMADILHA: configuracao aceita e ignorada")
    print("-" * LARGURA)
    print(f" spark.driver.memory declarado na configuracao ... {declarado}")
    print(f" heap que a JVM do driver realmente recebeu ..... {heap_mb} MiB")
    print(
        "\n Se os dois numeros nao batem, o Spark nao errou: a JVM ja estava no ar.\n"
        " Memoria de DRIVER se define ANTES do processo comecar:\n"
        "     spark-submit --driver-memory 2g ...\n"
        " Compare: rode este script com e sem a flag e olhe a segunda linha.\n"
    )


def demonstrar_timezone(spark: SparkSession) -> None:
    """Prova concreta de que configuracao muda RESULTADO, nao so performance."""
    print("O mesmo texto, interpretado com o fuso da sessao:")
    spark.sql(
        """
        SELECT current_timezone()                        AS fuso_da_sessao,
               to_timestamp('2026-01-17T15:28:57')       AS timestamp_lido,
               unix_timestamp(to_timestamp('2026-01-17T15:28:57')) AS epoch
        """
    ).show(truncate=False)
    print(
        "Troque spark.sql.session.timeZone para 'UTC' no settings.yaml,\n"
        "rode de novo e compare a coluna `epoch`. O dado de entrada e o mesmo.\n"
    )
