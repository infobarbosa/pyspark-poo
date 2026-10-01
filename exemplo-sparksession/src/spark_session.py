# exemplo-sparksession/src/spark_session.py
"""Duas formas de construir uma SparkSession.

`criar_sessao_didatica()`  -> o builder escrito na mao, parametro por parametro.
                              E o que se mostra na aula. NAO e o que se leva
                              para producao: configuracao virou codigo.

`SparkSessionManager.criar()` -> o builder dirigido por configuracao.
                              E a versao que vai para producao, e a evolucao
                              natural do `SparkSessionManager` do Passo 3.

Os dois produzem a mesma sessao. A diferenca e onde mora a configuracao.
"""

import logging

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


# ======================================================================
# VERSAO DIDATICA -- para ler em voz alta com a turma.
# ======================================================================
def criar_sessao_didatica() -> SparkSession:
    """Builder explicito, agrupado por tema. Cada linha tem um porque."""
    return (
        SparkSession.builder
        # ------------------------------------------------------------------
        # 1. IDENTIDADE -- quem e este job e onde ele roda
        # ------------------------------------------------------------------
        # Aparece na Spark UI, no History Server e nos alertas. Um nome
        # generico ("app") custa caro as 3h da manha.
        .appName("exemplo-sparksession")
        # Onde o cluster vive. local[*] / yarn / k8s://... / spark://host:7077
        # O --master do spark-submit SOBRESCREVE o que esta aqui.
        .master("local[*]")
        # ------------------------------------------------------------------
        # 2. RECURSOS -- quanta maquina este job pode usar
        # ------------------------------------------------------------------
        # ATENCAO: spark.driver.memory definido AQUI nao tem efeito em client
        # mode. A JVM do driver ja subiu quando este Python executa. Use
        # `spark-submit --driver-memory 4g`. (armadilha nº 2 do README)
        # .config("spark.driver.memory", "4g")   <-- tarde demais
        #
        # Teto para o que collect()/toPandas() pode trazer ao driver.
        # Existe para o job falhar com mensagem clara em vez de travar a JVM.
        .config("spark.driver.maxResultSize", "1g")
        # Memoria e cores de CADA executor. Em modo local isto e ignorado:
        # nao ha executor separado, o driver faz o trabalho.
        .config("spark.executor.memory", "4g")
        .config("spark.executor.cores", "4")
        # Fora do heap da JVM: buffers de shuffle, off-heap e os processos
        # Python do PySpark. Causa nº 1 de "Container killed by YARN".
        .config("spark.executor.memoryOverhead", "1g")
        # ------------------------------------------------------------------
        # 3. PARALELISMO E SHUFFLE -- onde a performance se ganha ou se perde
        # ------------------------------------------------------------------
        # O parametro mais impactante do Spark. Default: 200, SEMPRE, para
        # 1 KB ou 1 TB. Cada particao vira uma tarefa e um arquivo na saida.
        .config("spark.sql.shuffle.partitions", "8")
        # AQE: replaneja a consulta em tempo de execucao, com as estatisticas
        # reais. Ligado por padrao desde o Spark 3.2.
        .config("spark.sql.adaptive.enabled", "true")
        # Junta particoes pequenas depois do shuffle -- conserta um
        # shuffle.partitions exagerado sem voce precisar acertar o numero.
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # Detecta particao gigante em join e a quebra sozinho. A cura do
        # classico "199 tarefas terminaram, 1 roda ha 40 minutos".
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        # Ate este tamanho, a tabela menor e transmitida para todos os nos
        # e o join acontece sem shuffle. Default: 10 MB.
        .config("spark.sql.autoBroadcastJoinThreshold", "50m")
        # Tamanho alvo de cada particao na LEITURA de arquivos. Default 128 MB.
        # Arquivo unico de 10 GB nao paraleliza; 100 mil arquivos de 1 KB
        # tambem nao (problema dos small files).
        .config("spark.sql.files.maxPartitionBytes", "128m")
        # ------------------------------------------------------------------
        # 4. DEPENDENCIAS DA JVM -- a licao dos exemplos JDBC e Kafka
        # ------------------------------------------------------------------
        # Driver JDBC, conector Kafka, Delta, Iceberg... nada disso e pip.
        # Equivale ao --packages do spark-submit.
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.4")
        # ------------------------------------------------------------------
        # 5. COMPORTAMENTO SQL -- estes MUDAM O RESULTADO do seu job
        # ------------------------------------------------------------------
        # Sem isto, o fuso e o da JVM: o resultado do job depende da maquina
        # em que ele rodou. Fixe SEMPRE. (armadilha nº 4 do README)
        .config("spark.sql.session.timeZone", "America/Sao_Paulo")
        # overwrite apaga so as particoes que os dados novos tocam, em vez de
        # limpar o diretorio inteiro da tabela.
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        # Modo ANSI: overflow e divisao por zero levantam erro em vez de
        # devolver null silenciosamente. Passou a ser o padrao no Spark 4.
        .config("spark.sql.ansi.enabled", "true")
        # Arrow acelera muito toPandas() e pandas UDFs (troca a serializacao
        # linha a linha por transferencia colunar).
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        # ------------------------------------------------------------------
        # 6. OBSERVABILIDADE -- job sem log de evento e job que sumiu
        # ------------------------------------------------------------------
        # Grava o historico para o History Server. Sem isto, quando o job
        # termina a Spark UI morre junto e nao ha o que investigar.
        # .config("spark.eventLog.enabled", "true")
        # .config("spark.eventLog.dir", "/tmp/spark-events")
        .config("spark.ui.showConsoleProgress", "true")
        # ------------------------------------------------------------------
        # Devolve a sessao ATIVA se ja existir uma. (armadilha nº 3 do README)
        # ------------------------------------------------------------------
        .getOrCreate()
    )


# ======================================================================
# VERSAO DE PRODUCAO -- a configuracao mora no YAML, nao no codigo.
# ======================================================================
class SparkSessionManager:
    """Gerencia a criacao da sessao Spark a partir de um perfil de configuracao.

    Evolucao do `SparkSessionManager` do Passo 3 do tutorial: em vez de
    parametros fixos no codigo, o builder e alimentado pelo settings.yaml.
    Trocar de ambiente vira trocar de perfil -- sem tocar em .py.
    """

    @staticmethod
    def criar(perfil: dict) -> SparkSession:
        """Monta a sessao a partir de um dos perfis do settings.yaml."""
        if SparkSession.getActiveSession() is not None:
            logger.warning(
                "Ja existe uma SparkSession ativa. Configuracoes estaticas e de "
                "cluster deste builder serao IGNORADAS pelo getOrCreate()."
            )

        builder = SparkSession.builder.appName(perfil["app_name"]).master(perfil["master"])

        # O coracao da versao dirigida por configuracao: o builder nao sabe
        # quais parametros existem. Ele apenas aplica o que veio do YAML.
        for chave, valor in perfil.get("conf", {}).items():
            logger.info("  %s = %s", chave, valor)
            builder = builder.config(chave, valor)

        return builder.getOrCreate()
