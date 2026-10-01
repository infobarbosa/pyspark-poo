# exemplo-jdbc/src/jdbc_handler.py
"""Fronteira da aplicacao com um banco relacional.

Esta e a UNICA classe do exemplo que sabe o que e JDBC. Para o resto da
aplicacao ela apenas devolve e recebe DataFrames -- exatamente o papel que
o `DataHandler` cumpre no tutorial para arquivos JSON e CSV.
"""

import logging

from py4j.protocol import Py4JJavaError
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.utils import AnalysisException

logger = logging.getLogger(__name__)


class JDBCHandler:
    """Responsavel pela leitura (input) e escrita (output) de dados via JDBC."""

    def __init__(self, spark: SparkSession, conexao: dict, senha: str):
        """
        :param spark: sessao Spark ja construida (injetada, nao criada aqui).
        :param conexao: bloco `jdbc` do settings.yaml (url, user, driver).
        :param senha: senha lida de variavel de ambiente pelo main.py.
        """
        self.spark = spark
        self.url = conexao["url"]

        # As credenciais ficam em um unico lugar, montadas uma unica vez.
        self.propriedades = {
            "user": conexao["user"],
            "password": senha,
            "driver": conexao["driver"],
        }

    def ler(self, leitura: dict) -> DataFrame:
        """Le uma tabela (ou subquery) do banco, em paralelo quando configurado."""
        dbtable = leitura["dbtable"]
        logger.info("Lendo do banco: %s", dbtable)

        try:
            reader = (
                self.spark.read.format("jdbc")
                .option("url", self.url)
                .option("dbtable", dbtable)
                .option("fetchsize", leitura["fetchsize"])
                .options(**self.propriedades)
            )

            # Leitura paralela: o Spark quebra a consulta em N intervalos de
            # `partition_column` e abre N conexoes simultaneas ao banco.
            # ATENCAO: N conexoes = N conexoes de verdade. Combine com o DBA.
            particionamento = leitura.get("particionamento")
            if particionamento:
                logger.info(
                    "Leitura particionada por '%s' em %s conexoes",
                    particionamento["partition_column"],
                    particionamento["num_partitions"],
                )
                reader = (
                    reader.option("partitionColumn", particionamento["partition_column"])
                    .option("lowerBound", particionamento["lower_bound"])
                    .option("upperBound", particionamento["upper_bound"])
                    .option("numPartitions", particionamento["num_partitions"])
                )
            else:
                logger.warning(
                    "Leitura SEM particionamento: uma unica conexao trara todos "
                    "os dados para um unico executor."
                )

            return reader.load()

        except AnalysisException as e:
            # Tabela/coluna inexistente, schema incompativel.
            logger.error("Erro de Spark/SQL ao ler '%s': %s", dbtable, e)
            raise
        except Py4JJavaError as e:
            # Banco fora do ar, credencial invalida, driver ausente no classpath.
            logger.critical("Erro na JVM ao ler '%s' (conexao/driver): %s", dbtable, e)
            raise

    def escrever(self, df: DataFrame, escrita: dict) -> None:
        """Grava o DataFrame em uma tabela do banco."""
        dbtable = escrita["dbtable"]
        logger.info("Escrevendo em '%s' (mode=%s)", dbtable, escrita["mode"])

        try:
            (
                df.write.format("jdbc")
                .option("url", self.url)
                .option("dbtable", dbtable)
                .option("batchsize", escrita["batchsize"])
                # Evita o DROP TABLE implicito do mode=overwrite.
                .option("truncate", escrita["truncate"])
                .options(**self.propriedades)
                .mode(escrita["mode"])
                .save()
            )
            logger.info("Dados gravados com sucesso em '%s'", dbtable)

        except Py4JJavaError as e:
            # Tipico aqui: violacao de constraint, deadlock, disco cheio.
            logger.critical("Erro na JVM ao escrever em '%s': %s", dbtable, e)
            raise
