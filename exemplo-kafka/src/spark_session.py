# exemplo-kafka/src/spark_session.py
"""Criacao da sessao Spark -- e so isso.

Espelha o `session/spark_session.py` do tutorial. Repare que este arquivo e
praticamente IDENTICO ao do exemplo JDBC: muda apenas a coordenada Maven que
chega em `jars_packages`. Carregar o conector do Kafka e assunto da SESSAO,
nao do handler que consome o topico.
"""

import logging

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


class SparkSessionManager:
    """Gerencia a criacao e o acesso a sessao Spark."""

    @staticmethod
    def get_spark_session(app_name: str, jars_packages: str = "") -> SparkSession:
        """Cria e retorna uma sessao Spark.

        :param app_name: nome da aplicacao Spark.
        :param jars_packages: coordenadas Maven das dependencias da JVM
            (aqui, o conector spark-sql-kafka). Equivale ao --packages.
        :return: instancia da SparkSession.
        """
        logger.info("Criando a sessao Spark '%s'", app_name)

        builder = SparkSession.builder.appName(app_name).master("local[*]")

        if jars_packages:
            logger.info("Dependencias da JVM: %s", jars_packages)
            builder = builder.config("spark.jars.packages", jars_packages)

        return builder.getOrCreate()
