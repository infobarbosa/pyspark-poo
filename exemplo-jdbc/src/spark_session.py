# exemplo-jdbc/src/spark_session.py
"""Criacao da sessao Spark -- e so isso.

Espelha o `session/spark_session.py` do tutorial. A unica diferenca e o
parametro `jars_packages`: para falar com um banco, a JVM precisa do driver
JDBC no classpath, e esse e um assunto da SESSAO, nao do handler de dados.
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
            (aqui, o driver JDBC). Equivale ao --packages do spark-submit.
        :return: instancia da SparkSession.
        """
        logger.info("Criando a sessao Spark '%s'", app_name)

        builder = SparkSession.builder.appName(app_name).master("local[*]")

        if jars_packages:
            logger.info("Dependencias da JVM: %s", jars_packages)
            builder = builder.config("spark.jars.packages", jars_packages)

        return builder.getOrCreate()
