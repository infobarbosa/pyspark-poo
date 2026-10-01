# exemplo-jdbc/src/main.py
"""Raiz de Composicao (Composition Root) do exemplo JDBC.

Identico em forma ao `main.py` do tutorial: este arquivo NAO processa dados.
Ele apenas monta os componentes, injeta as dependencias, dispara o pipeline
e decide o que fazer quando algo falha.
"""

import logging
import sys

from jdbc_handler import JDBCHandler
from pipeline import Pipeline
from settings import carregar_config, configurar_logging, obter_senha
from spark_session import SparkSessionManager
from transformations import Transformation

logger = logging.getLogger(__name__)


def main() -> None:
    config = carregar_config()
    configurar_logging()

    # Falha ANTES de subir a sessao Spark se o segredo nao estiver no ambiente:
    # errar cedo e barato.
    senha = obter_senha(config["jdbc"]["senha_env"])

    spark = SparkSessionManager.get_spark_session(
        app_name=config["spark"]["app_name"],
        jars_packages=config["spark"]["jars_packages"],
    )
    logger.info("Iniciando job: %s", config["spark"]["app_name"])

    try:
        # Composition Root: o UNICO lugar que monta as dependencias concretas.
        data_handler = JDBCHandler(spark, conexao=config["jdbc"], senha=senha)
        transformer = Transformation()
        pipeline = Pipeline(data_handler, transformer)

        pipeline.run(config=config)

        logger.info("Pipeline finalizado com sucesso.")

    except Exception as e:
        logger.error("Erro durante a execucao do job: %s", e, exc_info=True)
        sys.exit(1)

    finally:
        # A sessao SEMPRE e encerrada, com ou sem erro.
        spark.stop()
        logger.info("Spark session encerrada.")


if __name__ == "__main__":
    main()
