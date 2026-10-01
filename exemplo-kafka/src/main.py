# exemplo-kafka/src/main.py
"""Raiz de Composicao (Composition Root) do exemplo Kafka.

Compare com o `main.py` do exemplo JDBC: a estrutura e a mesma. A unica
diferenca esta no ciclo de vida -- um job de streaming nao termina sozinho,
ele fica no ar ate ser interrompido.
"""

import logging
import sys

from kafka_handler import KafkaHandler
from pipeline import Pipeline
from settings import carregar_config, configurar_logging
from spark_session import SparkSessionManager
from transformations import Transformation

logger = logging.getLogger(__name__)


def main() -> None:
    config = carregar_config()
    configurar_logging()

    spark = SparkSessionManager.get_spark_session(
        app_name=config["spark"]["app_name"],
        jars_packages=config["spark"]["jars_packages"],
    )
    logger.info("Iniciando job: %s", config["spark"]["app_name"])

    try:
        # Composition Root: o UNICO lugar que monta as dependencias concretas.
        data_handler = KafkaHandler(spark, kafka=config["kafka"])
        transformer = Transformation()
        pipeline = Pipeline(data_handler, transformer)

        query = pipeline.run(config=config)

        logger.info("Streaming no ar. Ctrl+C para encerrar.")
        # O job fica vivo aqui, processando um micro-batch a cada trigger.
        query.awaitTermination()

    except KeyboardInterrupt:
        logger.info("Interrupcao solicitada pelo usuario.")

    except Exception as e:
        logger.error("Erro durante a execucao do job: %s", e, exc_info=True)
        sys.exit(1)

    finally:
        spark.stop()
        logger.info("Spark session encerrada.")


if __name__ == "__main__":
    main()
