# exemplo-sparksession/src/main.py
"""Raiz de Composicao (Composition Root) do exemplo de SparkSession.

Mesma forma dos exemplos JDBC e Kafka e do tutorial: este arquivo monta os
componentes e nao faz mais nada. A construcao da sessao vive em
`spark_session.py`; o trabalho sobre ela, em `inspecao.py`.

Uso:
    spark-submit exemplo-sparksession/src/main.py            # perfil do YAML
    spark-submit exemplo-sparksession/src/main.py cluster    # forca um perfil
"""

import logging
import sys

from inspecao import (
    conferir_driver_memory,
    demonstrar_timezone,
    exibir_perfil_de_referencia,
    relatorio_de_configuracao,
)
from settings import carregar_config, configurar_logging, obter_perfil
from spark_session import SparkSessionManager

logger = logging.getLogger(__name__)


def main() -> None:
    configurar_logging()

    config = carregar_config()
    nome_perfil, perfil = obter_perfil(config, sys.argv[1] if len(sys.argv) > 1 else None)

    # Perfil de producao e REFERENCIA para leitura: tentar subir uma sessao
    # 'yarn' no laptop nao da erro -- ela fica pendurada esperando um cluster
    # que nao existe. Entao apenas listamos o que o perfil declara.
    if not perfil["master"].startswith("local"):
        exibir_perfil_de_referencia(nome_perfil, perfil)
        return

    logger.info("Construindo a sessao com o perfil '%s':", nome_perfil)
    spark = SparkSessionManager.criar(perfil)

    # O log da JVM tem nivel proprio, independente do logging do Python.
    spark.sparkContext.setLogLevel("WARN")

    try:
        relatorio_de_configuracao(spark, definidos_por_nos=set(perfil.get("conf", {})))
        conferir_driver_memory(spark)
        demonstrar_timezone(spark)

    except Exception as e:
        logger.error("Erro durante a execucao do job: %s", e, exc_info=True)
        sys.exit(1)

    finally:
        spark.stop()
        logger.info("Spark session encerrada.")


if __name__ == "__main__":
    main()
