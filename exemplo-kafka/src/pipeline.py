# exemplo-kafka/src/pipeline.py
"""Orquestracao do pipeline de streaming.

Espelha o `pipeline/pipeline.py` do tutorial, com uma diferenca de ciclo de
vida: `run()` nao termina o trabalho -- ele DEVOLVE a query em execucao.
Quem decide esperar, parar ou monitorar e a Raiz de Composicao (main.py).
"""

import logging

from pyspark.sql.streaming import StreamingQuery

from kafka_handler import KafkaHandler
from transformations import Transformation

logger = logging.getLogger(__name__)


class Pipeline:
    """Encapsula a logica de execucao do pipeline de dados."""

    def __init__(self, data_handler: KafkaHandler, transformer: Transformation):
        self.data_handler = data_handler
        self.transformer = transformer

    def run(self, config: dict) -> StreamingQuery:
        """Monta o fluxo: consumo do topico, transformacao e escrita.

        :return: a StreamingQuery em execucao, para o main.py gerenciar.
        """
        logger.info("Pipeline iniciado...")

        pedidos_df = self.data_handler.ler_stream()

        logger.info("Adicionando a coluna valor_total")
        pedidos_df = self.transformer.add_valor_total_pedidos(pedidos_df)

        query = self.data_handler.escrever_stream(pedidos_df, config["saida"])

        logger.info("Pipeline no ar.")
        return query
