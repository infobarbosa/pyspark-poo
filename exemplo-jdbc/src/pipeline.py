# exemplo-jdbc/src/pipeline.py
"""Orquestracao do pipeline.

Espelha o `pipeline/pipeline.py` do tutorial. A classe NAO CRIA suas
dependencias: ela as recebe prontas no construtor (Injecao de Dependencias,
Passo 7). Por isso e possivel testa-la com um handler falso, sem banco nenhum.
"""

import logging

from jdbc_handler import JDBCHandler
from transformations import Transformation

logger = logging.getLogger(__name__)


class Pipeline:
    """Encapsula a logica de execucao do pipeline de dados."""

    def __init__(self, data_handler: JDBCHandler, transformer: Transformation):
        self.data_handler = data_handler
        self.transformer = transformer

    def run(self, config: dict) -> None:
        """Executa o pipeline completo: carga, transformacao e escrita."""
        logger.info("Pipeline iniciado...")

        pedidos_df = self.data_handler.ler(config["leitura"])
        logger.info("Particoes lidas do banco: %s", pedidos_df.rdd.getNumPartitions())
        pedidos_df.show(5, truncate=False)

        logger.info("Adicionando a coluna valor_total")
        pedidos_df = self.transformer.add_valor_total_pedidos(pedidos_df)

        logger.info("Calculando os 10 clientes de maior valor")
        top_10_df = self.transformer.get_top_10_clientes(pedidos_df)
        top_10_df.show(10, truncate=False)

        self.data_handler.escrever(top_10_df, config["escrita"])

        logger.info("Pipeline concluido com sucesso!")
