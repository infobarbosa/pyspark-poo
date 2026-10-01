# exemplo-jdbc/src/transformations.py
"""Regras de negocio puras.

Espelha o `processing/transformations.py` do tutorial -- e, de proposito, os
metodos tem os MESMOS nomes. Compare este arquivo com o do tutorial: eles sao
intercambiaveis. Nada aqui sabe que os dados vieram de um banco relacional.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class Transformation:
    """Contem as transformacoes e regras de negocio da aplicacao."""

    def add_valor_total_pedidos(self, pedidos_df: DataFrame) -> DataFrame:
        """Adiciona a coluna 'valor_total' (valor_unitario * quantidade)."""
        return pedidos_df.withColumn(
            "valor_total", F.col("valor_unitario") * F.col("quantidade")
        )

    def get_top_10_clientes(self, pedidos_df: DataFrame) -> DataFrame:
        """Calcula o valor total de pedidos por cliente e retorna os 10 maiores."""
        return (
            pedidos_df.groupBy("id_cliente")
            .agg(F.sum("valor_total").alias("valor_total"))
            .orderBy(F.desc("valor_total"))
            .limit(10)
        )
