# exemplo-kafka/src/transformations.py
"""Regras de negocio puras.

Espelha o `processing/transformations.py` do tutorial. Abra este arquivo ao
lado do `transformations.py` do exemplo JDBC e do tutorial: o metodo
`add_valor_total_pedidos` e LITERALMENTE o mesmo nos tres.

Arquivo, banco ou stream -- a regra de negocio nao muda. E esse o ganho de
isolar a fronteira externa em uma classe separada.
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
