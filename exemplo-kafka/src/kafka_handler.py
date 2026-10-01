# exemplo-kafka/src/kafka_handler.py
"""Fronteira da aplicacao com o Kafka.

Mesmo papel do `DataHandler` do tutorial: e a unica classe que sabe que
existe um broker do outro lado. O resto da aplicacao so ve DataFrames.
"""

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import (
    FloatType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

logger = logging.getLogger(__name__)


class KafkaHandler:
    """Responsavel por consumir de um topico e persistir o resultado."""

    def __init__(self, spark: SparkSession, kafka: dict):
        self.spark = spark
        self.kafka = kafka

    def _schema_pedido(self) -> StructType:
        """Schema explicito da mensagem.

        Em streaming nao existe `inferSchema`: o Spark nao pode "dar uma olhada"
        em dados que ainda nao chegaram. O contrato precisa ser declarado --
        o que torna a licao do Passo 1 do tutorial obrigatoria, nao opcional.
        """
        return StructType(
            [
                StructField("id_pedido", StringType(), True),
                StructField("produto", StringType(), True),
                StructField("valor_unitario", FloatType(), True),
                StructField("quantidade", LongType(), True),
                StructField("data_criacao", TimestampType(), True),
                StructField("uf", StringType(), True),
                StructField("id_cliente", LongType(), True),
            ]
        )

    def ler_stream(self) -> DataFrame:
        """Abre o stream do topico e devolve um DataFrame ja tipado."""
        logger.info(
            "Consumindo o topico '%s' em %s",
            self.kafka["topico"],
            self.kafka["bootstrap_servers"],
        )

        bruto = (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.kafka["bootstrap_servers"])
            .option("subscribe", self.kafka["topico"])
            .option("startingOffsets", self.kafka["starting_offsets"])
            .option("maxOffsetsPerTrigger", self.kafka["max_offsets_per_trigger"])
            # true (default) derruba o job se um offset sumir por retencao.
            # Em aula, false evita interrupcao; em producao, pense duas vezes.
            .option("failOnDataLoss", "false")
            .load()
        )

        # O Kafka nao entrega JSON: entrega BYTES.
        # O DataFrame vem sempre com key, value, topic, partition, offset,
        # timestamp e timestampType -- e `value` e binary.
        return (
            bruto.select(
                F.from_json(F.col("value").cast("string"), self._schema_pedido()).alias("pedido"),
                F.col("offset").alias("kafka_offset"),
                F.col("timestamp").alias("kafka_timestamp"),
            )
            # Mensagem malformada vira struct nulo (nao explode!). Descartamos
            # aqui; em producao, isto viraria uma dead letter queue.
            .filter(F.col("pedido").isNotNull())
            .select("pedido.*", "kafka_offset", "kafka_timestamp")
        )

    def escrever_stream(self, df: DataFrame, saida: dict) -> StreamingQuery:
        """Persiste o stream com checkpoint (e o que garante a retomada)."""
        logger.info("Gravando em '%s' (checkpoint em '%s')", saida["path"], saida["checkpoint"])

        return (
            df.writeStream.format(saida["formato"])
            .outputMode("append")
            .option("path", saida["path"])
            # Sem checkpointLocation nao ha tolerancia a falha: ao reiniciar,
            # o job nao sabe o que ja consumiu.
            .option("checkpointLocation", saida["checkpoint"])
            .trigger(processingTime=saida["trigger"])
            .start()
        )

    # ------------------------------------------------------------------
    # Variante em BATCH -- mesma fronteira, sem stream.
    # Util para reprocessamento de uma faixa fechada de offsets.
    # Repare que a transformacao de negocio nao mudaria uma virgula.
    # ------------------------------------------------------------------
    def ler_batch(self) -> DataFrame:
        """Le uma faixa fechada do topico, como se fosse um arquivo."""
        bruto = (
            self.spark.read.format("kafka")
            .option("kafka.bootstrap.servers", self.kafka["bootstrap_servers"])
            .option("subscribe", self.kafka["topico"])
            .option("startingOffsets", "earliest")
            .option("endingOffsets", "latest")
            .load()
        )
        return bruto.select(
            F.from_json(F.col("value").cast("string"), self._schema_pedido()).alias("pedido")
        ).select("pedido.*")
