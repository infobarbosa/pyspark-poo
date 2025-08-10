from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, LongType, ArrayType, DateType, FloatType

spark = SparkSession.builder.appName("Analise de Pedidos").getOrCreate()

# Schema do dataframe de clientes
schema_clientes = StructType(
    [
        StructField("id", LongType(), True),
        StructField("nome", StringType(), True),
        StructField("data_nasc", DateType(), True),
        StructField("cpf", StringType(), True),
        StructField("email", StringType(), True),
        StructField("interesses", ArrayType(StringType()), True)
    ]
)
# Abrir o dataframe de clientes
clientes = spark.read.option("compression", "gzip").json("data/clientes.gz", schema=schema_clientes)

clientes.show(5, truncate=False)

# Schema do dataframe de pedidos
schema_pedidos = StructType([
    StructField("id_pedido", StringType(), True),
    StructField("produto", StringType(), True),
    StructField("valor_unitario", FloatType(), True),
    StructField("quantidade", LongType(), True),
    StructField("data_criacao", TimestampType(), True),
    StructField("uf", StringType(), True),
    StructField("id_cliente", LongType(), True)
])

# Abrir o dataframe de pedidos
pedidos = spark.read.option("compression", "gzip").csv("data/pedidos.gz", header=True, schema=schema_pedidos, sep=";")
pedidos = pedidos.withColumn("valor_total", F.col("valor_unitario") * F.col("quantidade"))
pedidos.show(5, truncate=False)

# Calcular o valor total de pedidos por cliente e filtrar os 10 maiores
calculado = pedidos.groupBy("id_cliente") \
    .agg(F.sum("valor_total").alias("valor_total")) \
    .orderBy(F.desc("valor_total")) \
    .limit(10)

calculado.show(10, truncate=False)

# Fazer a junção dos dataframes
pedidos_clientes = calculado.join(clientes, clientes.id == calculado.id_cliente, "inner") \
    .select(calculado.id_cliente, clientes.nome, clientes.email, calculado.valor_total)

pedidos_clientes.show(20, truncate=False)

spark.stop()
