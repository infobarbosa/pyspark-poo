from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql import functions as F

# Inicializa a SparkSession
spark = SparkSession.builder.appName("CalculoDeBonus").getOrCreate()

# O bônus é para o código 101 (Diretor), no valor de 50% do salário.
cod_bonus_diretor = 101
percentual_bonus = 0.5

# --- Abordagem 2: A Solução com Schema Definido Manualmente ---
print("\n--- 2. Lendo com Schema Definido (Abordagem Segura) ---")

# Definindo explicitamente que 'cod_bonus' é uma String.
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("nome", StringType(), True),
    StructField("cargo", StringType(), True),
    StructField("salario", DoubleType(), True),
    StructField("cod_bonus", StringType(), True) # A definição correta!
])

# Criando o DataFrame com o schema seguro
df = spark.read.option("header", "true").schema(schema).csv("/tmp/data.csv")

print("Schema definido manualmente:")
df.printSchema()

print("\nDados lidos corretamente (preservando o '0' em '0101'):")
df.show()

# Agora, o cálculo de bônus funcionará como esperado.
# O bônus será aplicado ao 'cod_bonus' numérico 101, mas como nossa
# coluna agora é String, precisamos fazer o cast.
print(f"\nCalculando bônus de {percentual_bonus:.0%} para o código '{cod_bonus_diretor}' (de forma segura)...")
df = df.withColumn(
    "valor_bonus",
    F.when(F.col("cod_bonus") == str(cod_bonus_diretor), F.col("salario") * percentual_bonus).otherwise(0)
)

print("\nResultado do cálculo de bônus (CORRETO):")
df.show()
print("SUCESSO: Apenas Carlos Oliveira (Diretor) recebeu o bônus, como esperado.")

# Finaliza a SparkSession
spark.stop()