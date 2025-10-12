from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql import functions as F

# Inicializa a SparkSession
spark = SparkSession.builder.appName("RiscoInferSchemaSalarios").getOrCreate()

# --- Cenário: Confusão de Bônus por Causa do inferSchema ---

# --- Abordagem 1: O Risco do inferSchema=True ---
print("--- 1. Lendo com inferSchema (Abordagem Perigosa) ---")

# O Spark vai "olhar" os dados e tentar adivinhar o tipo de cada coluna.
# Ele verá '0101' (string) e 101 (int) na mesma coluna e pode decidir
# converter tudo para inteiro, pois é o tipo mais "comum" ou que se encaixa.

df = spark.read.option("inferSchema", "true").csv("/tmp/data.csv", header=True)

print("Schema inferido pelo Spark:")
df.printSchema()
# Resultado esperado: 'cod_bonus' será inferido como 'long' ou 'integer',
# o que fará com que "0101" seja lido como o número 101.

print("\nDados como o Spark os leu (com 'cod_bonus' corrompido):")
df.show()

# Agora, vamos simular o pagamento de um bônus.
# O bônus é para o código 101 (Diretor), no valor de 50% do salário.
cod_bonus_diretor = 101
percentual_bonus = 0.5

# A lógica de negócio errada:
# O analista João Silva, cujo código era "0101", agora tem o código 101.
# Ele receberá indevidamente o bônus do diretor!
print(f"\nCalculando bônus de {percentual_bonus:.0%} para o código '{cod_bonus_diretor}'...")
df_bonus = df.withColumn(
    "valor_bonus",
    F.when(F.col("cod_bonus") == cod_bonus_diretor, F.col("salario") * percentual_bonus).otherwise(0)
)

print("\nResultado do cálculo de bônus (INCORRETO):")
df_bonus.show()
print("PROBLEMA: João Silva (Analista) recebeu o bônus que era para Carlos Oliveira (Diretor)!")

spark.stop()
