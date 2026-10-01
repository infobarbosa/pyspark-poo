# exemplo-sparksession/src/demo_shuffle.py
"""Mostra, com numero na tela, o efeito de UM parametro.

`spark.sql.shuffle.partitions` e `spark.sql.adaptive.enabled` sao configuracoes
de runtime: da para muda-las com a sessao ja no ar. Aproveitamos isso para
rodar a MESMA consulta em tres cenarios e comparar.

Uso:
    spark-submit exemplo-sparksession/src/demo_shuffle.py
"""

import shutil
import sys
import time
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

SAIDA = Path(__file__).resolve().parents[1] / "saida" / "demo"

# (rotulo, shuffle.partitions, AQE)
CENARIOS = [
    ("1. Default do Spark, AQE desligado", "200", "false"),
    ("2. Default do Spark, AQE ligado   ", "200", "true"),
    ("3. Ajustado a mao, AQE desligado  ", "8", "false"),
]


def consulta(spark: SparkSession) -> DataFrame:
    """Um groupBy simples: 2 milhoes de linhas, apenas 50 chaves distintas."""
    return (
        spark.range(0, 2_000_000)
        .withColumn("chave", F.col("id") % 50)
        .groupBy("chave")
        .agg(F.count("*").alias("qtd"), F.sum("id").alias("soma"))
    )


def executar(spark: SparkSession, rotulo: str, particoes: str, aqe: str, destino: Path) -> None:
    spark.conf.set("spark.sql.shuffle.partitions", particoes)
    spark.conf.set("spark.sql.adaptive.enabled", aqe)

    inicio = time.perf_counter()
    df = consulta(spark)
    df.write.mode("overwrite").parquet(str(destino))
    duracao = time.perf_counter() - inicio

    # Cada particao NAO VAZIA do resultado vira um arquivo no destino.
    arquivos = len(list(destino.glob("*.parquet")))

    print(
        f" {rotulo} | tarefas de shuffle: {particoes:>3} | AQE={aqe:<5}"
        f" | arquivos gerados: {arquivos:>3} | {duracao:6.2f}s"
    )


def main() -> None:
    if SAIDA.exists():
        shutil.rmtree(SAIDA)

    spark = (
        SparkSession.builder.appName("demo-shuffle-partitions")
        .master("local[*]")
        .config("spark.log.level", "ERROR")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # Aquecimento: a primeira consulta de qualquer sessao paga a geracao de
    # codigo e o JIT da JVM. Sem isto, o cenario 1 pareceria lento por um
    # motivo que nao tem nada a ver com o parametro que queremos medir.
    consulta(spark).count()

    print("\n" + "=" * 100)
    print(" O resultado e IDENTICO nos tres cenarios: 50 linhas.")
    print(" O que muda e quantas tarefas e quantos arquivos foram necessarios para chegar la.")
    print("=" * 100)

    try:
        for indice, (rotulo, particoes, aqe) in enumerate(CENARIOS, start=1):
            executar(spark, rotulo, particoes, aqe, SAIDA / f"cenario_{indice}")

        print("=" * 100)
        print(
            "\n Cenario 1: o Spark disparou 200 tarefas para agregar 50 chaves. O destino\n"
            "            ficou com dezenas de arquivos minusculos (particao vazia nao vira\n"
            "            arquivo, mas a tarefa rodou e foi paga assim mesmo).\n"
            "            Esse e o 'small files problem' -- e ele veio de um DEFAULT.\n"
            " Cenario 2: a MESMA configuracao ruim, mas o AQE olhou o tamanho real dos\n"
            "            dados depois do shuffle e juntou tudo sozinho.\n"
            " Cenario 3: o numero certo na mao -- funciona, ate o volume mudar na semana\n"
            "            que vem. E por isso que o AQE existe.\n"
        )
        print(f" Inspecione os arquivos em: {SAIDA}\n")

    except Exception as e:
        print(f"Falha na demonstracao: {e}", file=sys.stderr)
        sys.exit(1)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
