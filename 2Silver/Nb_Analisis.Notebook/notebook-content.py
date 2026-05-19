# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "26d4a8e0-ee35-4760-81f2-163497b579d4",
# META       "default_lakehouse_name": "Lk_Silver",
# META       "default_lakehouse_workspace_id": "4929c9c5-4f48-4c13-b113-c52b98679915",
# META       "known_lakehouses": [
# META         {
# META           "id": "26d4a8e0-ee35-4760-81f2-163497b579d4"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import functions as F

silver = spark.read.table("dbo.silver_crypto_market")
#abfss://4929c9c5-4f48-4c13-b113-c52b98679915@onelake.dfs.fabric.microsoft.com/26d4a8e0-ee35-4760-81f2-163497b579d4/Tables/dbo/silver_crypto_market

dup_siglas = (silver
              .groupBy("sigla")
              .agg(
                  F.countDistinct("id_moneda").alias("n_monedas"),
                  F.collect_set("nombre_moneda").alias("nombres")   # lista única
              )
              .filter("n_monedas > 1")          # sólo siglas duplicadas
              .orderBy(F.desc("n_monedas"), "sigla"))

dup_siglas.show(truncate=False)

silver_rows = spark.read.table("dbo.silver_crypto_market").count()
print(f"Filas en Silver: {silver_rows:,}")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

silver = spark.read.table("silver_crypto_market")

# agrupa por toda la fila (usa to_json para convertir el struct en string)
dup_filas = (
    silver.groupBy(F.to_json(F.struct([F.col(c) for c in silver.columns])))
          .count()
          .filter("count > 1")
)

print("Filas completamente duplicadas:", dup_filas.count())
dup_filas.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
