# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "9956390f-6fac-4de5-9599-57fe0f66ec21",
# META       "default_lakehouse_name": "Lk_bronze",
# META       "default_lakehouse_workspace_id": "4929c9c5-4f48-4c13-b113-c52b98679915",
# META       "known_lakehouses": [
# META         {
# META           "id": "9956390f-6fac-4de5-9599-57fe0f66ec21"
# META         },
# META         {
# META           "id": "26d4a8e0-ee35-4760-81f2-163497b579d4"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# ╔════════════════════════════════════════════════════════════╗
# ║ BRONZE ➜ SILVER (FULL LOAD) – CoinGecko  USD, COP & EUR    ║
# ╚════════════════════════════════════════════════════════════╝

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# 1. Leer TODOS los JSON de Bronze
bronze_root = "Files/bronze/cryptos/*/*.json"

df_raw = (
    spark.read
         .option("multiLine", False)
         .json(bronze_root)
         .withColumn(
             "moneda_vs",
             F.regexp_extract(F.input_file_name(),
                              r'/([a-z]{3})_snapshot\.json$', 1))
         .withColumn(
             "fecha_snapshot",
             F.to_date(F.regexp_extract(
                 F.input_file_name(),
                 r'/cryptos/(\d{4}-\d{2}-\d{2})/', 1)))
         .withColumn("ts_ingestion", F.current_timestamp())
)

# 2. Selección + tipado
df_silver = (
    df_raw.select(
        "fecha_snapshot",
        "moneda_vs",
        F.col("id").alias("id_moneda"),
        F.col("symbol").alias("sigla"),
        F.col("name").alias("nombre_moneda"),
        F.col("current_price").cast("double").alias("precio_actual"),
        F.col("market_cap").cast("double").alias("capitalizacion"),
        F.col("total_volume").cast("double").alias("volumen_total"),
        F.col("price_change_percentage_24h").cast("double").alias("variacion_pct_24h"),
        F.col("market_cap_rank").cast("int").alias("puesto_market_cap"),
        F.col("high_24h").cast("double").alias("maximo_24h"),
        F.col("low_24h").cast("double").alias("minimo_24h"),
        F.col("ath").cast("double").alias("max_historico"),
        F.col("ath_change_percentage").cast("double")
          .alias("variacion_pct_ath"),
        F.to_timestamp("ath_date").alias("fecha_ath"),
        F.to_timestamp("last_updated").alias("ultima_actualizacion"),
        "ts_ingestion"
    )
)

# 3. Validaciones mínimas
n_reg = df_silver.count()
assert n_reg >= 250, f"⚠️ Se esperaban ≈300 registros, llegaron {n_reg}"

for col_oblig in ["id_moneda", "precio_actual", "capitalizacion"]:
    assert df_silver.filter(F.col(col_oblig).isNull()).count() == 0, \
        f"⚠️ NULLs en «{col_oblig}»"

# 4. Sobrescribir Silver (FULL LOAD)
silver_path = "abfss://4929c9c5-4f48-4c13-b113-c52b98679915@onelake.dfs.fabric.microsoft.com/26d4a8e0-ee35-4760-81f2-163497b579d4/Tables/dbo/silver_crypto_market" 
#
if DeltaTable.isDeltaTable(spark, silver_path):
    spark.sql("DROP TABLE silver_crypto_market")   # borra metadatos

(df_silver.write
          .format("delta")
          .mode("overwrite")            # recrea los archivos
          .option("overwriteSchema", "true")
          .partitionBy("fecha_snapshot")
          .save(silver_path))

print(f"🟢 Silver recreada con {n_reg} registros.")

# # 5. OPTIMIZE + ZORDER  (sin la columna de partición)
# spark.sql("""
#     OPTIMIZE delta.`Tables/silver_crypto_market`
#     ZORDER BY (id_moneda, moneda_vs)
# """)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
