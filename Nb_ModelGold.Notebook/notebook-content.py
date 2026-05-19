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
# META     },
# META     "warehouse": {
# META       "default_warehouse": "e38d99a6-3075-acb8-46d8-a70009677e58",
# META       "known_warehouses": [
# META         {
# META           "id": "e38d99a6-3075-acb8-46d8-a70009677e58",
# META           "type": "Datawarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# ── Extensiones Spark ↔ Warehouse
import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants

from datetime import datetime
from pyspark.sql import functions as F, types as T

# Rango del calendario
start_date, end_date = "2026-01-01", "2027-12-31"
num_days = (datetime.strptime(end_date, "%Y-%m-%d")
            - datetime.strptime(start_date, "%Y-%m-%d")).days + 1

# DataFrame de fechas
df_fecha = (spark.range(0, num_days)
            .withColumn("i",      F.col("id").cast("int"))
            .withColumn("fecha",  F.expr(f"date_add('{start_date}', i)"))
            .withColumn("fecha_key",
                        F.date_format("fecha", "yyyyMMdd").cast(T.IntegerType()))
            .withColumn("anio",          F.year("fecha"))
            .withColumn("mes",           F.month("fecha"))
            .withColumn("nombre_mes",    F.date_format("fecha", "MMMM"))
            .withColumn("trimestre",     F.quarter("fecha"))
            .withColumn("semana_iso",    F.weekofyear("fecha"))
            .withColumn("dia_mes",       F.dayofmonth("fecha"))
            .withColumn("dia_sem_iso",   F.dayofweek("fecha"))
            .withColumn("nombre_dia",    F.date_format("fecha", "EEEE"))
            .withColumn("es_fin_semana", F.expr("dia_sem_iso IN (6,7)"))
            .drop("id", "i"))

# Full‑load al Warehouse
(df_fecha.write
         .mode("overwrite")            # crea la tabla desde cero
         .synapsesql("WH_Gold.dbo.Dim_Fecha"))

print(f"✅ Dim_Fecha cargada: {df_fecha.count():,} filas "
      f"({start_date} → {end_date}).")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#  Spark ↔ Warehouse extensions
import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants

from pyspark.sql import Window, functions as F

# Cargar Silver
silver = spark.read.table("silver_crypto_market")

# Dimensión Moneda (sólo lo necesario)
dim_moneda = (silver
              .select("id_moneda", "sigla", "nombre_moneda")
              .distinct())

# Clave surrogate reproducible (alfabético por id_moneda)
w = Window.orderBy("id_moneda")
dim_moneda = (dim_moneda
              .withColumn("moneda_id", F.row_number().over(w))
              .select("moneda_id", "id_moneda", "sigla", "nombre_moneda"))

dim_moneda.show(10, truncate=False)

#  FULL‑LOAD al Warehouse
(dim_moneda.write
          .mode("overwrite")
          .synapsesql("WH_Gold.dbo.Dim_Moneda"))

print(f"✅ Dim_Moneda cargada ({dim_moneda.count()} filas).")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants

from pyspark.sql import Window, functions as F

silver  = spark.read.table("silver_crypto_market")

dim_vs = (silver.select("moneda_vs").distinct())

w_vs = Window.orderBy("moneda_vs")
dim_vs = (dim_vs
          .withColumn("vs_id", F.row_number().over(w_vs))
          .select("vs_id", "moneda_vs"))

dim_vs.show()

(dim_vs.write
       .mode("overwrite")
       .synapsesql("WH_Gold.dbo.Dim_Vs"))

print(f"✅ Dim_Vs cargada ({dim_vs.count()} filas).")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants

from pyspark.sql import functions as F

# 1 ▸ cargar materia prima y dimensiones
silver     = spark.read.table("silver_crypto_market")
dim_moneda = spark.read.synapsesql("WH_Gold.dbo.Dim_Moneda")
dim_vs     = spark.read.synapsesql("WH_Gold.dbo.Dim_Vs")

# 2 ▸ enriquecer y seleccionar columnas finales
f_hecho = (
    silver
    .withColumn(
        "fecha_key",
        F.date_format("fecha_snapshot", "yyyyMMdd").cast("int")
    )
    .join(dim_moneda.select("moneda_id", "id_moneda"), on="id_moneda")
    .join(dim_vs.select("vs_id", "moneda_vs"),           on="moneda_vs")
    .withColumn("fecha_ath", F.to_date("fecha_ath"))      # sólo fecha
    .select(
        "fecha_key", "moneda_id", "vs_id",
        "precio_actual", "capitalizacion", "volumen_total",
        "variacion_pct_24h", "maximo_24h", "minimo_24h",
        "puesto_market_cap", "max_historico", "variacion_pct_ath",
        "fecha_ath",              # DATE
        "ultima_actualizacion",   # DATETIME – auditoría
        "ts_ingestion"            # DATETIME – trazabilidad
    )
)

f_hecho.show(10, truncate=False)
print(f"Total filas a escribir: {f_hecho.count():,}")

# 3 ▸ FULL‑LOAD al Warehouse
(f_hecho.write
       .mode("overwrite")
       .synapsesql("WH_Gold.dbo.F_Hecho_Crypto_Diario"))

print("🟢  F_Hecho_Crypto_Diario cargada en WH_Gold.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
