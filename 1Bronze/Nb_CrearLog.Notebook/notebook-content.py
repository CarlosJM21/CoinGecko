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

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import lit

# Datos de insercion

fecha_hora = datetime.utcnow()
mensaje = "{message}"  #"Error: No se encontró la carpeta esperada en la ruta."
origen = "PL_Bronze"

# Crear esquema con nombres que coincidan exactamente con la tabla
schema = StructType([
	StructField("FechaHora", TimestampType(), True),
	StructField("Mensaje", StringType(), True),
	StructField("Origen", StringType(), True),
	StructField("IdPipeline", StringType(), True),
	StructField("MonedaError", StringType(), True)
])

# Crear Dataframe parcial
df_error = spark.createDataFrame([(fecha_hora, mensaje, origen, {IdPipeline}, {p_currency})], schema)

# Añadir la columa faltante
# df_error = df_error.withColumn("p_run_date", lit(p_run date)

# Insertar el registro en la tabla Logs.errores
df_error.write.mode("append").format("delta").save("Tables/Logs.errores")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
