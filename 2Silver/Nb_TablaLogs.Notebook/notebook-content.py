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

from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# Definir el esquema de la tabla
schema = StructType([
StructField("FechaHora", TimestampType(), True),  #Fecha y hora con precisión
StructField("Mensaje", StringType(), True), # Mensaje del error
StructField("Origen", StringType(), True),  # Fuente del error
StructField("PrunDate", StringType(), True), # Fecha lógica como string
StructField("IdPipeline",StringType(),True), # ID DEL PIPELINE
StructField("MonedaError",StringType(),True)  # Moneda Error en que bucle fallo
])

# Crear un DataFrame vacío con ese esquema
df_vacio = spark.createDataFrame([], schema)


# Guardar como tabla Delta en el esquema Logs
df_vacio.write.mode("overwrite").format("delta").save("Tables/Logs.errores")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
