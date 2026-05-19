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

from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType

# Definir esquema
schema = StructType([
	StructField("nombre", StringType(), True),
	StructField("correo", StringType(), True),
	StructField("Cargo", StringType(), True)
])

# Datos de ejemplo
data = [
Row(nombre="Carlos", correo="Carlos@gmail.com", Cargo="BI"),
Row(nombre="Pedro", correo="Pedro1@yopmail.com", Cargo="Finanzas"),
#Row(nombre="Soporte- BI", correo="soporte.bigempresa.com", tipo="alerta")
]

# Crear Dataframe y guardar como tabla
df_destinatarios = spark.createDataFrame(data, schema)
df_destinatarios .write.mode("overwrite").format("delta").save("Tables/Parametros.destinatarios")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
