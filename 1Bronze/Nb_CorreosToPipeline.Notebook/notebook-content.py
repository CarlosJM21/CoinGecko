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

from pyspark.sql.functions import col

# Leer tabla desde el Lakehouse
df_destinatarios = spark.read.format("delta").load("Tables/Parametros.destinatarios")

# Recoger todos los correos sin importar el cargo
correos = [row["correo"] for row in df_destinatarios.collect()]

# Eliminar duplicados (opcional)
correos_unicos = sorted(set(correos))

# Unirlos en una cadena separada por coma
todos_los_correos = ",".join(correos_unicos)

# Devolver solo el string como salida
mssparkutils.notebook.exit(todos_los_correos)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
