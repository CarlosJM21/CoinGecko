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
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

#Parámetros inyectados por el pipeline (no los declares manualmente en Fabric)
# p_run_date =...
# p_force_reingest = ...

# Ruta base esperada (sin /lakehouse ni /root)
base_path = f"Files/bronze/cryptos/{p_run_date}/"

# Verifica si la carpeta existe en OneLake
def carpeta_existe(path):
	try:
		msparkutils.fs.Is(path)
		return True
	except:
		return False

# Lógica principal
if carpeta_existe(base_path):
	if not p_force_reingest:
		print(f" ERROR: La carpeta '{base_path}' ya existe.")
		raise Exception(f" Abortando: snapshot para '{p_run_date}' ya fue procesado. Usa p_force_reingest=true para reprocesar.")
	else:
		print(f" Carpeta '{base_path}' existe pero será eliminada (p_force_reingest = true).")
		mssparkutils.fs.rm(base_path, True)
		print("Carpeta eliminada. Continuamos.")
else:
    print(f" OK: La carpeta '{base_path}' no existe. Continuamos con la ingesta.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

