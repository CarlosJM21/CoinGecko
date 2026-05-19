CREATE TABLE [dbo].[F_Hecho_Crypto_Diario] (

	[fecha_key] int NULL, 
	[moneda_id] int NULL, 
	[vs_id] int NULL, 
	[precio_actual] float NULL, 
	[capitalizacion] float NULL, 
	[volumen_total] float NULL, 
	[variacion_pct_24h] float NULL, 
	[maximo_24h] float NULL, 
	[minimo_24h] float NULL, 
	[puesto_market_cap] int NULL, 
	[max_historico] float NULL, 
	[variacion_pct_ath] float NULL, 
	[fecha_ath] date NULL, 
	[ultima_actualizacion] datetime2(6) NULL, 
	[ts_ingestion] datetime2(6) NULL
);