CREATE TABLE [dbo].[Dim_Fecha] (

	[fecha] date NULL, 
	[fecha_key] int NULL, 
	[anio] int NULL, 
	[mes] int NULL, 
	[nombre_mes] varchar(max) NULL, 
	[trimestre] int NULL, 
	[semana_iso] int NULL, 
	[dia_mes] int NULL, 
	[dia_sem_iso] int NULL, 
	[nombre_dia] varchar(max) NULL, 
	[es_fin_semana] bit NULL
);