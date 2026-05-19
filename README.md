# CoinGecko Full Carga- Microsoft Fabric End-to-End Solution

Este repositorio contiene un ejercicio de Full Load de fabric que la arquitectura analítica medallon para gnerar la solución 
de ingeniería de datos *End-to-End* para la ingesta, procesamiento y visualización de métricas del mercado de criptomonedas, 
utilizando la API de **CoinGecko** como origen de datos y **Microsoft Fabric** como plataforma centralizada bajo un esquema 
de carga completa (*FULL Load*).

## 🛠️ Fundamentos Tecnológicos del Ecosistema

El pipeline de datos y las estrategias de procesamiento implementadas se basan en los cuatro pilares del Big Data moderno:

### 1. 🗄️ Big Data Basics
Gestión de conjuntos de datos masivos e históricos que por su volumen, velocidad y variedad superan las capacidades tradicionales.
*   **Estructurado:** Tablas relacionales optimizadas para analítica.
*   **No Estructurado:** Logs y almacenamiento de archivos planos en el Data Lake.
*   **Semiestructurado:** Respuestas e intercambios de mensajes en formato **JSON** nativo de la API de CoinGecko.
*   **Gran Volumen:** Estrategias escalables para el histórico diario del mercado cripto.

### 2. ⚡ Apache Spark
Motor de cómputo distribuido en memoria utilizado para la transformación a gran escala.
*   **Procesamiento en Memoria:** Minimiza los costos de lectura/escritura en disco acelerando los notebooks.
*   **Análisis Rápido:** Optimización de agregaciones y cálculos financieros complejos de las últimas 24 horas.
*   **Ecosistema Integral:** Uso de Spark SQL y DataFrames estructurados para moldear la data.

### 3. 🐘 Hadoop Architecture
Principios distribuidos que sientan las bases de los sistemas de almacenamiento modernos.
*   **HDFS & Clusters:** Conceptos de almacenamiento distribuido y tolerancia a fallos replicados en la infraestructura Cloud.
*   **Escalabilidad:** Capacidad de absorber el crecimiento horizontal del histórico de cotizaciones de manera transparente.

### 4. 🔄 Procesamiento ETL / ELT
Estrategias avanzadas de integración de datos.
*   **ETL / ELT:** Extracción desde la API, carga en bruto al Data Lake y posterior transformación en la nube para maximizar el
*   rendimiento del cómputo.
*   **Flujo de Datos:** Orquestación automatizada de tuberías (*pipelines*) libres de cuellos de botella.

---

## 🧩 Implementación en Microsoft Fabric

### 🎯 ¿Por qué Microsoft Fabric?
*   **Unificación:** Consolida ingesta, ingeniería y BI en un único entorno SaaS, eliminando la complejidad de conectar múltiples
*     plataformas aisladas.
*   **Centralización del Dato:** A través de **OneLake**, evita la duplicación y copia innecesaria de datos entre los distintos
*     equipos analíticos.
*   **Gobernanza Completa:** Mantiene el acceso a herramientas potentes sin perder el control de la seguridad ni disparar los costos
*     de infraestructura.

### 🏗️ Arquitectura y Capas Operativas
El flujo se organiza de forma transversal utilizando los siguientes componentes nativos:
*   **Datastores:** Repositorio único en **OneLake** configurado mediante **Lakehouses** y **Data Warehouses**.
*   **Compute:** Ejecución optimizada utilizando motores **Spark** para transformación y **Warehouse Capacity** para consultas
    SQL analíticas.
*   **Orchestration:** Automatización de flujos continuos mediante Data Factory **Pipelines**.
*   **Serving:** Exposición del modelo de datos listo para el consumo del negocio mediante **Power BI** y endpoints de consulta.

---

## 🏅 Arquitectura Medallón (Flujo End-to-End)

El repositorio implementa un patrón de diseño lógico por fases para el refinamiento progresivo de la calidad de los datos analíticos:

1.  **Capa Bronce (Ingesta):** Un proceso de **Pipeline** extrae la información del mercado cripto desde la Web App de CoinGecko.
      Un componente **Copy Job** almacena los datos exactamente en su formato **JSON** original dentro de carpetas (`Folder`)
      en el **Lakehouse**.
2.  **Capa Plata (Curación):** Mediante un **Notebook de Spark**, los archivos JSON crudos se leen, limpian y estructuran. En esta
      fase se realiza la remoción de registros duplicados, tipado de datos y estandarización completa, guardando el resultado en
      **tablas en formato Delta**.
3.  **Capa Oro (Modelo Semántico):** Utilizando código optimizado en un **Notebook con Apache Spark**, se toman las tablas Delta
      curadas y se genera el modelo dimensional definitivo dentro del **Data Warehouse**, quedando disponible de forma inmediata
      para construir el informe interactivo en **Power BI**.

### 👥 Roles Clave en el Proyecto
*   **Arquitecto / Ingeniero de Datos:** Diseña y automatiza los pipelines de movimiento de datos (Capa Bronce a Plata).
*   **Científico de Datos / Ingeniero ML:** Explota las tablas limpias de la capa plata para crear análisis predictivos del mercado
*   **Desarrollador BI / Analista de Negocio:** Diseña el modelo en estrella de la capa oro y genera cuadros de mando para responder
     preguntas clave (*"¿Cuánto varió el volumen?", "¿Por qué bajó la capitalización?"*).

---

## ⭐ Plan: Modelo de Datos en Estrella (Capa Oro)

Para analizar el comportamiento diario del mercado, la capa Oro expone un diseño relacional optimizado para lecturas de alta velocidad:

*   **Tabla de Hechos (`F_Hecho_Crypto_Diario`):** Almacena las métricas numéricas y cuantitativas del negocio: capitalización de mercado,
       precio actual, máximos/mínimos de las últimas 24 horas, variaciones porcentuales, volumen total diario y marcas de tiempo de
       ingesta (`ts_ingestion`).
*   **Tablas de Dimensiones (Relaciones 1:N):**
    *   **`Dim_Fecha`:** Desglose del tiempo (año, mes, día del mes, día de la semana ISO, indicador de fin de semana).
    *   **`Dim_Moneda`:** Datos descriptivos del activo (id de la moneda, nombre completo y sigla/ticker).
    *   **`Dim_Vs`:** Moneda de contraparte utilizada para la comparación de la tasa de cambio (`moneda_vs`).

---

## 📁 Estructura del Repositorio

El código del proyecto se organiza bajo la siguiente estructura modular:

```text
├── .gitignore
├── README.md
└── 00-microsoft-fabric-solution/ 
    ├── 01-datastores/           # Definiciones de OneLake, Lakehouse y Warehouse
    ├── 02-bronce-ingestion/     # Pipelines de Data Factory, llamadas a la API y Copy Jobs (JSON)
    ├── 03-plata-curation/       # Notebooks para tratamiento, limpieza y conversión a tablas Delta
    ├── 04-oro-modeling/         # Código Spark para la creación de Hechos y Dimensiones (Modelo Estrella)
    └── 05-serving-bi/           # Modelos semánticos e informes visuales en Power BI
```

---

## 🚀 Comenzando

### Prerrequisitos
*   Cuenta activa en el entorno de **Microsoft Fabric** con una capacidad asignada (Trial o Premium).
*   Acceso a la API pública de **CoinGecko**.

### Despliegue de la Solución
1. Clona el repositorio en tu espacio de trabajo local o directamente conéctalo a tu Workspace de Fabric usando la integración con Git:
   ```bash
   git clone https://github.com
   ```
2. Importa los archivos de la carpeta `00-microsoft-fabric-solution/` dentro de tu entorno de Fabric.
3. Ejecuta el pipeline de la capa `02-bronce-ingestion` para iniciar la carga completa (*FULL Load*) de los datos.
