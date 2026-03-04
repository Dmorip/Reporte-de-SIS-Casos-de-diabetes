# Proyecto: Análisis Epidemiológico de Pacientes con Diabetes (SIS - Perú)

## 📌 Descripción del Proyecto
Este proyecto es un pipeline de datos desarrollado para realizar un análisis integral de los pacientes afiliados al Seguro Integral de Salud (SIS) en Perú con diagnóstico de Diabetes. Utilizando **Apache Spark (PySpark)** en el entorno de **Databricks**, el proyecto procesa datos en volumen para limpiar, transformar y extraer insights valiosos ('Data Insights') sobre esta enfermedad crónica a nivel poblacional.

El objetivo principal es responder a preguntas de negocio del rubro de Salud Pública (demográficas y temporales) para comprender el impacto de la enfermedad en distintas regiones, los grupos etarios más vulnerables y evaluar la carga de comorbilidades (hipertensión, obesidad, salud mental) durante los picos críticos de diagnóstico.

## 🚀 Tecnologías y Herramientas
- **Lenguaje Core:** Python 3
- **Procesamiento Big Data:** Apache Spark (PySpark SQL / DataFrame API)
- **Entorno Cloud:** Databricks
- **Técnicas aplicadas:** Construcción de ETLs, Limpieza de Datos (Data Cleaning), Optimización en Memoria (`.cache()`), Ingeniería de Características (Feature Engineering).

## 📊 Arquitectura del Análisis

### 1. Extracción y Limpieza (ETL)
- **Ingesta:** Lectura optimizada del catálogo de Unity/Hive Metastore (`workspace.default`).
- **Data Cleaning:**
  - Deduplicación de pacientes utilizando identificadores únicos anonimizados.
  - Transformación y "Casteo" de Strings a formatos nativos de Fecha (Date).
  - Filtrado de valores atípicos clínicos (Outliers como edades negativas o mayores a 120 años).
- **Feature Engineering:**
  - Discretización de datos numéricos (Creación de Categorías de Rango Etario).
  - Cálculo matemático de años de convivencia con la condición.

### 2. Análisis Exploratorio de Datos Poblacional (EDA)
Mediante expresiones de agregación de Spark (`groupBy`, `agg`, `when`), el proyecto genera respuestas tabulares a:
- **Perfil Demográfico:**
  - Edad promedio calculada de forma dinámica por variación clínica (Tipo 1, Tipo 2, No especificada).
  - Rankings (Top N) de concentración de pacientes a nivel Distrital y Departamental.
- **Análisis Epidemiológico Temporal:**
  - Curva evolutiva histórica de nuevos diagnósticos agrupados por año.
  - *Drill-down* mensual y diario para aislar picos atípicos en el sistema hospitalario (Ej: Enero 2025).
- **Análisis Clínico y Comorbilidades:**
  - Matriz de cálculo de riesgos cruzados (Pacientes que simultáneamente sufren hipertensión, obesidad o problemas de salud mental) aislados en ventanas de alta incidencia.

## 💡 Cómo Ejecutar este Código en Databricks
Este código está diseñado para ser copiado y ejecutado modularmente como un **Notebook nativo de Databricks**.

1. Levanta un clúster (Compute) estándar en Databricks.
2. Copia el código de `src/diabetes_portafolio.py` en distintas celdas delimitadas por el comentario `# COMMAND ----------`.
3. Asegúrate de poseer la tabla fuente de datos sanitarios montada en tu entorno (ej. `workspace.default.pacientes_diabetes`).
4. **Ejecuta la celda de limpieza una sola vez.** El script emplea buenas prácticas haciendo `.cache()` del DataFrame limpio, asegurando que las posteriores 8 celdas de consultas se resuelvan en fracción de segundos usando la memoria RAM del clúster.

---

*Desarrollado por Daniel como demostración técnica de capacidades en Data Engineering y Big Data Analytics con Apache Spark.*
