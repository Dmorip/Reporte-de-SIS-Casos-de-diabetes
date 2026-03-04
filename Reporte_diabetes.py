# Databricks notebook source
# Importación de Librerías
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, datediff, floor, when, 
    avg, count, year, month, dayofmonth, round
)

# COMMAND ----------

# Función Principal de ETL (Limpieza y Transformación)
def limpieza_data(spark):
    print("--- Iniciando proceso de transformación de datos (Diabetes SIS) ---")

    # 1. Carga de Datos desde el Catálogo de Databricks
    try:
        df_diabetes = spark.table("workspace.default.pacientes_diabetes")
        print(f"Total de registros iniciales en la base de datos: {df_diabetes.count()}")
    except Exception as e:
        print(f"Error al cargar la tabla: {e}")
        return None
    
    # 2. Transformación de Fechas (Casteo de String a Date)
    print("Convirtiendo formatos de fechas (yyyyMMdd -> Date)...")
    df_fec_transform = (
        df_diabetes
        .withColumn("FECHA_CORTE", to_date(col("FECHA_CORTE").cast("string"), "yyyyMMdd"))
        .withColumn("FECHA_AFILIADOS_SIS", to_date(col("FECHA_AFILIADOS_SIS").cast("string"), "yyyyMMdd"))
        .withColumn("FECHA_PRIMER_DX", to_date(col("FECHA_PRIMER_DX").cast("string"), "yyyyMMdd"))
    )
    # 3. Limpieza de Ruido y Creación de Nuevas Características (Feature Engineering)
    print("Calculando variables clínicas y demográficas...")
    df_limpio = (
        df_fec_transform
        # a) Eliminamos posibles registros duplicados basados en el ID del paciente
        .dropDuplicates(["CODIGO_ANONIMIZADO"])
        
        # b) Filtramos edades incongruentes (errores de digitación en hospitales)
        .filter((col("EDAD") >= 0) & (col("EDAD") <= 120))
        
        # c) Categorizamos la edad en grupos etarios para reporting epidemiológico
        .withColumn(
            "RANGO_EDAD",
            when(col("EDAD") < 18, "Menores de 18")
            .when((col("EDAD") >= 18) & (col("EDAD") < 40), "18-39 años")
            .when((col("EDAD") >= 40) & (col("EDAD") < 60), "40-59 años")
            .otherwise("60 a más")
        )
        
        # d) Calculamos los años de convivencia con la enfermedad
        .withColumn("ANOS_DESDE_DX", floor(datediff(col("FECHA_CORTE"), col("FECHA_PRIMER_DX")) / 365.25))
        # Corregimos posibles negativos si la fecha de corte es anterior al DX por error del sistema
        .withColumn("ANOS_DESDE_DX", when(col("ANOS_DESDE_DX") < 0, 0).otherwise(col("ANOS_DESDE_DX")))
    )
    
    print("--- Proceso ETL finalizado exitosamente ---")
    return df_limpio

# COMMAND ----------

# DBTITLE 1, Ejecución del Pipeline ETL
# Ejecutamos la limpieza UNA SOLA VEZ y guardamos el dataset resultante en memoria
# Esto optimiza drásticamente el rendimiento de todas las consultas posteriores.
df_diabetes_limpio = limpieza_data(spark)
# df_diabetes_limpio.cache()


# COMMAND ----------

# Edad Promedio de los pacientes según el Tipo de Diabetes
print("Edad promedio por diagnóstico clínico:")
df_edad_tipo_dx = df_diabetes_limpio.groupBy("TIPO_DIABETES") \
                                    .agg(round(avg("EDAD"), 1).alias("EDAD_PROMEDIO")) \
                                    .orderBy("EDAD_PROMEDIO")
display(df_edad_tipo_dx)

# COMMAND ----------

# Top 10 Distritos críticos en la capital (LIMA)
df_top_distritos_lima = df_diabetes_limpio.filter(col("DEPARTAMENTO") == "LIMA") \
    .groupBy("DISTRITO") \
    .agg(count("*").alias("CANTIDAD_PACIENTES")) \
    .orderBy(col("CANTIDAD_PACIENTES").desc()) \
    .limit(10)
    
print("Top 10 Distritos con mayor concentración de pacientes en LIMA:")
display(df_top_distritos_lima)

# COMMAND ----------

# Comparativa de Tipos de Diabetes por Departamento Pivot
def top_diabetes_por_departamento(df, departamento):

    return df.filter(col("DEPARTAMENTO") == departamento) \
             .groupBy("TIPO_DIABETES") \
             .agg(count("*").alias("CANTIDAD_CASOS")) \
             .orderBy(col("CANTIDAD_CASOS").desc())

print("Distribución Clínica en LIMA:")
display(top_diabetes_por_departamento(df_diabetes_limpio, "LIMA"))

print("Distribución Clínica en PIURA:")
display(top_diabetes_por_departamento(df_diabetes_limpio, "PIURA"))

print("Distribución Clínica en AMAZONAS:")
display(top_diabetes_por_departamento(df_diabetes_limpio, "AMAZONAS"))

# COMMAND ----------

# Serie de Tiempo: Nuevos Diagnósticos por Año Histórico
df_conteo_anual = df_diabetes_limpio.withColumn("ANIO_DX", year(col("FECHA_PRIMER_DX"))) \
    .groupBy("ANIO_DX") \
    .agg(count("*").alias("NUEVOS_CASOS")) \
    .orderBy("ANIO_DX")

print("Evolución de nuevos casos diagnosticados por año:")
display(df_conteo_anual)

# COMMAND ----------

# DBTITLE 1, Zooming Temporal: Diagnósticos diarios en Enero 2025
df_enero_2025 = df_diabetes_limpio.filter(
    (year(col("FECHA_PRIMER_DX")) == 2025) & 
    (month(col("FECHA_PRIMER_DX")) == 1)
)

df_conteo_diario = df_enero_2025.withColumn("DIA", dayofmonth(col("FECHA_PRIMER_DX"))) \
    .groupBy("DIA") \
    .agg(count("*").alias("NUEVOS_CASOS_DIA")) \
    .orderBy("DIA")

print("Volumen de nuevos diagnósticos diarios durante Enero 2025:")
display(df_conteo_diario)

# COMMAND ----------

#Drill-down: Análisis clínico del día con mayor pico (13 de Enero 2025)
# Aislamos el día de mayor registro para entender la presión sobre el sistema de salud
df_dia_pico = df_diabetes_limpio.filter(
    (year(col("FECHA_PRIMER_DX")) == 2025) &
    (month(col("FECHA_PRIMER_DX")) == 1) &
    (dayofmonth(col("FECHA_PRIMER_DX")) == 13)
)

print("Origen geográfico de los casos detectados el día pico (13 Ene 2025):")
df_distritos_pico = df_dia_pico.groupBy("DISTRITO").count().orderBy(col("count").desc())
display(df_distritos_pico)

# COMMAND ----------

# Perfil de Comorbilidades de los infectados en el día pico
print("Resumen de cargas cruzadas (Comorbilidades) en pacientes del día pico:")
df_resumen_salud = df_dia_pico.select(
    count(when(col("CON_DX_HIPERTENSION") == "SI", 1)).alias("RIESGO_HIPERTENSION"),
    count(when(col("CON_DX_OBESIDAD") == "SI", 1)).alias("RIESGO_OBESIDAD"),
    count(when(col("CON_DX_SALUDMENTAL") == "SI", 1)).alias("RIESGO_SALUD_MENTAL"),
    count("*").alias("TOTAL_CASOS_DETECTADOS_EL_DIA")
)
display(df_resumen_salud)
