"""
Procesamiento en batch con Apache Spark (PySpark)

Conjunto de datos: Temperatura Ambiente del Aire (IDEAM), Datos Abiertos Colombia
Fuente: https://www.datos.gov.co/Ambiente-y-Desarrollo-Sostenible/Temperatura-Ambiente-del-Aire/sbwg-7ju4

Problema: identificar en que departamentos, estaciones y horas del dia se concentran
las temperaturas mas altas, para apoyar alertas tempranas de calor extremo.

Uso:
    python3 batch_temperatura.py
    python3 batch_temperatura.py <ruta_del_csv>     (opcional, por ejemplo un archivo local)
"""
import os
import sys

from pyspark.sql import SparkSession, functions as F

# ---------------------------------------------------------------------------
# Parametros
# ---------------------------------------------------------------------------
RUTA_ENTRADA = (
    sys.argv[1]
    if len(sys.argv) > 1
    else "hdfs://localhost:9000/Tarea3_Temperatura/temperatura.csv"
)
CARPETA_SALIDA = os.path.expanduser("~/resultados_batch")
UMBRAL_CALOR = 35.0          # grados Celsius para considerar un evento de calor
TEMP_MIN, TEMP_MAX = -10.0, 50.0   # rango fisicamente razonable para Colombia

os.makedirs(CARPETA_SALIDA, exist_ok=True)

spark = (
    SparkSession.builder.appName("Tarea3_Batch_Temperatura")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# 1. Carga de datos desde la fuente original (CSV almacenado en HDFS)
# ---------------------------------------------------------------------------
print("\n=== 1. CARGA DE DATOS ===")
df_raw = spark.read.format("csv").option("header", "true").load(RUTA_ENTRADA)
total_raw = df_raw.count()
print(f"Registros cargados: {total_raw:,}")
print("Esquema original (todo llega como texto):")
df_raw.printSchema()
df_raw.show(5, truncate=False)

# ---------------------------------------------------------------------------
# 2. Limpieza y transformacion
# ---------------------------------------------------------------------------
print("\n=== 2. LIMPIEZA Y TRANSFORMACION ===")

# Si alguna columna descriptiva no viene en el archivo, se crea con un valor por defecto
for columna in ["departamento", "municipio", "nombreestacion"]:
    if columna not in df_raw.columns:
        df_raw = df_raw.withColumn(columna, F.lit("SIN DATO"))

df = (
    df_raw
    .withColumn("valor", F.col("valorobservado").cast("double"))
    .withColumn("fecha_hora", F.to_timestamp("fechaobservacion"))
    .withColumn("departamento", F.upper(F.trim(F.col("departamento"))))
    .withColumn("municipio", F.upper(F.trim(F.col("municipio"))))
    .withColumn("nombreestacion", F.trim(F.col("nombreestacion")))
)

# a) Eliminar registros sin valor, sin fecha o sin estacion
df = df.dropna(subset=["valor", "fecha_hora", "codigoestacion"])
total_sin_nulos = df.count()

# b) Eliminar duplicados (misma estacion, mismo sensor y misma fecha-hora)
claves = [c for c in ["codigoestacion", "codigosensor", "fecha_hora"] if c in df.columns]
df = df.dropDuplicates(claves)

# c) Eliminar lecturas fuera de un rango fisicamente razonable (datos crudos sin validar)
df = df.filter((F.col("valor") >= TEMP_MIN) & (F.col("valor") <= TEMP_MAX))

# d) Variables derivadas para el analisis temporal
df = (
    df.withColumn("anio", F.year("fecha_hora"))
    .withColumn("mes", F.month("fecha_hora"))
    .withColumn("hora", F.hour("fecha_hora"))
    .withColumn("evento_calor", (F.col("valor") >= UMBRAL_CALOR).cast("int"))
)
df.cache()
total_final = df.count()

print(f"Registros iniciales:                 {total_raw:,}")
print(f"Eliminados por nulos:                {total_raw - total_sin_nulos:,}")
print(f"Eliminados por duplicados/fuera de rango: {total_sin_nulos - total_final:,}")
print(f"Registros finales para el analisis:  {total_final:,}")
print("Esquema despues de la limpieza (columnas nuevas: valor, fecha_hora, anio, mes, hora):")
df.select("codigoestacion", "departamento", "municipio", "valor", "fecha_hora",
          "anio", "mes", "hora", "evento_calor").printSchema()

# ---------------------------------------------------------------------------
# 3. Analisis exploratorio de datos (EDA)
# ---------------------------------------------------------------------------
print("\n=== 3. ANALISIS EXPLORATORIO ===")

print("Estadisticas descriptivas de la temperatura (grados C):")
df.select("valor").summary("count", "mean", "stddev", "min", "25%", "50%", "75%", "max").show()

rango = df.agg(F.min("fecha_hora").alias("desde"), F.max("fecha_hora").alias("hasta"),
               F.countDistinct("codigoestacion").alias("estaciones"),
               F.countDistinct("departamento").alias("departamentos"))
rango.show(truncate=False)

# Ejemplo con RDD: conteo de registros por departamento (map + reduceByKey)
print("Registros por departamento (RDD con map y reduceByKey), top 5:")
rdd_conteo = (df.select("departamento").rdd
              .map(lambda fila: (fila["departamento"], 1))
              .reduceByKey(lambda a, b: a + b))
for depto, cantidad in rdd_conteo.takeOrdered(5, key=lambda x: -x[1]):
    print(f"  {depto}: {cantidad:,}")

# Temperatura por departamento
por_depto = (df.groupBy("departamento")
             .agg(F.round(F.avg("valor"), 2).alias("temp_promedio"),
                  F.round(F.max("valor"), 2).alias("temp_maxima"),
                  F.round(F.min("valor"), 2).alias("temp_minima"),
                  F.count("*").alias("lecturas"))
             .orderBy(F.desc("temp_promedio")))
print("Temperatura por departamento (top 10 mas calidos):")
por_depto.show(10, truncate=False)

# Estaciones mas calurosas (solo las que tienen suficientes lecturas)
estaciones = (df.groupBy("codigoestacion", "nombreestacion", "departamento")
              .agg(F.round(F.avg("valor"), 2).alias("temp_promedio"),
                   F.round(F.max("valor"), 2).alias("temp_maxima"),
                   F.count("*").alias("lecturas"))
              .filter(F.col("lecturas") >= 100)
              .orderBy(F.desc("temp_promedio")))
print("Top 10 estaciones con mayor temperatura promedio:")
estaciones.show(10, truncate=False)

# Comportamiento por hora del dia y por mes
por_hora = df.groupBy("hora").agg(F.round(F.avg("valor"), 2).alias("temp_promedio")).orderBy("hora")
por_mes = df.groupBy("mes").agg(F.round(F.avg("valor"), 2).alias("temp_promedio")).orderBy("mes")
print("Temperatura promedio por hora del dia:")
por_hora.show(24)

# Eventos de calor (>= umbral)
calor = (df.groupBy("departamento")
         .agg(F.sum("evento_calor").alias("eventos_calor"), F.count("*").alias("lecturas"))
         .withColumn("porcentaje", F.round(F.col("eventos_calor") * 100 / F.col("lecturas"), 2))
         .orderBy(F.desc("eventos_calor")))
print(f"Lecturas con temperatura >= {UMBRAL_CALOR} C por departamento (top 10):")
calor.show(10, truncate=False)

# ---------------------------------------------------------------------------
# 4. Visualizacion de resultados (graficas guardadas como imagenes PNG)
# ---------------------------------------------------------------------------
print("\n=== 4. VISUALIZACION ===")
try:
    import matplotlib
    matplotlib.use("Agg")   # sin entorno grafico, solo guarda archivos
    import matplotlib.pyplot as plt

    pd_depto = por_depto.limit(15).toPandas().sort_values("temp_promedio")
    pd_hora = por_hora.toPandas()
    pd_mes = por_mes.toPandas()
    pd_calor = calor.limit(10).toPandas().sort_values("eventos_calor")

    # Grafica 1: temperatura promedio por departamento
    plt.figure(figsize=(9, 6))
    plt.barh(pd_depto["departamento"], pd_depto["temp_promedio"], color="#d9534f")
    plt.xlabel("Temperatura promedio (C)")
    plt.title("Los 15 departamentos con mayor temperatura promedio")
    plt.tight_layout()
    plt.savefig(os.path.join(CARPETA_SALIDA, "1_temperatura_por_departamento.png"), dpi=120)
    plt.close()

    # Grafica 2: temperatura promedio por hora del dia
    plt.figure(figsize=(9, 5))
    plt.plot(pd_hora["hora"], pd_hora["temp_promedio"], marker="o", color="#f0ad4e")
    plt.xticks(range(0, 24))
    plt.xlabel("Hora del dia")
    plt.ylabel("Temperatura promedio (C)")
    plt.title("Comportamiento de la temperatura durante el dia")
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(os.path.join(CARPETA_SALIDA, "2_temperatura_por_hora.png"), dpi=120)
    plt.close()

    # Grafica 3: temperatura promedio por mes
    plt.figure(figsize=(9, 5))
    plt.bar(pd_mes["mes"].astype(str), pd_mes["temp_promedio"], color="#5bc0de")
    plt.xlabel("Mes")
    plt.ylabel("Temperatura promedio (C)")
    plt.title("Temperatura promedio por mes")
    plt.tight_layout()
    plt.savefig(os.path.join(CARPETA_SALIDA, "3_temperatura_por_mes.png"), dpi=120)
    plt.close()

    # Grafica 4: eventos de calor por departamento
    plt.figure(figsize=(9, 5))
    plt.barh(pd_calor["departamento"], pd_calor["eventos_calor"], color="#c9302c")
    plt.xlabel(f"Lecturas con temperatura >= {UMBRAL_CALOR} C")
    plt.title("Departamentos con mas eventos de calor")
    plt.tight_layout()
    plt.savefig(os.path.join(CARPETA_SALIDA, "4_eventos_de_calor.png"), dpi=120)
    plt.close()

    print(f"Graficas guardadas en: {CARPETA_SALIDA}")
except ImportError:
    print("matplotlib o pandas no estan instalados. Ejecute: pip install matplotlib pandas")

# Tablas de resultados en CSV (archivos pequenos)
por_depto.toPandas().to_csv(os.path.join(CARPETA_SALIDA, "resumen_por_departamento.csv"), index=False)
estaciones.limit(20).toPandas().to_csv(os.path.join(CARPETA_SALIDA, "top_estaciones.csv"), index=False)

print("\nProcesamiento en batch finalizado.")
spark.stop()
