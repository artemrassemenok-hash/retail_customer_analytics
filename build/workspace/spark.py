# %% [markdown]
# ### Start SparkSession

# %%
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime
import pandas as pd

# Самая простая инициализация
spark = SparkSession.builder \
    .appName("RetailAnalysis") \
    .config("spark.sql.shuffle.partitions", "50") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("Spark готов")


# %%

import os

csv_path = "data/OnlineRetail.csv"

if os.path.exists(csv_path):
    print(f"CSV файл уже существует: {csv_path}")
    print("Можно сразу использовать в Spark!")
    
    # Проверяем
    file_size = os.path.getsize(csv_path) / (1024*1024)
    print(f"Размер файла: {file_size:.1f} MB")
    
else:
    print("Нужно установить openpyxl для конвертации Excel")
    print("Запусти: !pip install openpyxl")

# %%

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. Инициализация Spark
spark = SparkSession.builder \
    .appName("RetailAnalysis") \
    .config("spark.sql.shuffle.partitions", "50") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Чтение CSV (у тебя уже есть!)
df = spark.read.csv("data/OnlineRetail.csv", 
                    header=True, 
                    inferSchema=True)

print(f"Загружено {df.count():,} строк")
print(f"Колонок: {len(df.columns)}")

print("\nПервые 5 строк:")
df.show(5)

# 3. Сохраняем для RFM
raw_df = df
print("Данные готовы для анализа!")

# %%

from pyspark.sql import functions as F

# 1. Очистка данных
print("\n1. Очистка данных...")
clean_df = raw_df.filter(
    (F.col("Quantity") > 0) &
    (F.col("UnitPrice") > 0) &
    (F.col("CustomerID").isNotNull())
)

print(f"   Было: {raw_df.count():,} строк")
print(f"   Стало: {clean_df.count():,} строк")

# 2. Сначала посмотрим формат дат в данных
print("\n2. Анализируем формат дат...")
print("   Примеры дат из файла:")
clean_df.select("InvoiceDate").limit(5).show(truncate=False)

# 3. Преобразование дат (правильный формат!)
print("\n3. Преобразование данных...")

# Попробуем разные форматы дат
try:
    # Формат 1: M/d/yy H:mm (год 10 вместо 2010)
    processed_df = clean_df.withColumn(
        "InvoiceDate",
        F.to_timestamp(F.col("InvoiceDate"), "M/d/yy H:mm")
    )
except Exception as e1:
    print(f"   Формат M/d/yy не сработал: {e1}")
    try:
        # Формат 2: d/M/yy H:mm (европейский)
        processed_df = clean_df.withColumn(
            "InvoiceDate",
            F.to_timestamp(F.col("InvoiceDate"), "d/M/yy H:mm")
        )
    except Exception as e2:
        print(f"   Формат d/M/yy не сработал: {e2}")
        # Формат 3: Просто как строка, потом вручную
        processed_df = clean_df.withColumn(
            "InvoiceDate",
            F.concat(
                F.substring(F.col("InvoiceDate"), 1, 6),
                F.lit("20"),
                F.substring(F.col("InvoiceDate"), 7, 2),
                F.substring(F.col("InvoiceDate"), 9, 100)
            )
        ).withColumn(
            "InvoiceDate",
            F.to_timestamp(F.col("InvoiceDate"), "M/d/yyyy H:mm")
        )

# Добавляем расчет цены
processed_df = processed_df.withColumn(
    "TotalPrice",
    F.col("Quantity") * F.col("UnitPrice")
)

print("   Пример преобразованных данных:")
processed_df.select("InvoiceNo", "InvoiceDate", "Quantity", "UnitPrice", "TotalPrice").show(5)

# 4. RFM расчет
print("\n4. Расчет RFM метрик...")

# Находим максимальную дату в данных
max_date = processed_df.agg(F.max("InvoiceDate")).collect()[0][0]
print(f"   Последняя дата в данных: {max_date}")
print(f"   Используем для recency: {max_date}")

rfm_df = processed_df.groupBy("CustomerID").agg(
    # Recency: дни с последней покупки
    F.datediff(F.lit(max_date), F.max("InvoiceDate")).alias("recency_days"),
    
    # Frequency: количество уникальных транзакций
    F.countDistinct("InvoiceNo").alias("frequency"),
    
    # Monetary: общая сумма покупок
    F.sum("TotalPrice").alias("monetary"),
    
    # Дополнительные метрики
    F.count("*").alias("total_items"),
    F.min("InvoiceDate").alias("first_purchase"),
    F.max("InvoiceDate").alias("last_purchase")
).filter(F.col("monetary") > 0)

print(f"   Проанализировано клиентов: {rfm_df.count():,}")

print("\nТоп-10 клиентов по доходности:")
rfm_df.orderBy(F.col("monetary").desc()).show(10)

print("\nСтатистика RFM:")
rfm_df.select(
    F.avg("recency_days").alias("avg_recency"),
    F.avg("frequency").alias("avg_frequency"),
    F.avg("monetary").alias("avg_monetary"),
    F.sum("monetary").alias("total_revenue")
).show()

print("RFM анализ завершен!")

# %%
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 1. RFM Scoring (1-5) - исправленный
window_r = Window.orderBy(F.col("recency_days").asc())
window_f = Window.orderBy(F.col("frequency").desc())
window_m = Window.orderBy(F.col("monetary").desc())

# Определяем разные пути для сохранения
output_path_detailed = "rfm_results/rfm_results_detailed"
output_path_stats = "rfm_results/rfm_results_stats"

rfm_scored = rfm_df.withColumn("r_score", F.ntile(5).over(window_r)) \
                   .withColumn("f_score", F.ntile(5).over(window_f)) \
                   .withColumn("m_score", F.ntile(5).over(window_m))

# 2. Сегментация клиентов
rfm_segmented = rfm_scored.withColumn("segment",
    F.when((F.col("r_score") >= 4) & (F.col("f_score") >= 4), "Champions")
     .when((F.col("r_score") >= 4) & (F.col("f_score") >= 3), "Loyal")
     .when((F.col("r_score") >= 4), "New")
     .when((F.col("r_score") >= 3) & (F.col("f_score") >= 3), "Potential")
     .when((F.col("r_score") <= 2) & (F.col("f_score") >= 4), "At Risk")
     .when((F.col("r_score") <= 2) & (F.col("f_score") <= 2), "Lost")
     .otherwise("Others")
)


segment_stats = rfm_segmented.groupBy("segment").agg(
    F.count("*").alias("customer_count"),
    F.sum("monetary").alias("total_revenue"),
    F.round(F.avg("monetary"), 2).alias("avg_revenue_per_customer"),
    F.round(F.avg("recency_days"), 1).alias("avg_recency_days")
).orderBy(F.col("total_revenue").desc())

# Показываем результаты
segment_stats.show(truncate=False)

# 3. Сохраняем для дальнейшего использования
print("Сохраняю результаты анализа...")

# Сохраняем детальные данные по клиентам
rfm_segmented.write \
    .option("header", "true") \
    .mode("overwrite") \
    .csv(output_path_detailed)

# Сохраняем агрегированную статистику по сегментам (в отдельную папку)
segment_stats.write \
    .option("header", "true") \
    .mode("overwrite") \
    .csv(output_path_stats)

print("✅ Данные сохранены:")
print(f"   - {output_path_detailed} (детальные данные по клиентам)")
print(f"   - {output_path_stats} (агрегированная статистика по сегментам)")

# 4. Выводим итоговую статистику
total_customers = rfm_segmented.count()
total_revenue = rfm_segmented.agg(F.sum("monetary")).collect()[0][0]


