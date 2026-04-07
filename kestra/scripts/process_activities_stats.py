import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, month, year, when, concat, lit, to_timestamp, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

# --- CONFIGURATION ---
FIRST_MONTH = int(os.getenv("FIRST_MONTH", 6)) # Par défaut Juin (6)

spark = SparkSession.builder \
    .appName("ActivitiesSocialYearStats") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# --- SCHÉMA ---
activity_schema = StructType([
    StructField("id", StringType()),
    StructField("employee_id", StringType()),
    StructField("sport", StringType()),
    StructField("begin_date", LongType()), # Timestamp (ms ou µs depending on Debezium)
    StructField("duration_sec", IntegerType()),
    StructField("distance_meters", IntegerType())
])

payload_schema = StructType([
    StructField("payload", StructType([
        StructField("after", activity_schema)
    ]))
])

# --- Stream reading  ---
df_raw = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "redpanda:9092") \
    .option("subscribe", "cdc.public.sports_activities") \
    .option("failOnDataLoss", "false") \
    .load()

# --- TRANSFORMATIONS MÉTIER ---
df_processed = df_raw.select(from_json(col("value").cast("string"), payload_schema).alias("data")) \
    .select("data.payload.after.*") \
    .filter(col("id").isNotNull()) 

# Conversion du timestamp Debezium en date réelle
# Note: Debezium Postgres envoie souvent des microsecondes, d'où le / 1000000
df_with_date = df_processed.withColumn("activity_date", from_unixtime(col("begin_date") / 1000000).cast("timestamp"))

# Calcul de l'Année Sociale (Campaign Year)
# Exemple: si FIRST_MONTH = 9, une activité en Octobre 2023 -> "2023-2024"
# Une activité en Janvier 2024 -> "2023-2024"
df_social = df_with_date.withColumn("start_year", 
    when(month(col("activity_date")) >= FIRST_MONTH, year(col("activity_date")))
    .otherwise(year(col("activity_date")) - 1)
).withColumn("social_year", concat(col("start_year"), lit("-"), col("start_year") + 1)).withColumn("_event_timestamp", current_timestamp())

# --- ÉCRITURE DELTA ---
query = df_social.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/opt/spark/checkpoints/activities_stats_gold") \
    .start("/opt/spark/data/delta-lake/activities_stats_gold")

query.awaitTermination()