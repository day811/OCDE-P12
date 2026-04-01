import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf, lit, when, current_timestamp, month, year
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
from cryptography.fernet import Fernet

# --- CONFIGURATION & PARAMÈTRES ---
# Récupération de la clé depuis l'environnement (injectée via Docker)
CRYPT_KEY = os.getenv("CRYPT_KEY")
BONUS_PERCENTAGE = 0.05
FISCAL_YEAR_START_MONTH = os.getenv("FIRST_MONTH")  # L'année de référence change en Septembre
ACTIVE_MODES = ["Vélo/Trottinette/Autres", "Marche/running"]

# Initialisation de Spark avec le support Delta Lake
spark = SparkSession.builder \
    .appName("EmployeeGoldStreaming") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# --- DÉCRYPTAGE (UDF) ---
def decrypt_val(cipher_text):
    if cipher_text is None or not CRYPT_KEY:
        return None
    try:
        f = Fernet(CRYPT_KEY.encode())
        return f.decrypt(cipher_text.encode()).decode()
    except Exception:
        return "Error_Decrypt"

decrypt_udf = udf(decrypt_val, StringType())

# --- SCHÉMAS ---
# Schéma des données dans 'after' (types corrigés selon ton DDL)
user_schema = StructType([
    StructField("id", StringType()),
    StructField("last_name", StringType()),
    StructField("first_name", StringType()),
    StructField("age", IntegerType()),
    StructField("business_unit", StringType()),
    StructField("salary", DecimalType(10, 2)),
    StructField("distance_kms", DecimalType(10, 1)),
    StructField("margin_kms", DecimalType(10, 1)),
    StructField("sport_type", StringType()),
    StructField("transport_mode", StringType())
])

# Schéma complet avec l'enveloppe CDC Debezium
payload_schema = StructType([
    StructField("payload", StructType([
        StructField("after", user_schema)
    ]))
])

# --- LECTURE STREAM ---
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "redpanda:9092") \
    .option("subscribe", "cdc.public.employees") \
    .option("startingOffsets", "earliest") \
    .load()

# --- LOGIQUE MÉTIER ---
df_final = df_raw.select(from_json(col("value").cast("string"), payload_schema).alias("data")) \
    .select("data.payload.after.*") \
    .filter(col("id").isNotNull()) \
    .withColumn("first_name", decrypt_udf(col("first_name"))) \
    .withColumn("last_name", decrypt_udf(col("last_name"))) \
    .withColumn("processed_at", current_timestamp()) \
    .withColumn("is_eligible_bonus", 
        when(
            (col("transport_mode").isin(ACTIVE_MODES)) & 
            (col("margin_kms") >= 0), 
            lit(True)
        ).otherwise(lit(False))
    ) \
    .withColumn("bonus_amount", 
        when(col("is_eligible_bonus") == True, 
             col("salary") * lit(BONUS_PERCENTAGE))
        .otherwise(lit(0.0))
    )
# --- STOCKAGE DELTA LAKE ---
query = df_final.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/opt/spark/data/checkpoints/employees_gold") \
    .start("/opt/spark/data/delta-lake/employees_gold")

query.awaitTermination()