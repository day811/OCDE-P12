from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType

spark = SparkSession.builder \
    .appName("CleanPostgresStream") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# 1. Définition du schéma Debezium (on cible le payload)
# On définit juste ce qu'on veut extraire pour rester simple
user_schema = StructType([
    StructField("id", StringType()),
    StructField("sport_type", StringType()),
    StructField("age", IntegerType()),
    StructField("distance_kms", DecimalType(10,1)),
    StructField("margin_kms", DecimalType(10,1)),
    StructField("salary", DecimalType(10,2)),
    StructField("business_unit", StringType())
])

payload_schema = StructType([
    StructField("payload", StructType([
        StructField("after", user_schema),
        StructField("op", StringType())
    ]))
])

# 2. Lecture du flux
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "redpanda:9092") \
    .option("subscribe", "cdc.public.employees") \
    .option("startingOffsets", "earliest") \
    .load()

# 3. Extraction des données "after" (la nouvelle ligne)
# On parse le JSON, on va dans payload -> after, puis on aplatit les colonnes
df_parsed = df.select(from_json(col("value").cast("string"), payload_schema).alias("data"))

# On descend dans la structure : data -> payload -> after
clean_df = df_parsed.select("data.payload.after.*")

# FILTRE CRUCIAL : On ne garde que si 'after' n'est pas nul
# (C'est ce qui évite les lignes d'entêtes seules pour les messages de suppression ou d'init)
clean_df = clean_df.filter(col("id").isNotNull())

# 4. Écriture sur le disque (Volume bindé)
# On utilise le format CSV pour que tu puisses l'ouvrir facilement sur ton PC
query = clean_df.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "/opt/spark/data/employees_clean") \
    .option("checkpointLocation", "/opt/spark/checkpoints/employees_v1") \
    .option("header", "true") \
    .start()

query.awaitTermination()