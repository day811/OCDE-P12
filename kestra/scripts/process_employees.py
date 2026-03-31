from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder \
    .appName("CleanPostgresStream") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# 1. Définition du schéma Debezium (on cible le payload)
# On définit juste ce qu'on veut extraire pour rester simple
user_schema = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("role", StringType()),
    StructField("department", StringType())
])

payload_schema = StructType([
    StructField("payload", StructType([
        StructField("after", user_schema)
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
clean_df = df.select(from_json(col("value").cast("string"), payload_schema).alias("data")) \
                .select("data.payload.after.*") \
                .filter(col("id").isNotNull()) # On ignore les messages de suppression (after est nul)

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