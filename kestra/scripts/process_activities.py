from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialisation de la session Spark
spark = SparkSession.builder \
    .appName("TestDebeziumRead") \
    .getOrCreate()

# Lecture du flux depuis Redpanda
# On se connecte au broker et on écoute le topic des activités
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "redpanda:9092") \
    .option("subscribe", "cdc.public.sports_activities") \
    .option("startingOffsets", "earliest") \
    .load()

# Pour ce test, on transforme juste la valeur binaire en texte
query = df.selectExpr("CAST(value AS STRING)") \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("checkpointLocation", "/opt/spark/checkpoints/test_read") \
    .start()

query.awaitTermination()