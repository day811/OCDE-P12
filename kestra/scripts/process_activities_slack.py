import os
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from cryptography.fernet import Fernet

spark = SparkSession.builder \
    .appName("ActivitiesSlack") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

LOG_NOTIF = os.getenv('LOG_NOTIF',"NO") # NO : To Slack, YES: To logs, NOTHING: No action
print(f"Send message to log instead of Slack : {LOG_NOTIF}")
webhook_id = os.getenv('WEBHOOK_ID','')
if not webhook_id:
    print(f"Missing web hook id")
    exit(1)
KESTRA_API_URL = "http://kestra:8080/api/v1/executions/webhook/sds.infra/post_activity/" + webhook_id
#print(f"Kestra webhook url : {KESTRA_API_URL}")

# Get CRYPT KEY TO ENCRYPT COMMENTS
CRYPT_KEY = os.getenv("CRYPT_KEY")
if not CRYPT_KEY:
    print(f"Missing crypt key")
    exit(1)


# --- DÉCRYPT (UDF) ---
def decrypt_val(cipher_text) -> str:
    if cipher_text is None or not CRYPT_KEY:
        return ""
    try:
        f = Fernet(CRYPT_KEY.encode())
        return f.decrypt(cipher_text.encode()).decode()
    except Exception:
        print(f"Error while decrypting comment")
        return "Error_Decrypt"

# Kafka Reading
df_raw = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "redpanda:9092") \
    .option("subscribe", "cdc.public.sports_activities") \
    .option("failOnDataLoss", "false") \
    .load()

activity_schema = StructType([
    StructField("id", StringType()),
    StructField("comment", StringType())
])

payload_schema = StructType([
    StructField("payload", StructType([
        StructField("after", activity_schema)
    ]))
])
# Send to Kestra function
def send_to_kestra(batch_df, batch_id):
    # make a list(Dict) from it
    records = batch_df.collect()
    print(f"Batch {batch_id} reçu avec {batch_df.count()} lignes")
    if LOG_NOTIF != 'NOTHING':
        for row in records:
            decrypt_comment = decrypt_val(row.comment)
            clean_comment = decrypt_comment.replace('"', "'").strip()
            if clean_comment:
                print(f"Traitement ID {row.id}, Comment length: {len(row.comment) if row.comment else 0}")
                payload = {
                    "comment": clean_comment,
                    "log_notif" : LOG_NOTIF
                }
                try:
                    # Triggers the flowin kestra
                    requests.post(KESTRA_API_URL, json=payload, timeout=5)
                except Exception as e:
                    print(f"Erreur API Kestra: {e}")
            else:
                print(f"Treatement ID {row.id}, Do not keep due to empty Comment")
    else:
        print(f"Not sending activities to Slack/Log")    

# Parsing and launching
query = df_raw.select(from_json(col("value").cast("string"), payload_schema).alias("data")) \
    .select("data.payload.after.*") \
    .filter(col("id").isNotNull()) \
    .writeStream \
    .foreachBatch(send_to_kestra) \
    .option("checkpointLocation", "/opt/spark/checkpoints/activities_slack_gold") \
    .start()

query.awaitTermination()