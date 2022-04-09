import os
from google.cloud import pubsub_v1
from google.cloud import storage
from pyspark.sql import SparkSession
from time import time, sleep

spark = SparkSession.builder.appName('anz_coding_challenge').getOrCreate()

# private key for authentication
credential_path = "svc_pubsub_pvt_key.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credential_path

# csv file is stored in GCP storage bucket
# read csv file in dataframe
df_csv = spark.read.format("csv").option("header", "true") \
    .option("inferSchema", "true") \
    .load("gs://anz-coding-challenge-1001/input-files/ANZ-synthesised-transaction-dataset.csv")

# convert dataframe into json
df_json = df_csv.toJSON()

# get the total records count
total_rec_cnt = df_json.count()

# pubsub topic details
project_id = "anz-coding-challenge"
topic_id = "anz-customer-pos"

publisher = pubsub_v1.PublisherClient()
# The `topic_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/topics/{topic_id}`
topic_path = publisher.topic_path(project_id, topic_id)

# run df_json in loop and send 100 records in every 1 min
cnt = 0
while cnt < total_rec_cnt:
    datacollect = df_json.collect()[cnt:cnt + 100]
    for row in datacollect:
        data = row.encode("utf-8")
        future = publisher.publish(topic_path, data)
    print(f"Published messages to {topic_path}.")
    cnt += 100
    sleep(60 - time() % 60)
