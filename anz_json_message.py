# Databricks notebook source
# reading file into dataframe
df_csv = spark.read.format("csv").option("header", "true")\
.option("inferSchema","true").load("dbfs:/FileStore/shared_uploads/anilsjcit@gmail.com/ANZ_synthesised_transaction_dataset.csv")

# COMMAND ----------

# check few records from dataframe
df_csv.show(truncate=False)

# COMMAND ----------

# converting dataframe into csv format
df_json=df_csv.toJSON()

# COMMAND ----------

# check few records from json dataframe
datacollect = df_json.collect()
for row in datacollect:
    print(row)

# COMMAND ----------

# run loop for 100 records in every one minute
# get the total records count
total_rec_cnt = df_json.count()
display(total_rec_cnt)

# COMMAND ----------

from time import time, sleep
cnt = 0
while cnt < total_rec_cnt:
    datacollect = df_json.collect()[cnt:cnt+100]
    for row in datacollect:
        print(row)
    cnt+=100
    sleep(60 - time() % 60)
    print(cnt)
    print("Its working")

# COMMAND ----------


