# Databricks notebook source
# reading file ANZ_synthesised_transaction_dataset into dataframe 
df_txn = spark.read.format("csv").option("header", "true")\
.option("inferSchema","true").load("dbfs:/FileStore/shared_uploads/anilsjcit@gmail.com/ANZ_synthesised_transaction_dataset.csv")

# COMMAND ----------

# reading file postal_code into dataframe 
df_pc = spark.read.format("csv").option("header", "true")\
.option("inferSchema","true").load("dbfs:/FileStore/shared_uploads/anilsjcit@gmail.com/postal_codes.csv")

# COMMAND ----------

# 
df_txn_auth = df_txn.filter((df_txn['status'] == "authorized") & (df_txn['card_present_flag'] == 0))

# COMMAND ----------

#show the results of Apply rule: Status = “authorized” and card_present_flag=0
df_txn_auth.show(truncate=False)

# COMMAND ----------

#Showing results based on group by to verify filter is working as expected
df_txn_auth.groupBy("status").count().show()

# COMMAND ----------

#Showing results based on group by to verify filter is working as expected
df_txn_auth.groupBy("status","card_present_flag").count().show()

# COMMAND ----------

#Split the long_lat and merchant_long_lat fields for qualified records into long, lat and merch_long, merch_lat fields.
#first lets select few columns from dataframe
df_longlat = df_txn.select("status","account","long_lat","merchant_id","merchant_long_lat")

# COMMAND ----------

#selecting first 15 records
df_longlat.show(15,truncate=False)

# COMMAND ----------

#splitting longitude and latitude details
from pyspark.sql.functions import split
df_split = df_longlat.withColumn('long', split(df_longlat['long_lat'], ' ').getItem(0)) \
                     .withColumn('lat', split(df_longlat['long_lat'], ' ').getItem(1)) \
                     .withColumn('merch_long', split(df_longlat['merchant_long_lat'], ' ').getItem(0)) \
                     .withColumn('merch_lat', split(df_longlat['merchant_long_lat'], ' ').getItem(1))
df_split.show()

# COMMAND ----------

#Join postal reference data to pull the postal code to the output record
#Check few records from postal code csv file
df_pc.show(15,truncate=False)

# COMMAND ----------

#Do the inner join with field merchant_suburb from df_txn and field suburb from df_pc
df_join = df_txn.join(df_pc,df_txn.merchant_suburb ==  df_pc.suburb,"inner")
df_join.show(truncate=False)

# COMMAND ----------

#select few records to verify join is working fine or not
df_join.select("status","account","long_lat","merchant_id","merchant_long_lat","merchant_suburb","state","postal_code").show(15,truncate=False)


# COMMAND ----------


