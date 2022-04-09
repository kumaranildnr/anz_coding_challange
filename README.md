# anz_coding_challange
Real time streaming job to read transaction data and write into Bigquery

# Requirement Details

The challenge presented to you is to build a real-time streaming job to read customers' transactions and apply business rules to filter the transactions before writing them to a big-query table. The tasks are:

PFA synthesized a transaction dataset containing three months' worth of transactions for 100 hypothetical customers. It includes purchases, recurring transactions, and salary transactions.
PFA, postal reference code for each suburb in a state.
Create a schema by understanding the data in JSON format.
Write a producer program to generate 100 transactions per min onto a GCP pub/sub Topic called "anz-customer-pos" in a JSON format.

**** Streaming Job Requirements *******

Read the messages from the topic "anz-customer-pos" and validate the message as per the schema created in step 3.
If the input message fails schema validation, write those invalid messages onto a GCS bucket at gs://anz-coding-challenge-1001/error-records.
If the message is as per schema, apply the below business filter to select the record for the BQ sink.

Status = “authorized” and card_present_flag=0

Split the long_lat and merchant_long_lat fields for qualified records into long, lat and merch_long, merch_lat fields.
Join postal reference data to pull the postal code to the output record.
Write the output records to " authorized-transactions " BQ table under "my-dataset."
It would be nice to create a template out of your data flow job, but this is optional.
We would be happier if you could create a job name called "anz-trans-pub-sub-to-bq" to run only b/w 8 am to 9 am daily using cloud composer. This task is entirely optional.

