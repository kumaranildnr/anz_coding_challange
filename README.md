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

# Assignement completion details

**Prerequisite - **
Created new free account in Google cloud to create and deploy all the solutions.

**1. Technical Architecture**

Based on my understanding on coding challenge, I have created the technical architecture design and mapped all the steps need to be considered to built the Real Time streaming job.

![Architecture](https://user-images.githubusercontent.com/103310597/162565709-756add69-9ef0-4daf-b743-f1d0aafa2880.JPG)

**2. Created JSON Schema based on input data**

Refer the attached file transaction_schema.json to see the details of schema created based on input csv file.

Sample:

![json schema](https://user-images.githubusercontent.com/103310597/162566329-77817451-c35b-4a07-84ce-af67c2bbbc8f.JPG)

Note: Followed very simple approach to design single object level JSON schema for simplicity. This design can be enhanced to have array, nested fields etc. based on actual business requirement and details.

**3. Created Topic - pubsub**

To create a topic, follow these steps:
- In the Cloud Console, go to the Pub/Sub Topics page. Go to Topics.
- Click Create topic.
- In the Topic ID field, enter an ID for your topic.
- Retain the option Add a default subscription.
- Do not select the other options.
- Click Create topic.


![pubsub](https://user-images.githubusercontent.com/103310597/162566654-8128427e-14ba-4759-9855-6c0b479e4033.JPG)

**4. Created Cloud Storage - GCP Bucket**

Cloud storage is created to store the incoming csv files which will be read by publisher program to push it into anz-customer-pos topic.

![storage](https://user-images.githubusercontent.com/103310597/162567047-294bf105-ff2e-42ef-9fa3-9fbc5e24a83a.JPG)

**5. Created BigQuery - Dataset and Tables**

Streaming job is suppose to read the data from topic, apply business transformation rule and final results need to be loaded into Bigquery 
my_dataset.authorized-transactions 

![bigquery](https://user-images.githubusercontent.com/103310597/162567234-ac2d72c3-fb8f-47aa-8f7e-1b6e99cb4a9f.JPG)
