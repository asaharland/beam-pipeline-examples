# Apache Beam Examples

## About
This repository contains Apache Beam code examples for running on Google Cloud Dataflow. The following examples are contained in this repository:
* Batch pipeline
    * Reading from AWS S3 and writing to Google BigQuery
    * Reading from Google Cloud Storage and writing to Google BigQuery

## Batch Pipeline
The goal of the example code is to calculate the total amount transferred for each user_id in the transfers_july.csv.
This is purely fictitious example that covers the following steps: 
* Reads a CSV file from AWS S3 
* Converts the CSV file into a Java Object
* Creates key, value pairs where user_id is the key and amount is the value
* Sums the amount for each user_id
* Writes the result to BigQuery

### Running the example

#### Setup & Configuration
* Ensure that you have billing enabled for your project
* Enable the following Google Cloud Platform APIs:
    * Cloud Dataflow, Compute Engine, Stackdriver Logging, Google Cloud Storage, Google Cloud Storage JSON, BigQuery, Google Cloud Pub/Sub, Google Cloud Datastore, and Google Cloud Resource Manager APIs.
* Create a Google Cloud Storage bucket to stage your Cloud Dataflow code. Make sure you note the bucket name as you will need it later.
* Create a BigQuery dataset called finance. Keep note of the fully qualified dataset name which is in the format projectName:finance
* Upload the transfers_july.csv to your AWS S3/Google Cloud Storage bucket

#### Reading from AWS S3 and writing to BigQuery
```
mvn compile exec:java \
-Dexec.mainClass=com.harland.example.batch.BigQueryImportPipeline \
-Dexec.args="--project=<GCP PROJECT ID> \
--bucketUrl=s3://<S3 BUCKET NAME> \
--awsRegion=eu-west-1 \
--bqTableName=<BIGQUERY TABLE e.g. project:finance.transactions> \
--awsAccessKey=<YOUR ACCESS KEY> \
--awsSecretKey=<YOUR SECRET KEY> \
--runner=DataflowRunner \
--region=europe-west1 \
--stagingLocation=gs://<DATAFLOW BUCKET>/stage/ \
--tempLocation=gs://<DATAFLOW BUCKET>/temp/"
```

#### Reading from Google Cloud Storage and writing to BigQuery
```
mvn compile exec:java \
-Dexec.mainClass=com.harland.example.batch.BigQueryImportPipeline \
-Dexec.args="--project=<GCP PROJECT ID> \
--bucketUrl=gs://<GCS BUCKET NAME> \
--bqTableName=<BIGQUERY TABLE e.g. project:finance.transactions> \
--runner=DataflowRunner \
--region=europe-west1 \
--stagingLocation=gs://<DATAFLOW BUCKET>/stage/ \
--tempLocation=gs://<DATAFLOW BUCKET>/temp/"
```

## Built with
* Java 8
* Maven 3
* Apache Beam 2.5.0
