JSON to BIGQUERY

This project implements a serverless data pipeline that automatically processes JSON files uploaded to Google Cloud Storage (GCS) and loads them into a BigQuery table using Cloud Functions.
Solution Architecture
Components:
Google Cloud Storage (GCS) Bucket: Receives uploaded JSON files
Cloud Function: Triggered on file upload events
BigQuery: Destination for the processed data
Workflow:
User uploads a JSON file to the designated GCS bucket
Cloud Storage triggers the Cloud Function via a storage event
Cloud Function reads the JSON file
Cloud Function transforms/validates the data (if needed)
Cloud Function loads the data into BigQuery
