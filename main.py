import functions_framework
import logging
import traceback
import re

from google.cloud import bigquery
from google.cloud import storage

import yaml

# Set up logging
logging.basicConfig(level=logging.INFO)

# Load schema configuration
with open("./schemas.yaml") as schema_file:
    config = yaml.safe_load(schema_file)

PROJECT_ID = 'gcppractice-453702'
BQ_DATASET = 'staging'
CS = storage.Client()
BQ = bigquery.Client()
job_config = bigquery.LoadJobConfig()

@functions_framework.cloud_event
def hello_gcs(cloud_event):
    try:
        # Extract relevant data from the CloudEvent
        data = cloud_event.data
        bucket_name = data['bucket']  # Bucket name
        file_name = data['name']     # File name
        time_created = data['timeCreated']  # Time created

        logging.info("Bucket name: %s", bucket_name)
        logging.info("File name: %s", file_name)
        logging.info("Time created: %s", time_created)

        # Process the file based on the configuration
        for table in config:
            table_name = table.get('name')
            if re.search(table_name.replace('_', '-'), file_name) or re.search(table_name, file_name):
                table_schema = table.get('schema')
                _check_if_table_exists(table_name, table_schema)
                table_format = table.get('format')
                if table_format == 'NEWLINE_DELIMITED_JSON':
                    _load_table_from_uri(bucket_name, file_name, table_schema, table_name)
    except Exception as e:
        logging.error("Error processing file. Cause: %s", traceback.format_exc())

def _check_if_table_exists(table_name, table_schema):
    table_id = f"{PROJECT_ID}.{BQ_DATASET}.{table_name}"

    try:
        BQ.get_table(table_id)
    except Exception:
        logging.warning('Creating table: %s', table_name)
        schema = create_schema_from_yaml(table_schema)
        table = bigquery.Table(table_id, schema=schema)
        table = BQ.create_table(table)
        logging.info("Created table %s", table_id)

def _load_table_from_uri(bucket_name, file_name, table_schema, table_name):
    uri = f'gs://{bucket_name}/{file_name}'
    table_id = f"{PROJECT_ID}.{BQ_DATASET}.{table_name}"

    schema = create_schema_from_yaml(table_schema)
    job_config.schema = schema
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.write_disposition = 'WRITE_APPEND'

    load_job = BQ.load_table_from_uri(
        uri,
        table_id,
        job_config=job_config,
    )

    load_job.result()
    logging.info("Job finished for table %s", table_id)

def create_schema_from_yaml(table_schema):
    schema = []
    for column in table_schema:
        if column['type'] == 'RECORD':
            nested_fields = create_schema_from_yaml(column['fields'])
            schema_field = bigquery.SchemaField(column['name'], column['type'], column['mode'], fields=nested_fields)
        else:
            schema_field = bigquery.SchemaField(column['name'], column['type'], column['mode'])
        schema.append(schema_field)
    return schema