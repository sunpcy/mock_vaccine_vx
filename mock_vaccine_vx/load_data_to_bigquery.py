import json
import os

from google.cloud import bigquery
from google.oauth2 import service_account


DATA_FOLDER = "data"

keyfile = "/workspaces/mock_vaccine_vx/mock_vaccine_vx/cred/load-data-to-bigquery.json"
service_account_info = json.load(open(keyfile))
credentials = service_account.Credentials.from_service_account_info(service_account_info)
project_id = "sidata-test" # แก้ไข project_id ให้สอดคล้องกับ GCP project ของตัวเอง
dataset = "mock_vaccine_vx"
client = bigquery.Client(
    project=project_id,
    credentials=credentials,
)

# กำหนด schema สำหรับแต่ละตาราง
schemas = {
    "vaccination": [
        bigquery.SchemaField("vaccination_id", "STRING"),
        bigquery.SchemaField("vaccine_date", "DATE"),
        bigquery.SchemaField("vaccine_id", "STRING"),
        bigquery.SchemaField("person_id", "STRING"),
        bigquery.SchemaField("location_id", "STRING"),
    ],
    "person": [
        bigquery.SchemaField("person_id", "STRING"),
        bigquery.SchemaField("first_name", "STRING"),
        bigquery.SchemaField("last_name", "STRING"),
        bigquery.SchemaField("sex", "STRING"),
        bigquery.SchemaField("date_of_birth", "STRING"),
        bigquery.SchemaField("province", "STRING"),
    ],
    "employee": [
        bigquery.SchemaField("person_id", "STRING"),
        bigquery.SchemaField("employee_id", "STRING"),
        bigquery.SchemaField("employee_department", "STRING"),
    ],
    "vaccine": [
        bigquery.SchemaField("vaccine_id", "STRING"),
        bigquery.SchemaField("vaccine_name", "STRING"),
        bigquery.SchemaField("price", "FLOAT"),
    ],
    "location": [
        bigquery.SchemaField("location_id", "STRING"),
        bigquery.SchemaField("location_name", "STRING"),
        bigquery.SchemaField("province", "STRING"),
        bigquery.SchemaField("longitude", "FLOAT"),
        bigquery.SchemaField("latitude", "FLOAT"),
    ],
}

data_list = ["employee", "location", "person", "vaccination", "vaccine"]
for data in data_list:
    # ดึง schema จาก dictionary 
    schema = schemas[data]

    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.CSV,
        autodetect=False,  
        schema=schema,  # ใช้ schema ที่ดึงจาก dictionary
    )

    file_path = f"{DATA_FOLDER}/{data}.csv"
    with open(file_path, "rb") as f:
        table_id = f"{project_id}.{dataset}.{data}"
        job = client.load_table_from_file(f, table_id, job_config=job_config)
        job.result()

    table = client.get_table(table_id)
    print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")