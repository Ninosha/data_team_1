


import os
from google.cloud import bigquery

url = "/home/ninosha/Downloads/nino-project-349013-28e265e0c120.json"
project_name = "nino-project"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = url
client = bigquery.Client()
table_id = "nino-project-349013.123.test_data"
table = client.get_table(table_id)
schema = table.schema

job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,
    schema=schema,
)
file_url = "/home/ninosha/Downloads/products.csv"
with open(file_url, "rb") as source_file:
    job = client.load_table_from_file(source_file, table_id,
                                      job_config=job_config)

job.result()  # Waits for the job to complete.

# metdata:
# dataset1: dataset csv meta, table: tablebi
# dataset2