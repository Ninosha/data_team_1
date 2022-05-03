"""
module you can import, it can go to one's bigquery with credentials json
 and get metadata of tables... data is saved in storage via cloud
 function/postgres as marts.
"""

import os
from google.cloud import bigquery

url = "/home/ninosha/Desktop/testing/DATA/nino-project-349013-" \
      "8a8d680f044d.json"
project_name = "nino-project"
from pprint import pprint
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = url
client = bigquery.Client()
dataset = client.get_dataset("123")
dataset_meta = {"id": dataset.dataset_id,
                "project": dataset.project,
                "created": dataset.created,
                "updated": dataset.modified,
                "location": dataset.location,
                "description": dataset.description,
                "model": dataset.model}
pprint(dataset_meta)

file_url = "/home/ninosha/Downloads/products.csv"
table_id = "nino-project-349013.123.test_data"

table = client.get_table(table_id)

if ":" in table.full_table_id:
    sch = f"{dataset.project}.{dataset.dataset_id}.{table.table_id}"
    print(sch, )
fields = [{"field_name": field.name,
           "type": field.field_type,
           "mode": field.mode,
           "description": field.description}
          for field in table.schema]

res = {"creation_date": table.created, "last_update": table.modified,
       "id": table_id, "description": table.description,
       "location": table.location, "fields": fields,
       "records": table.num_rows}
from pprint import pprint

datasets = client.list_datasets()

print(datasets)
for dataset in datasets:

    tables = client.list_tables(dataset.dataset_id)

    for table in tables:
        print(table)
        table_full_id = f"{dataset.project}." \
                        f"{dataset.dataset_id}." \
                        f"{table.table_id}"

        table = client.get_table(table_full_id)

        fields = [{"field_name": field.name,
                   "type": field.field_type,
                   "mode": field.mode,
                   "description": field.description}
                  for field in table.schema]

        res = {"creation_date": table.created,
               "last_update": table.modified,
               "id": table.full_table_id,
               "description": table.description,
               "location": table.location, "fields": fields,
               "records": table.num_rows}

# bigquery_service = bigquery.Client()
# isnert = bigquery.jobs.inser()
# dataset = bigquery_service.dataset("123")

tables = client.list_tables("123")
#
# for table in tables:
#     print(table.project, table.dataset_id, table_id, table.created)
