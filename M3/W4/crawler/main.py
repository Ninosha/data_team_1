"""
module you can import, it can go to one's bigquery with credentials json
 and get metadata of tables... data is saved in storage via cloud
 function/postgres as marts.
"""

import os
from google.cloud import bigquery
from pprint import pprint
import pandas as pd

url = "/home/ninosha/Downloads/nino-project-349013-28e265e0c120.json"
project_name = "nino-project"

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = url
client = bigquery.Client()

file_url = "/home/ninosha/Downloads/products.csv"
table_id = "nino-project-349013.123.test_data"

datasets = client.list_datasets()

dataset_list = []
tables_list = []
for dataset in datasets:
    dataset = client.get_dataset(dataset.dataset_id)
    dataset_meta = {"id": dataset.dataset_id,
                    "project": dataset.project,
                    "created": dataset.created.strftime(
                        "%m/%d/%Y, %H:%M:%S"),
                    "updated": dataset.modified.strftime(
                        "%m/%d/%Y, %H:%M:%S"),
                    "location": dataset.location,
                    "description": dataset.description}
    dataset_list.append(dataset_meta)

    tables = client.list_tables(dataset.dataset_id)

    for table in tables:
        table_full_id = f"{dataset.project}." \
                        f"{dataset.dataset_id}." \
                        f"{table.table_id}"

        table = client.get_table(table_full_id)

        fields = [{
            "field_name": field.name,
            "type": field.field_type,
            "mode": field.mode,
            "description": field.description}
            for field in table.schema]

        table_meta = {"dataset": dataset.full_dataset_id,
                      "creation_date": table.created.strftime(
                          "%m/%d/%Y, %H:%M:%S"),
                      "last_update": table.modified.strftime(
                          "%m/%d/%Y, %H:%M:%S"),
                      "id": table.full_table_id,
                      "description": table.description,
                      "location": table.location,
                      "fields": fields,
                      "records": table.num_rows,
                      "size": table.__sizeof__(),
                      "expiration_date": table.expires.strftime(
                          "%m/%d/%Y") if table.expires else None,
                      "partitioning": table.partitioning_type,
                      "partitioning_expiration": table.partition_expiration}

        tables_list.append(table_meta)

        # try:
        #     parts = client.list_partitions(table)
        #     print(f"{parts=}")
        # except:
        #     continue

dataset_df = pd.DataFrame(dataset_list)
table_df = pd.DataFrame(tables_list)
pprint(dataset_df)
pprint(table_df["partitioning_expiration"])
