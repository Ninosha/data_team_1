import os
from google.cloud import bigquery


# request
def hello_world():
    project_name = "nino-project"

    client = bigquery.Client()
    client.list_datasets()
    dataset = client.get_dataset("123")

    tables = client.list_tables("123")


    for table in tables:
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


# package>=version
hello_world()
