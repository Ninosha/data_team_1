import os
from google.cloud import bigquery


def hello_world(request):
    project_name = "nino-project"

    client = bigquery.Client()

    tables = client.list_tables("123")

    for table in tables:
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
fsspec
google-cloud-bigquery
gcfs