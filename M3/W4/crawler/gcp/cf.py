import os
from google.cloud import bigquery
import pandas as pd


def metadata_to_storage(request):
    try:
        # get environment variables
        storage_name = os.environ.get("storage_name")
        dataset_dir = os.environ.get("dataset_dir")
        table_dir = os.environ.get("table_dir")

    except ValueError as e:
        return ValueError(e)

    try:
        # get client bigquery object
        client = bigquery.Client()

        # get list of dataset client has
        datasets = client.list_datasets()

        # loop through datasets, fetch metadata from each dataset
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

            # convert each dataset dict metadata to pandas dataframe
            dataset_df = pd.DataFrame([dataset_meta])

            # save dataset metadata to storage as csv file
            dataset_df.to_csv(
                f'gs://{storage_name}/{dataset_dir}/{dataset.dataset_id}.csv'
            )

            # get tables list of each dataset
            tables = client.list_tables(dataset.dataset_id)

            # loop through tables, fetch metadata of each table
            for table in tables:
                # get table full id to fetch table schema
                table_full_id = f"{dataset.project}." \
                                f"{dataset.dataset_id}." \
                                f"{table.table_id}"

                # get object of table
                table = client.get_table(table_full_id)

                # get fields from table schema
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
                              "field_name": fields["field_name"],
                              "field_type": fields["type"],
                              "field_mode": fields["mode"],
                              "field_description": fields["description"],
                              "records": table.num_rows,
                              "size": table.__sizeof__(),

                              "expiration_date": table.expires.strftime(
                                  "%m/%d/%Y")
                              if table.expires else None,

                              "partitioning": table.partitioning_type,
                              "partitioning_expiration": table.partition_expiration}

                table_df = pd.DataFrame([table_meta])

                table_df.to_csv(
                    f'gs://{storage_name}/{table_dir}/{table.table_id}.csv'
                )

    except AttributeError as e:
        return AttributeError(e)

    except Exception as e:
        return Exception(e)

schema:
{field_name: field_type, field_name: field_type}
