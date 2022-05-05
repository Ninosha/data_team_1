import os
import pandas as pd
from google.cloud import bigquery
from datetime import datetime


def metadata_to_storage(request):

    """
    function fetches metadata on datasets/tables from bigquery,
    saves fetched data to bucket
    :param request: request from workflow
    :return: str/error
    """

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

        dataset_list = []
        tables_list = []

        # loop through datasets, fetch metadata from each dataset
        for dataset in datasets:
            dataset = client.get_dataset(dataset.dataset_id)
            dataset_meta = {"id": dataset.dataset_id,
                            "project": dataset.project,
                            "created": dataset.created.strftime(
                                "%m/%d/%Y %H:%M:%S"),
                            "updated": dataset.modified.strftime(
                                "%m/%d/%Y %H:%M:%S"),
                            "location": dataset.location,
                            "description": dataset.description}
            dataset_list.append(dataset_meta)

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
                fields = [
                    {
                        "field_name": field.name,
                        "type": field.field_type,
                        "mode": field.mode
                    }
                    for field in table.schema]

                # create final table metadata dictionary
                table_meta = {"dataset": dataset.full_dataset_id,
                              "creation_date": table.created.strftime(
                                  "%m/%d/%Y %H:%M:%S"),
                              "last_update": table.modified.strftime(
                                  "%m/%d/%Y %H:%M:%S"),
                              "id": table.full_table_id,
                              "location": table.location,
                              "fields": fields,
                              "row_number": table.num_rows,
                              "table_size": table.__sizeof__(),
                              "expiration_date": table.expires.strftime(
                                  "%m/%d/%Y") if table.expires else None,
                              "partitioning": table.partitioning_type}

                tables_list.append(table_meta)

        # convert each list of dataset metadata to pandas dataframe
        dataset_df = pd.DataFrame(dataset_list)

        # convert each list of tables metadata to pandas dataframe
        table_df = pd.DataFrame(tables_list)

        # get the current time
        now = datetime.now()
        current_time = now.strftime("%m/%d/%Y, %H:%M:%S")

        # save dataset metadata to storage as csv file
        dataset_df.to_csv(
            f'gs://{storage_name}/{dataset_dir}/{current_time}-datasets.csv',
            index=False
        )

        # save tables metadata to storage as csv file
        table_df.to_csv(
            f'gs://{storage_name}/{table_dir}/{current_time}-tables.csv',
            index=False
        )

        return "metadata was uploaded to bucket"

    # returns error if attribute for dataset/table attribute not exists
    except AttributeError as e:
        return AttributeError(e)


"""
requirements:
google-cloud-bigquery
pandas
fsspec
gcsfs
"""