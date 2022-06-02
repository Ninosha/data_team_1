import os
import logging
from google.cloud import bigquery
from modules.main_funcs import get_table_metadata, fetch_datasets, \
    tables_to_bigquery, parse_all_datasets
from modules.utils import read_config
from datetime import datetime


# environment variables
STORAGE_NAME = os.getenv("storage_name")
CONFIG_URL = os.getenv("config_url")
DEST_DATASET = os.getenv("dest_dataset")


def metadata_to_bgq(request):
    """
    function fetches metadata on datasets/tables from bigquery,
    inserts to bigquery metadata dataset

    :param request: request from workflow
    :return: str/error
    """

    # current time
    now = datetime.now()
    current_month = now.strftime("%m-%Y")
    current_time = now.strftime("%m/%Y/%d, %H:%M:%S")

    client = bigquery.Client()
    project_id = client.project

    # getting request json
    request_data = request.get_json(silent=True)

    # reading configuration json
    status, data = read_config(CONFIG_URL)

    # check in what form data is posted
    # if request data contains dictionary with key "data"
    # fetches datasets from request
    if request_data and request_data["data"]:
        datasets = request_data["data"]
        tables = fetch_datasets(datasets, client, project_id)

    # if status of parse_all in config is True fetching all datasets
    elif status:
        tables = parse_all_datasets(client)

    # if status of parse_all in configuration is False, fetching custom
    elif not status and data:
        tables = fetch_datasets(data, client, project_id)

    else:
        logging.info("Function needs specifying what datasets to fetch"
                     "metadata not uploaded")

        return "metadata not uploaded"

    # fetch tables metadata
    table_metadata = get_table_metadata(tables, current_time)

    # insert tables metadata to metadata dataset in bigquery
    tables_to_bigquery(
            table_metadata, project_id, DEST_DATASET, current_month
        )

    return "metadata was uploaded to bucket"
