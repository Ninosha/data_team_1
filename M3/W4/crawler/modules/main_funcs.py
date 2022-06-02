import pandas as pd
import logging


def get_table_metadata(tables, current_time):
    """
    function gets received tables metadata saves into tables dictionary

    :param tables: lst/table objects
    :param current_time:
    :return: list of dicts/tables metadata
    """

    tables_list = []

    for table in tables:
        # get fields from table schema
        fields = [[
            {
                "field_name": field.name,
                "type": field.field_type,
                "mode": field.mode
            }
            for field in table.schema]]

        # create final table metadata dictionary
        table_meta = {"metadata_date": current_time,
                      "dataset": table.dataset_id,
                      "creation_date": table.created.strftime(
                          "%m/%d/%Y %H:%M:%S"),
                      "last_update": table.modified.strftime(
                          "%m/%d/%Y %H:%M:%S"),
                      "id": table.full_table_id,
                      "location": table.location,
                      "row_number": table.num_rows,
                      "table_size": table.__sizeof__(),

                      "expiration_date": table.expires.strftime(
                          "%m/%d/%Y") if table.expires else "null",

                      "partitioning": table.partitioning_type
                      if table.partitioning_type
                      else "null",

                      "fields": fields
                      }

        tables_list.append(table_meta)

        logging.info(f"{table} metadata fetched")

    return tables_list


def fetch_datasets(data, client, project_id):
    """
    fetches custom datasets/tables received from request/config.json if
    any table/dataset name is wrong function skips and logs name of
    wrong table/dataset name

    :param data: dict/data from request/config
    :param client: client obj
    :param project_id: str/env variable
    :return: lst/table objects
    """

    table_objs = []

    # loops through data fetching tables except ones starting "metadata"
    for dataset in data:
        if "metadata" in dataset:
            pass
        else:
            dataset_id = f"{project_id}.{dataset}"

            for table in data[dataset]:
                try:
                    table_full_id = f"{dataset_id}." \
                                    f"{table}"
                    table = client.get_table(table_full_id)
                    table_objs.append(table)

                except ValueError as e:
                    logging.info(
                        f"{dataset} or {table} doesn't exist"
                    )
                    continue

    logging.info(f"custom tables objects fetched")

    return table_objs


def parse_all_datasets(client):
    """
    function use to parse all datasets/tables if parse_all variables are
                                                    True in config.json

    :param client: client obj
    :return: list/table objects
    """

    list_objs = []
    datasets = client.list_datasets()

    for dataset in datasets:
        dataset_name = dataset.dataset_id
        if "metadata" in dataset_name:
            pass
        else:
            tables = list(client.list_tables(dataset))

            for table in tables:
                table = client.get_table(
                    str(table.full_table_id).replace(":", ".")
                )

                list_objs.append(table)

    logging.info("all table objects of all datasets fetched")

    return list_objs


def tables_to_bigquery(tables_metadata, project_id,
                       destination_dataset, current_month):
    """
    function to insert tables metadata to bigquery dataset

    :param tables_metadata: list of dict/table metadata dictionaries
    :param project_id: str/project id
    :param destination_dataset: str/destination table name
    :param current_month: str/current month
    :return: str/message
    """

    for table_mt in tables_metadata:
        table_df = pd.DataFrame(table_mt)
        table_name = str(table_mt["id"]).split(":")[1].split(".")[1]

        dest_table_name = f"" \
                          f"{destination_dataset}." \
                          f"{table_name}-" \
                          f"{current_month} "

        table_df["fields"] = table_df["fields"].astype("string")

        # inserts table metadata dataframe to bigquery
        table_df.to_gbq(
            destination_table=dest_table_name,
            project_id=project_id,
            if_exists="append",
        )

        logging.info(f"table metadata successfully inserted into "
                     f"bigquery destination table {dest_table_name}")

    return "metadata successfully inserted"
