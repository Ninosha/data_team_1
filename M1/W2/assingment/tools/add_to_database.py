def add_to_database(json_data, conn, cursor):
    """
    :param json_data: fetched_data returned from fetch_data.py
    :param conn: connection for database
    :param cursor: cursor for database
    :return: executes query to database
    """

    for item in json_data:
        del item["countryInfo"]
        checked_columns = [key.replace('"', '') for key in item.keys()]
        columns = ",".join([str(s).lower() for s in checked_columns])
        checked_values = ["'" + value.replace("'", "") + "'" if type(value) == str else value for
                          value in item.values()]
        values = ",".join([str(s) for s in checked_values])

        sql = f"INSERT INTO covid ({columns}) VALUES ({values});"

        cursor.execute(sql)
        conn.commit()



    return True
