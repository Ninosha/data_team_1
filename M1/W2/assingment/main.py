from tools.check_connection import check_connection
from tools.fetch_data import fetch_data
from tools.add_to_database import add_to_database


if __name__ == '__main__':

    conn_check = check_connection()

    if conn_check:
        conn, cursor = conn_check

        data_json = fetch_data()

        result = add_to_database(data_json, conn, cursor)

        print(f"Insertion to the Database: {result}")

    else:
        print(f"Insertion to the Database: False   Error: {conn_check}")
