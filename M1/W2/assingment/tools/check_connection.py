import psycopg2


def check_connection():
    """
    :return: returns connection and cursor if database connected, if not returns ConnectionError
    """

    try:
        conn = psycopg2.connect(
            host="localhost",
            database="postgres",
            user="nino1",
            password="qwerty!")

        cursor = conn.cursor()
        return conn, cursor

    except ConnectionError as e:
        return f"{e}"
