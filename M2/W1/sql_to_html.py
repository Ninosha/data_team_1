import pandas as pd
from sqlalchemy import create_engine


def sql_to_html(table_name):
    engine = create_engine('postgresql://nino1:qwerty!@localhost:5432/postgres')
    df = pd.read_sql_table(table_name, engine)

    df_in_progress = df.loc[df["status"] == "IN PROGRESS"]
    df_finished = df.loc[df["status"] == "FINISHED"]

    return df.to_html(classes=["dataframe"]), len(df_in_progress), len(df_finished)

#