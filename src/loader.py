import sqlite3 as sql
import pandas as pd


class DataframeLoader:

    def __init__(self, database_path):
        self.database_path = database_path
        self.connection = None

    def connect_to_database(self):
        self.connection = sql.connect(self.database_path)

    def add_dataframe_to_database(self, dataframe, table_name):
        dataframe.to_sql(table_name, self.connection, if_exists="replace", index=False)

    def close_connection(self):
        self.connection.close()