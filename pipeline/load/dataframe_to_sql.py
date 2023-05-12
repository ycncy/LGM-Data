import mysql.connector
import pandas as pd


class DataframeLoader:

    def __init__(self, database_host, database_user, database_password, database_name):
        self.database_host = database_host
        self.database_user = database_user
        self.database_password = database_password
        self.database_name = database_name
        self.connection = None

    def connect_to_database(self):
        self.connection = mysql.connector.connect(host=self.database_host, user=self.database_user, password=self.database_password, database=self.database_name)

        print("Connecté à la base de données.")

    def close_connection(self):
        self.connection.close()

        print("Déconnecté de la base de données")

    def get_all_tables(self):
        cursor = self.connection.cursor()

        cursor.execute("SHOW TABLES")

        tables = []

        for row in cursor:
            tables.append(row[0])

        cursor.close()

        return tables

    def add_dataframe_to_database(self, dataframe, table_name):
        col_types = {}
        for col in dataframe.columns:
            if pd.api.types.is_string_dtype(dataframe[col]):
                col_types[col] = 'VARCHAR(255)'
            elif pd.api.types.is_float_dtype(dataframe[col]):
                col_types[col] = 'FLOAT(2)'
            elif pd.api.types.is_integer_dtype(dataframe[col]):
                col_types[col] = 'INT'
            elif pd.api.types.is_datetime64_dtype(dataframe[col]) or col == "begin_at" or col == "end_at" or col == "scheduled_at" or col == "original_scheduled_at":
                col_types[col] = 'DATETIME'
            elif pd.api.types.is_bool_dtype(dataframe[col]):
                col_types[col] = 'BOOLEAN'
            else:
                col_types[col] = 'VARCHAR(255)'

        chunksize = 1000

        for i, chunk in enumerate(dataframe.groupby(dataframe.index // chunksize)):
            query = f"INSERT INTO {table_name} ({', '.join(chunk[1].columns)}) VALUES ({', '.join(['%s'] * len(chunk[1].columns))})"
            data = [tuple(x) for x in chunk[1].values]
            with self.connection.cursor() as cursor:
                cursor.executemany(query, data)

        self.connection.commit()
        print(f"Table {table_name} ajoutée à la base de données")

    def get_last_record_datetime_from_table(self, table_name):
        select_all_columns = f"SELECT * FROM `{table_name}` LIMIT 1;"

        cursor = self.connection.cursor()

        cursor.execute(select_all_columns)

        cursor.fetchall()

        column_names = [desc[0] for desc in cursor.description]

        if "begin_at" not in column_names and "modified_at" not in column_names:
            return ""

        begin_at_select_request = f"SELECT begin_at FROM `{table_name}` ORDER BY begin_at DESC LIMIT 1"
        modified_at_select_request = f"SELECT modified_at FROM `{table_name}` ORDER BY modified_at DESC LIMIT 1"

        select_request = begin_at_select_request if "begin_at" in column_names else modified_at_select_request

        cursor.execute(select_request)

        last_record_datetime = ""

        for row in cursor:
            last_record_datetime = row[0]

        cursor.close()

        return last_record_datetime