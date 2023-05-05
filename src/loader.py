import pandas as pd
from sqlalchemy import create_engine, types


class DataframeLoader:

    def __init__(self, database_path, username, password, database):
        self.database_path = database_path
        self.username = username
        self.password = password
        self.database = database
        self.engine = None

    def connect_to_database(self):
        self.engine = create_engine(f"mysql+mysqlconnector://{self.username}:{self.password}@{self.database_path}/{self.database}")

        print("Connecté à la base de données.")

    def add_dataframe_to_database(self, dataframe, table_name):
        col_types = {}
        for col in dataframe.columns:
            if pd.api.types.is_string_dtype(dataframe[col]):
                col_types[col] = types.String(length=255)
            elif pd.api.types.is_float_dtype(dataframe[col]):
                col_types[col] = types.Float(precision=2)
            elif pd.api.types.is_integer_dtype(dataframe[col]):
                col_types[col] = types.Integer()
            elif pd.api.types.is_datetime64_dtype(dataframe[col]):
                col_types[col] = types.Time()
            elif pd.api.types.is_bool_dtype(dataframe[col]):
                col_types[col] = types.Boolean()
            else:
                col_types[col] = types.String(length=255)

        chunksize = 1000

        for i, chunk in enumerate(dataframe.groupby(dataframe.index // chunksize)):
            chunk[1].to_sql(table_name, con=self.engine, if_exists='append', dtype=col_types)

        print(f"Table {table_name} ajoutée à la base de données")

    def close_connection(self):
        self.engine.dispose()