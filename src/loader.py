from sqlalchemy import create_engine


class DataframeLoader:

    def __init__(self, database_path, username, password, database):
        self.database_path = database_path
        self.username = username
        self.password = password
        self.database = database
        self.engine = None

    def connect_to_database(self):
        self.engine = create_engine(f"mysql+mysqlconnector://{self.username}:{self.password}@{self.database_path}/{self.database}")

        print("Connecté à la base de données :))")

    def add_dataframe_to_database(self, dataframe, table_name):
        chunksize = 1000

        for i, chunk in enumerate(dataframe.groupby(dataframe.index // chunksize)):
            chunk[1].to_sql(table_name, con=self.engine, if_exists='append')

        print(f"Table {table_name} ajoutée à la base de données")

    def close_connection(self):
        self.engine.dispose()