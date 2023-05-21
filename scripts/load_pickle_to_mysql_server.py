import os

import pandas as pd

from pipeline.load.mysql_data_manager import MySQLDataManager

database_host = os.environ.get('DATABASE_HOST')
database_name = os.environ.get('DATABASE_NAME')
database_user = os.environ.get('DATABASE_USER')
database_password = os.environ.get('DATABASE_PASSWORD')


mysql_data_manager = MySQLDataManager(database_host, database_user, database_password, database_name)

mysql_data_manager.connect_to_database()

path_to_dataframes = "../backup/dataframes"

for dataframe in os.listdir(path_to_dataframes):

    df = pd.read_pickle(os.path.join(path_to_dataframes, dataframe))

    mysql_data_manager.add_dataframe_to_database(df, dataframe)

mysql_data_manager.close_connection()