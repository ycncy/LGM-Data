import os

import pandas as pd

from pipeline.load.mysql_data_manager import MySQLDataManager

dataframe_loader = MySQLDataManager("lgm.cihggjssark1.eu-west-3.rds.amazonaws.com", "admin", "azertyuiop", "test")

dataframe_loader.connect_to_database()

path_to_dataframes = "../dataframes"

for dataframe in os.listdir(path_to_dataframes):

    df = pd.read_pickle(os.path.join(path_to_dataframes, dataframe))

    dataframe_loader.add_dataframe_to_database(df, dataframe)

dataframe_loader.close_connection()