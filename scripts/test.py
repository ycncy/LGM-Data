import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'pipeline')))
sys.path.extend(['..\\..\\Data'])
from pipeline.load.mysql_data_manager import MySQLDataManager


try:
    database_host = os.environ['DATABASE_HOST']
    database_name = os.environ['DATABASE_NAME']
    database_user = os.environ['DATABASE_USER']
    database_password = os.environ['DATABASE_PASSWORD']

    print(database_host)
    print(database_name)
    print(database_user)
    print(database_password)

    mysql_manager = MySQLDataManager(database_host, database_user, database_password, database_name)
    mysql_manager.connect_to_database()

except:
    print("Error while connecting to database")