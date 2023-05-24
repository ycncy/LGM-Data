import asyncio
import os
import sys

parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(parent_dir)

from pipeline.load.mysql_data_manager import MySQLDataManager

async def main():
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
        await mysql_manager.connect_to_database()

    except:
        print("Error while connecting to database")

asyncio.run(main())