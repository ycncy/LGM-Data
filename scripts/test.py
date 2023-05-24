import asyncio
import os
import sys

parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(parent_dir)

from pipeline.load.mysql_data_manager import MySQLDataManager

async def main():
    try:
        mysql_manager = MySQLDataManager("lgm.cihggjssark1.eu-west-3.rds.amazonaws.com", "admin", "azertyuiop", "main")
        await mysql_manager.connect_to_database()

    except:
        print("Error while connecting to database")

asyncio.run(main())