import asyncio
from datetime import datetime

from machine_learning.data.collect_data import DataCollector
from machine_learning.data.data_processing import *
from model import *
from data_processing import *


async def main():
    mysql_data_manager = DataCollector("34.155.63.44", "admin", "azertyuiop", "main")

    await mysql_data_manager.connect_to_database()

    data = await mysql_data_manager.collect_all_matches_infos_to_train()

    await mysql_data_manager.close_connection()

    final_dataframe = generate_data_representations(clean_matches_infos_dataframe(data))

    final_dataframe.to_pickle("../dataframes/final_dataframe")

if __name__ == '__main__':
    asyncio.run(main())