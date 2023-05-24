import os
import sys

parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(parent_dir)

import asyncio
import datetime
import time

from pipeline.extract.asynchronus_extraction_with_date_range import AsynchronousDateRangeDataExtractor
from pipeline.load.mysql_data_manager import MySQLDataManager
from pipeline.transform.clean_and_transform_dataframe import *

date_range_data_extractor = AsynchronousDateRangeDataExtractor()

videogames_id_list = [26, 1]

mysql_data_manager = MySQLDataManager("lgm.cihggjssark1.eu-west-3.rds.amazonaws.com", "admin", "azertyuiop", "main")


async def main():
    await mysql_data_manager.connect_to_database()

    current_datetime = pd.to_datetime(datetime.datetime.now())
    last_record_datetime = pd.to_datetime(datetime.datetime.now() - datetime.timedelta(hours=1))
    start_time = time.time()

    leagues_df = await date_range_data_extractor.fetch_leagues_with_date_range(last_record_datetime, current_datetime, videogames_id_list)

    if not leagues_df.empty:
        leagues_id_list = list(set(leagues_df.id.to_list() + await mysql_data_manager.get_table_id_list("league")))
    else:
        leagues_id_list = await mysql_data_manager.get_table_id_list("league")

    print(leagues_df)

    series_df = await date_range_data_extractor.fetch_series_with_date_range(leagues_id_list, last_record_datetime, current_datetime)

    if not series_df.empty:
        series_id_list = list(set(series_df.id.to_list() + await mysql_data_manager.get_table_id_list("serie")))
    else:
        series_id_list = await mysql_data_manager.get_table_id_list("serie")

    print(series_df)

    tournaments_df = await date_range_data_extractor.fetch_tournaments_with_date_range(series_id_list, last_record_datetime, current_datetime)

    if not tournaments_df.empty:
        tournaments_id_list = list(set(tournaments_df.id.to_list() + await mysql_data_manager.get_table_id_list("tournament")))
    else:
        tournaments_id_list = await mysql_data_manager.get_table_id_list("tournament")

    print(tournaments_df)

    matches_raw_df, matches_streams_raw_df, matches_games_raw_df, matches_opponents_raw_df = await date_range_data_extractor.fetch_raw_all_matches_infos_with_date_range(tournaments_id_list, last_record_datetime)

    print(matches_raw_df)
    print(matches_streams_raw_df)
    print(matches_games_raw_df)
    print(matches_opponents_raw_df)

    teams_raw_df, players_raw_df = await date_range_data_extractor.fetch_raw_teams_and_players_from_tournaments_id_list(tournaments_id_list)

    print(teams_raw_df)
    print(players_raw_df)

    end_time = time.time()

    print(end_time - start_time)

    start_time = time.time()

    dataframes = {'team': clean_teams_dataframe(teams_raw_df), 'player': clean_players_dataframe(players_raw_df), 'league': clean_leagues_dataframe(leagues_df), 'serie': clean_series_dataframe(series_df),
                  'tournament': clean_tournaments_dataframe(tournaments_df), 'matchs': clean_matches_dataframe(matches_raw_df), 'match_game': clean_games_dataframe(matches_games_raw_df),
                  'match_opponent': clean_opponents_dataframe(matches_opponents_raw_df), 'match_stream': clean_streams_dataframe(matches_streams_raw_df), }

    end_time = time.time()

    print(end_time - start_time)

    start_time = time.time()

    await mysql_data_manager.insert_or_update_data_async(dataframes['team'], 'team')
    await mysql_data_manager.insert_or_update_data_async(dataframes['player'], 'player')

    await mysql_data_manager.insert_or_update_data_async(dataframes['league'], 'league')
    await mysql_data_manager.insert_or_update_data_async(dataframes['serie'], 'serie')
    await mysql_data_manager.insert_or_update_data_async(dataframes['tournament'], 'tournament')
    await mysql_data_manager.insert_or_update_data_async(dataframes['matchs'], 'matchs')

    tasks = [mysql_data_manager.insert_or_update_data_async(dataframes['match_game'], 'match_game'), mysql_data_manager.insert_or_update_data_async(dataframes['match_opponent'], 'match_opponent'), mysql_data_manager.insert_or_update_data_async(dataframes['match_stream'], 'match_stream')]
    await asyncio.gather(*tasks)

    end_time = time.time()

    print(end_time - start_time)

    await mysql_data_manager.close_connection()


if __name__ == "__main__":
    asyncio.run(main())
