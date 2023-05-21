import asyncio
import datetime
import os

from pipeline.extract.asynchronus_extraction_with_date_range import AsynchronousDateRangeDataExtractor
from pipeline.load.mysql_data_manager import MySQLDataManager
from pipeline.transform.clean_and_transform_dataframe import *

date_range_data_extractor = AsynchronousDateRangeDataExtractor()

database_host = os.environ.get('DATABASE_HOST')
database_name = os.environ.get('DATABASE_NAME')
database_user = os.environ.get('DATABASE_USER')
database_password = os.environ.get('DATABASE_PASSWORD')

videogames_id_list = [26, 1]

mysql_data_manager = MySQLDataManager("lgm.cihggjssark1.eu-west-3.rds.amazonaws.com", "admin", "azertyuiop", "main")
mysql_data_manager.connect_to_database()


async def main():
    last_record_datetime = pd.to_datetime("2023-04-27 00:00:00")
    current_datetime = pd.to_datetime(datetime.datetime.now())

    leagues_df = await date_range_data_extractor.fetch_leagues_with_date_range(last_record_datetime, current_datetime, videogames_id_list)

    leagues_id_list = list(set(leagues_df.id.to_list() + mysql_data_manager.get_table_id_list("league")))

    series_df = await date_range_data_extractor.fetch_series_with_date_range(leagues_id_list, last_record_datetime, current_datetime)

    series_id_list = list(set(series_df.id.to_list() + mysql_data_manager.get_table_id_list("serie")))

    tournaments_df = await date_range_data_extractor.fetch_tournaments_with_date_range(series_id_list, last_record_datetime, current_datetime)

    tournaments_id_list = tournaments_df.id.to_list() + mysql_data_manager.get_table_id_list("tournament")

    matches_raw_df, matches_streams_raw_df, matches_games_raw_df, matches_opponents_raw_df = await date_range_data_extractor.fetch_raw_all_matches_infos_with_date_range(tournaments_id_list, last_record_datetime)

    teams_raw_df, players_raw_df = await date_range_data_extractor.fetch_raw_teams_and_players_from_tournaments_id_list(tournaments_id_list)

    dataframes = {'league': leagues_df, 'serie': series_df, 'tournament': tournaments_df, 'matchs': matches_raw_df, 'match_game': matches_games_raw_df, 'match_opponent': matches_opponents_raw_df, 'match_stream': matches_streams_raw_df,
                  'team': teams_raw_df, 'player': players_raw_df}

    for file_name, dataframe in dataframes.items():
        mysql_data_manager.insert_or_update_data(dataframe, file_name)

asyncio.run(main())
