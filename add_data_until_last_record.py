import concurrent.futures
import datetime
import os

import pandas as pd

from pipeline.extract.extract_data_with_date_range import DateRangeDataExtractor
from pipeline.load.mysql_data_manager import MySQLDataManager
from pipeline.transform.clean_and_transform_dataframe import *

date_range_data_extractor = DateRangeDataExtractor()

database_host = os.environ.get('DATABASE_HOST')
database_name = os.environ.get('DATABASE_NAME')
database_user = os.environ.get('DATABASE_USER')
database_password = os.environ.get('DATABASE_PASSWORD')

mysql_data_manager = MySQLDataManager("lgm.cihggjssark1.eu-west-3.rds.amazonaws.com", "admin", "azertyuiop", "main")
mysql_data_manager.connect_to_database()

videogames_id_list = [26, 1]

current_datetime = datetime.datetime.now()
current_datetime = pd.to_datetime(current_datetime).tz_localize('UTC')


#league_last_record_datetime = mysql_data_manager.get_last_record_datetime_from_table("serie")
league_last_record_datetime = "2023-04-27 00:00:00"

leagues_df = date_range_data_extractor.fetch_leagues_with_date_range(league_last_record_datetime, current_datetime, videogames_id_list)

series_df = pd.DataFrame()
tournaments_df = pd.DataFrame()
matches_df = pd.DataFrame()
matches_streams_df = pd.DataFrame()
matches_games_df = pd.DataFrame()
matches_opponents_df = pd.DataFrame()
teams_df = pd.DataFrame()
players_df = pd.DataFrame()

current_series_id_list = mysql_data_manager.get_table_id_list("serie")
new_leagues_id_list = mysql_data_manager.get_table_id_list("league") + leagues_df.id.to_list()
series_df = date_range_data_extractor.fetch_series_with_date_range(new_leagues_id_list, "2023-04-27 00:00:00")

print(series_df)

current_tournaments_id_list = mysql_data_manager.get_last_record_datetime_from_table("tournament")
new_series_id_list = mysql_data_manager.get_table_id_list("serie") + series_df.id.to_list()
tournaments_df = date_range_data_extractor.fetch_tournaments_with_date_range(new_series_id_list, current_tournaments_id_list)

print(tournaments_df)

match_last_record_datetime = mysql_data_manager.get_last_record_datetime_from_table("match")
new_tournaments_id_list = mysql_data_manager.get_table_id_list("tournament") + tournaments_df.id.to_list()
matches_df, matches_streams_df, matches_games_df, matches_opponents_df = date_range_data_extractor.fetch_raw_all_matches_infos(new_tournaments_id_list)

matches_df = pd.DataFrame()
matches_streams_df = pd.DataFrame()
matches_games_df = pd.DataFrame()
matches_opponents_df = pd.DataFrame()

num_threads = 4
with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
    futures = []

    for tournament_id in new_tournaments_id_list:
        future = executor.submit(date_range_data_extractor.fetch_raw_all_matches_infos_with_date_range, tournament_id, "2023-04-27 00:00:00")
        futures.append(future)

    concurrent.futures.wait(futures)

    for future in futures:
        match_df, stream_df, game_df, opponent_df = future.result()

        matches_df = pd.concat([matches_df, match_df])
        matches_streams_df = pd.concat([matches_streams_df, stream_df])
        matches_games_df = pd.concat([matches_games_df, game_df])
        matches_opponents_df = pd.concat([matches_opponents_df, opponent_df])


print(matches_df)
print(matches_streams_df)
print(matches_games_df)
print(matches_opponents_df)


# teams_df, players_df = date_range_data_extractor.fetch_raw_teams_and_players_from_tournaments_id_list(new_tournaments_id_list)
#
# if not leagues_df.empty:
#     series_df = date_range_data_extractor.fetch_series_with_date_range(league_last_record_datetime, current_datetime, leagues_df.id.to_list())
#
#     if not series_df.empty:
#         tournaments_df = date_range_data_extractor.fetch_tournaments_with_date_range(league_last_record_datetime, current_datetime, series_df.id.to_list())
#
#         if not tournaments_df.empty:
#             matches_df, matches_streams_df, matches_games_df, matches_opponents_df = date_range_data_extractor.fetch_raw_all_matches_infos(tournaments_df.id.to_list())
#
#             teams_df, players_df = date_range_data_extractor.fetch_raw_teams_and_players_from_tournaments_id_list(tournaments_df.id.to_list())
#
# if not leagues_df.empty:
#     mysql_data_manager.insert_into_table("league", clean_leagues_dataframe(leagues_df))
# if not series_df.empty:
#     mysql_data_manager.insert_into_table("serie", clean_series_dataframe(series_df))
# if not players_df.empty:
#     mysql_data_manager.insert_into_table("player", clean_players_dataframe(players_df))
# if not teams_df.empty:
#     mysql_data_manager.insert_into_table("team", clean_teams_dataframe(teams_df))
# if not tournaments_df.empty:
#     mysql_data_manager.insert_into_table("tournament", clean_tournaments_dataframe(tournaments_df))
# if not matches_df.empty:
#     mysql_data_manager.insert_into_table("match", clean_matches_dataframe(matches_df))
# if not matches_games_df.empty:
#     mysql_data_manager.insert_into_table("match_game", clean_games_dataframe(matches_games_df))
# if not matches_streams_df.empty:
#     mysql_data_manager.insert_into_table("match_stream", clean_streams_dataframe(matches_streams_df))
# if not matches_opponents_df.empty:
#     mysql_data_manager.insert_into_table("match_opponent", clean_opponents_dataframe(matches_opponents_df))

mysql_data_manager.close_connection()
