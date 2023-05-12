import concurrent.futures
import datetime
import schedule
import time

from pipeline.extract.extract_data_with_date_range import DateRangeDataExtractor
from pipeline.transform.clean_and_transform_dataframe import *
from pipeline.load.mysql_data_manager import MySQLDataManager

date_range_data_extractor = DateRangeDataExtractor()

mysql_data_manager = MySQLDataManager("lgm.cihggjssark1.eu-west-3.rds.amazonaws.com", "admin", "azertyuiop", "main")
mysql_data_manager.connect_to_database()

videogames_id_list = [26, 1]


def run_program():
    current_datetime = datetime.datetime.now()
    current_datetime = pd.to_datetime(current_datetime).tz_localize('UTC')

    league_last_record_datetime = mysql_data_manager.get_last_record_datetime_from_table("serie")
    leagues_df = date_range_data_extractor.fetch_leagues_with_date_range(league_last_record_datetime, current_datetime, videogames_id_list)

    series_df = pd.DataFrame()
    tournaments_df = pd.DataFrame()
    matches_df = pd.DataFrame()
    matches_streams_df = pd.DataFrame()
    matches_games_df = pd.DataFrame()
    matches_opponents_df = pd.DataFrame()
    teams_df = pd.DataFrame()
    players_df = pd.DataFrame()

    if not leagues_df.empty:
        series_df = date_range_data_extractor.fetch_series_with_date_range(league_last_record_datetime, current_datetime, leagues_df.id.to_list())

        if not series_df.empty:
            tournaments_df = date_range_data_extractor.fetch_tournaments_with_date_range(league_last_record_datetime, current_datetime, series_df.id.to_list())

            if not tournaments_df.empty:
                matches_df, matches_streams_df, matches_games_df, matches_opponents_df = date_range_data_extractor.fetch_raw_all_matches_infos(tournaments_df.id.to_list())

                teams_df, players_df = date_range_data_extractor.fetch_raw_teams_and_players_from_tournaments_id_list(tournaments_df.id.to_list())

    if not leagues_df.empty:
        mysql_data_manager.insert_into_table("league", clean_leagues_dataframe(leagues_df))
    if not series_df.empty:
        mysql_data_manager.insert_into_table("serie", clean_series_dataframe(series_df))
    if not players_df.empty:
        mysql_data_manager.insert_into_table("player", clean_players_dataframe(players_df))
    if not teams_df.empty:
        mysql_data_manager.insert_into_table("team", clean_teams_dataframe(teams_df))
    if not tournaments_df.empty:
        mysql_data_manager.insert_into_table("tournament", clean_tournaments_dataframe(tournaments_df))
    if not matches_df.empty:
        mysql_data_manager.insert_into_table("match", clean_matches_dataframe(matches_df))
    if not matches_games_df.empty:
        mysql_data_manager.insert_into_table("match_game", clean_games_dataframe(matches_games_df))
    if not matches_streams_df.empty:
        mysql_data_manager.insert_into_table("match_stream", clean_streams_dataframe(matches_streams_df))
    if not matches_opponents_df.empty:
        mysql_data_manager.insert_into_table("match_opponent", clean_opponents_dataframe(matches_opponents_df))

    mysql_data_manager.close_connection()


schedule.every().hour.do(run_program)

while True:
    schedule.run_pending()
    time.sleep(1)
