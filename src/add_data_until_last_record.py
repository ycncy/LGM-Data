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

    with concurrent.futures.ThreadPoolExecutor() as executor:
        league_last_record_datetime = mysql_data_manager.get_last_record_datetime_from_table("league")
        leagues_thread = executor.submit(date_range_data_extractor.fetch_leagues_with_date_range, league_last_record_datetime, current_datetime, videogames_id_list)

        series_last_record_datetime = mysql_data_manager.get_last_record_datetime_from_table("serie")
        series_thread = executor.submit(date_range_data_extractor.fetch_series_with_date_range, series_last_record_datetime, current_datetime, videogames_id_list)

        tournaments_last_record_datetime = mysql_data_manager.get_last_record_datetime_from_table("tournament")
        tournaments_thread = executor.submit(date_range_data_extractor.fetch_tournaments_with_date_range, tournaments_last_record_datetime, current_datetime, videogames_id_list)

        leagues_df = leagues_thread.result()
        series_df = series_thread.result()
        tournaments_df = tournaments_thread.result()

    matches_df, matches_streams_df, matches_games_df, matches_opponents_df = date_range_data_extractor.fetch_raw_all_matches_infos(tournaments_df.id.to_list())

    if not leagues_df.empty:
        mysql_data_manager.insert_into_table("league", leagues_df)
    if not series_df.empty:
        mysql_data_manager.insert_into_table("serie", clean_series_dataframe(series_df))
    if not tournaments_df.empty:
        mysql_data_manager.insert_into_table("tournament", clean_tournaments_dataframe(tournaments_df))
    if not matches_df.empty:
        mysql_data_manager.insert_into_table("match", clean_matches_dataframe(matches_df))
    if not matches_games_df.empty:
        mysql_data_manager.insert_into_table("match_game", clean_games_dataframe(matches_games_df))
    if not matches_opponents_df.empty:
        mysql_data_manager.insert_into_table("match_opponent", clean_opponents_dataframe(matches_opponents_df))
    if not matches_streams_df.empty:
        mysql_data_manager.insert_into_table("match_stream", clean_streams_dataframe(matches_streams_df))

    mysql_data_manager.close_connection()


schedule.every().hour.do(run_program)

while True:
    schedule.run_pending()
    time.sleep(1)
