import pandas as pd

from pipeline.extract.data_extractor import DataExtractor
from pipeline.transform import clean_and_transform_dataframe as transformer

videogames = ["valorant", "league-of-legends"]

data_extractor = DataExtractor()

# videogames_df = transformer.clean_videogames_dataframe(data_extractor.fetch_raw_videogames(videogames))
#
# pd.to_pickle(videogames_df, "../dataframes/videogame")
#
# leagues_df = transformer.clean_leagues_dataframe(data_extractor.fetch_raw_leagues(videogames_df.index.to_list()))
#
# pd.to_pickle(leagues_df, "../dataframes/league")
#
# series_df = transformer.clean_series_dataframe(data_extractor.fetch_raw_series(leagues_df.index.to_list()))
#
# pd.to_pickle(series_df, "../dataframes/serie")
#
# tournaments_df = transformer.clean_tournaments_dataframe(data_extractor.fetch_raw_tournaments(series_df.index.to_list()))
#
# pd.to_pickle(tournaments_df, "../dataframes/tournament")
#
tournaments_df = pd.read_pickle("../dataframes/tournament")

matches, streams, games, opponents = data_extractor.fetch_raw_all_matches_infos(tournaments_df.index.to_list())
#
# pd.to_pickle(transformer.clean_opponents_dataframe(opponents), "../dataframes/match_opponent")
# pd.to_pickle(transformer.clean_streams_dataframe(streams), "../dataframes/match_stream")
#
pd.to_pickle(transformer.clean_matches_dataframe(matches), "../dataframes/match")
# pd.to_pickle(transformer.clean_games_dataframe(games), "../dataframes/match_game")
#
# teams_df, players_df = data_extractor.fetch_raw_teams_and_players_from_tournaments_id_list()
#
# pd.to_pickle(transformer.clean_teams_dataframe(teams_df), "../dataframes/team")
# pd.to_pickle(transformer.clean_players_dataframe(players_df), "../dataframes/player")