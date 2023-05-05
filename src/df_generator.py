import pandas as pd

from src.extractor import DataExtractor
import transformer

videogames = ["valorant", "league-of-legends"]

data_extractor = DataExtractor()

videogames_df = transformer.clean_videogames_dataframe(data_extractor.fetch_raw_videogames(videogames))

pd.to_pickle(videogames_df, "../dataframes/videogames")

leagues_df = transformer.clean_leagues_dataframe(data_extractor.fetch_raw_leagues(videogames_df.index.to_list()))

pd.to_pickle(leagues_df, "../dataframes/leagues")

series_df = transformer.clean_series_dataframe(data_extractor.fetch_raw_series(leagues_df.index.to_list()))

pd.to_pickle(series_df, "../dataframes/series")

tournaments_df = transformer.clean_tournaments_dataframe(data_extractor.fetch_raw_tournaments(series_df.index.to_list()))

pd.to_pickle(tournaments_df, "../dataframes/tournaments")

matches, streams, games, opponents = data_extractor.fetch_raw_all_matches_infos(tournaments_df.index.to_list())

pd.to_pickle(transformer.clean_opponents_dataframe(opponents), "../dataframes/opponents")
pd.to_pickle(transformer.clean_streams_dataframe(streams), "../dataframes/streams")

pd.to_pickle(transformer.clean_matches_dataframe(matches), "../dataframes/matches")
pd.to_pickle(transformer.clean_games_dataframe(games), "../dataframes/games")

teams_df, players_df = data_extractor.fetch_raw_teams_and_players_from_tournaments_id_list()

pd.to_pickle(transformer.clean_teams_dataframe(teams_df), "../dataframes/teams")
pd.to_pickle(transformer.clean_players_dataframe(players_df), "../dataframes/players")