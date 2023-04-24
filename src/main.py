import pandas as pd

from src.extractor import DataExtractor
import transformer

if __name__ == '__main__':
    videogames = ["valorant", "league-of-legends"]
    keys = ["iIQ96kjaO3LAmgkle-hG6sPIuS6qEz6OvS3KogDMq5CUP6Nbl1Y", "4OvqoW8WwOUUNmCOJDiCS-rWSGowbcOss4Hn-tTqm6Me-FM4GXI", "QKufsi6ZbCvMLqXyjuhAm0NdxzFrWAfQ8ESBKTEtbEMngz0k6hU",
            "c5RlYlk_JeeAcY7nONW59Y1eRXhyTxxdDOvdkvKfGPl5ZonAB14", "87mdyvzjLRlXdIjHlMzgfXPZs2ZxB6XfUE27sNlhh4byBzWK_HM", "kie1ZNdJz1FzqaCEkBjW7c5fL9-p91Wj9cq24BHWbdg7RuM4Emc",
            "IbuMRytPybtW_2wQ0iLbZZcr4zYx0hfQsAt07MLU5WY3b_M8fsc", "kYATGmvXnqx52cwGbECJKWIocofedOQr_YnVbYWGIGDF4aYpdIk", "Jj_EamWkuCYJiBROZ6YjL69JE8g33IhOBcvLYCV8WFFGOi_CILc"]

    i = 0
    key_index = 0

    videogames_df = pd.DataFrame()
    leagues_df = pd.DataFrame()
    series_df = pd.DataFrame()
    tournaments_df = pd.DataFrame()
    matches_df = pd.DataFrame()
    games_df = pd.DataFrame()
    players_df = pd.DataFrame()
    teams_df = pd.DataFrame()
    opponents_df = pd.DataFrame()
    streams_df = pd.DataFrame()
    tournaments_participants_df = pd.DataFrame()

    data_extractor = DataExtractor()

    leagues_id = []
    series_id = []
    tournaments_id = []
    matches_id = []

    data_extractor.set_api_key(keys[key_index])

    for videogame in videogames:
        if i == 999:
            if key_index == len(keys) - 1:
                key_index = 0
            key_index = key_index + 1
            data_extractor.set_api_key(keys[key_index])

        videogame_df, temp_leagues_id = transformer.clean_videogame_dataframe(data_extractor.fetch_raw_videogame(videogame))

        leagues_id.extend(temp_leagues_id)

        videogames_df = pd.concat([videogames_df, videogame_df])
        i = i + 1

    for league in leagues_id:
        if i == 999:
            if key_index == len(keys) - 1:
                key_index = 0
            key_index = key_index + 1
            data_extractor.set_api_key(keys[key_index])

        league_df, temp_series_id = transformer.clean_league_dataframe(data_extractor.fetch_raw_league(league))

        series_id.extend(temp_series_id)

        leagues_df = pd.concat([leagues_df, league_df])
        i = i + 1

    for serie in series_id:
        if i == 999:
            if key_index == len(keys) - 1:
                key_index = 0
            key_index = key_index + 1
            data_extractor.set_api_key(keys[key_index])

        serie_df, temp_tournaments_id = transformer.clean_serie_dataframe(data_extractor.fetch_raw_serie(serie))

        tournaments_id.extend(temp_tournaments_id)

        series_df = pd.concat([series_df, serie_df])
        i = i + 1

    for tournament in tournaments_id:
        if i == 999:
            if key_index == len(keys) - 1:
                key_index = 0
            key_index = key_index + 1
            data_extractor.set_api_key(keys[key_index])

        tournament_df, temp_matches_id = transformer.clean_tournament_dataframe(data_extractor.fetch_raw_tournament(tournament))

        matches_id.extend(temp_matches_id)

        tournaments_df = pd.concat([tournaments_df, tournament_df])
        i = i + 1


    videogames_df.to_pickle("../dataframes/videogamedf")
    leagues_df.to_pickle("../dataframes/leaguesdf")
    series_df.to_pickle("../dataframes/seriesdf")
    tournaments_df.to_pickle("../dataframes/tournamentsdf")
