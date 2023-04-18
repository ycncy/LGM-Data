import requests
import pandas as pd


class DataExtractor:

    def __init__(self):
        self.api_key = None
        self.api_url = "https://api.pandascore.co"
        self.header = None

    def set_api_key(self, new_api_key):
        self.api_key = new_api_key
        self.header = {"accept": "application/json", "authorization": f"Bearer {new_api_key}"}

    def fetch_raw_videogame(self, videogame):
        url = f"{self.api_url}/videogames/{videogame}"

        game_info = requests.get(url, headers=self.header).json()

        first_dataframe = pd.json_normalize(game_info)

        print(pd.DataFrame(first_dataframe.leagues[0]))

        if hasattr(pd.DataFrame(first_dataframe.leagues[0]), 'id'):
            id_list = pd.DataFrame(first_dataframe.leagues[0]).id.to_list()
        else:
            id_list = []

        return first_dataframe, id_list

    def fetch_raw_league(self, league):
        url = f"{self.api_url}/leagues/{league}"

        league_info = requests.get(url, headers=self.header).json()

        first_dataframe = pd.json_normalize(league_info)

        if league_info["videogame"]["id"] is not None:
            first_dataframe["videogame_id"] = league_info["videogame"]["id"]

        if hasattr(pd.DataFrame(first_dataframe.series[0]), 'id'):
            id_list = pd.DataFrame(first_dataframe.series[0]).id.to_list()
        else:
            id_list = []

        return first_dataframe, id_list

    def fetch_raw_serie(self, serie):
        url = f"{self.api_url}/series/{serie}"

        serie_info = requests.get(url, headers=self.header).json()

        first_dataframe = pd.json_normalize(serie_info)

        if hasattr(pd.DataFrame(first_dataframe.tournaments[0]), 'id'):
            id_list = pd.DataFrame(first_dataframe.tournaments[0]).id.to_list()
        else:
            id_list = []

        return first_dataframe, id_list

    def fetch_raw_tournament(self, tournament):
        url = f"{self.api_url}/tournaments/{tournament}"

        tournament_info = requests.get(url, headers=self.header).json()

        print(tournament_info["matches"])

        first_dataframe = pd.json_normalize(tournament_info)

        if hasattr(pd.DataFrame(first_dataframe.matches[0]), 'id'):
            id_list = pd.DataFrame(first_dataframe.matches[0]).id.to_list()
        else:
            id_list = []

        return first_dataframe, id_list

    def fetch_raw_match(self, match):
        url = f"{self.api_url}/matches/{match}"

        match_info = requests.get(url, headers=self.header).json()

        first_dataframe = pd.json_normalize(match_info)

        if hasattr(pd.DataFrame(first_dataframe.game[0]), 'id'):
            id_list = pd.DataFrame(first_dataframe.game[0]).id.to_list()
        else:
            id_list = []

        return first_dataframe, id_list

    def fetch_raw_games(self, match):
        url = f"{self.api_url}/matches/{match}"

        games_info = requests.get(url, headers=self.header).json()["games"]

        return pd.json_normalize(games_info)

    def fetch_raw_team(self, team):
        url = f"{self.api_url}/teams/{team}"

        raw_team = requests.get(url, headers=self.header).json()

        return pd.json_normalize(raw_team)

    def fetch_raw_players_from_team(self, team):
        url = f"{self.api_url}/teams/{team}"

        response = requests.get(url, headers=self.header).json()
        players_infos = response["players"]
        team_id = response["id"]

        players_df = pd.json_normalize(players_infos)
        players_df["team_id"] = team_id

        return players_df

    def fetch_raw_stream_from_match(self, match):
        url = f"{self.api_url}/matches/{match}"

        raw_stream = requests.get(url, headers=self.header).json()

        first_dataframe = pd.json_normalize(raw_stream["streams_list"])
        first_dataframe["match_id"] = raw_stream["id"]

        return first_dataframe

    def fetch_raw_opponent_from_match(self, match):
        url = f"{self.api_url}/matches/{match}"

        raw_opponent = requests.get(url, headers=self.header).json()["opponents"]

        return pd.json_normalize(raw_opponent)