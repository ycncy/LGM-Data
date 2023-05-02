import datetime
import time

import requests
import pandas as pd


class DataExtractor:

    def __init__(self):
        self.api_key_list = ["c5RlYlk_JeeAcY7nONW59Y1eRXhyTxxdDOvdkvKfGPl5ZonAB14", "kie1ZNdJz1FzqaCEkBjW7c5fL9-p91Wj9cq24BHWbdg7RuM4Emc", "8KFYOfkL2CcvuYJFqQNOPPzahE8NdrgNvSnaI7qnX35wkVMO92c",
                             "mnYh70jlLSFNbN8oyhNyJhCxNIPTkeE0T8LiS4A7tj6M-XjCYH0", "87mdyvzjLRlXdIjHlMzgfXPZs2ZxB6XfUE27sNlhh4byBzWK_HM", "_3Q2-zdmQe1Yp92GWNN3nRYwbSyoD81DTMfC8wxgxLzCprWE28k",
                             "iIQ96kjaO3LAmgkle-hG6sPIuS6qEz6OvS3KogDMq5CUP6Nbl1Y", "4OvqoW8WwOUUNmCOJDiCS-rWSGowbcOss4Hn-tTqm6Me-FM4GXI", "QKufsi6ZbCvMLqXyjuhAm0NdxzFrWAfQ8ESBKTEtbEMngz0k6hU", ]
        self.api_key_index = 0
        self.api_call_counter = 0
        self.api_key = self.api_key_list[self.api_key_index]
        self.api_url = "https://api.pandascore.co"
        self.header = {"accept": "application/json", "authorization": f"Bearer {self.api_key}"}

    def set_api_key(self, new_api_key):
        self.api_key = new_api_key
        self.header = {"accept": "application/json", "authorization": f"Bearer {new_api_key}"}

    def check_api_key(self):
        if self.api_call_counter % 1000 == 0:
            if self.api_key_index == len(self.api_key_list) - 1:
                datetime_now = datetime.datetime.now()
                next_hour = (datetime_now.hour + 1)

                datetime_next_hour = datetime.datetime(year=datetime_now.year, month=datetime_now.month, day=datetime_now.day, hour=next_hour, minute=0, second=0)

                time.sleep((datetime_next_hour - datetime_now).total_seconds())

                self.api_key_index = 0

                self.set_api_key(self.api_key_list[self.api_key_index])

            else:
                self.api_key_index += 1

                self.set_api_key(self.api_key_list[self.api_key_index])

    def fetch_raw_videogames(self, videogames_slug_list):
        videogames_df = pd.DataFrame()

        for videogame_slug in videogames_slug_list:
            self.check_api_key()
            url = f"{self.api_url}/videogames/{videogame_slug}"

            response = requests.get(url, headers=self.header).json()

            temp_dataframe = pd.json_normalize(response)

            videogames_df = pd.concat([videogames_df, temp_dataframe])

            self.api_call_counter += 1

        return videogames_df

    def fetch_raw_leagues(self, videogames_id_list):
        leagues_df = pd.DataFrame()

        for videogame_id in videogames_id_list:
            self.check_api_key()
            url = f"{self.api_url}/videogames/{videogame_id}"

            response = requests.get(url, headers=self.header).json()

            temp_dataframe = pd.json_normalize(response["leagues"])

            temp_dataframe["videogame_id"] = response["id"]

            leagues_df = pd.concat([leagues_df, temp_dataframe])

            self.api_call_counter += 1

        return leagues_df

    def fetch_raw_series(self, leagues_id_list):
        series_df = pd.DataFrame()

        for league_id in leagues_id_list:
            self.check_api_key()
            url = f"{self.api_url}/leagues/{league_id}"

            response = requests.get(url, headers=self.header).json()["series"]

            temp_dataframe = pd.json_normalize(response)

            series_df = pd.concat([series_df, temp_dataframe])

            self.api_call_counter += 1

        return series_df

    def fetch_raw_tournaments(self, series_id_list):
        tournaments_df = pd.DataFrame()

        for serie_id in series_id_list:
            self.check_api_key()
            url = f"https://api.pandascore.co/series/{serie_id}"

            response = requests.get(url, headers=self.header).json()["tournaments"]

            temp_dataframe = pd.json_normalize(response)

            tournaments_df = pd.concat([tournaments_df, temp_dataframe])

            self.api_call_counter += 1

        return tournaments_df

    def fetch_raw_all_matches_infos(self, tournaments_id_list):
        matches_raw_df = pd.DataFrame()
        matches_streams_raw_df = pd.DataFrame()
        matches_games_raw_df = pd.DataFrame()
        matches_opponents_raw_df = pd.DataFrame()

        for tournament_id in tournaments_id_list:
            self.check_api_key()
            url = f"{self.api_url}/tournaments/{tournament_id}/matches"

            matches_info = requests.get(url, headers=self.header).json()

            tournament_matches_streams_df = pd.DataFrame()
            tournament_matches_games_df = pd.DataFrame()
            tournament_matches_opponents_df = pd.DataFrame()

            for match in matches_info:
                if isinstance(match, dict):
                    if "streams_list" in match and isinstance(match["streams_list"], list):
                        tournament_matches_streams_df = pd.concat([tournament_matches_streams_df, pd.json_normalize(match["streams_list"])])
                    else:
                        tournament_matches_streams_df = pd.DataFrame()

                    if "games" in match and isinstance(match["games"], list):
                        tournament_matches_games_df = pd.concat([tournament_matches_games_df, pd.json_normalize(match["games"])])
                    else:
                        tournament_matches_games_df = pd.DataFrame()

                        if "opponents" in match and isinstance(match["opponents"], list):
                            opponents_df = pd.DataFrame()
                            if len(match["opponents"]) == 2:
                                opponents_df = pd.json_normalize({"match_id": match["id"], "home_id": match["opponents"][0]["opponent"]["id"], "away_id": match["opponents"][1]["opponent"]["id"]})
                            elif len(match["opponents"]) == 1:
                                opponents_df = pd.json_normalize({"match_id": match["id"], "home_id": match["opponents"][0]["opponent"]["id"]})
                        else:
                            opponents_df = pd.DataFrame()

                        tournament_matches_opponents_df = pd.concat([tournament_matches_opponents_df, opponents_df])
                        tournament_matches_opponents_df["match_id"] = match["id"]
                        tournament_matches_streams_df["match_id"] = match["id"]

                        match_raw_df = pd.json_normalize(match)

                        matches_raw_df = pd.concat([matches_raw_df, match_raw_df])

                    matches_streams_raw_df = pd.concat([matches_streams_raw_df, tournament_matches_streams_df])
                    matches_games_raw_df = pd.concat([matches_games_raw_df, tournament_matches_games_df])
                    matches_opponents_raw_df = pd.concat([matches_opponents_raw_df, tournament_matches_opponents_df])

            self.api_call_counter += 1

        return matches_raw_df, matches_streams_raw_df, matches_games_raw_df, matches_opponents_raw_df

    def fetch_raw_teams(self, tournaments_id_list):
        teams_raw_infos = []

        for tournament_id in tournaments_id_list:
            url = f"{self.api_url}/tournaments/{tournament_id}/teams?sort=&page=1&per_page=50"

            raw_teams = requests.get(url, headers=self.header).json()

            teams_raw_infos.extend(raw_teams)

        teams_raw_df = pd.json_normalize(teams_raw_infos)

        return teams_raw_df

    def fetch_raw_players_from_teams_id_list(self, teams_id_list):
        players_raw_df = pd.DataFrame()

        for team_id in teams_id_list:
            url = f"{self.api_url}/players?filter[team_id]={team_id}&sort=&page=1&per_page=50"

            response = requests.get(url, headers=self.header).json()

            temp_players_df = pd.json_normalize(response)
            temp_players_df["team_id"] = team_id

            players_raw_df = pd.concat([players_raw_df, temp_players_df])

        return players_raw_df
