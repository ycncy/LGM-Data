import datetime
import time

import requests
import pandas as pd


# "mnYh70jlLSFNbN8oyhNyJhCxNIPTkeE0T8LiS4A7tj6M-XjCYH0", "87mdyvzjLRlXdIjHlMzgfXPZs2ZxB6XfUE27sNlhh4byBzWK_HM", "_3Q2-zdmQe1Yp92GWNN3nRYwbSyoD81DTMfC8wxgxLzCprWE28k",
# "iIQ96kjaO3LAmgkle-hG6sPIuS6qEz6OvS3KogDMq5CUP6Nbl1Y", "4OvqoW8WwOUUNmCOJDiCS-rWSGowbcOss4Hn-tTqm6Me-FM4GXI", "QKufsi6ZbCvMLqXyjuhAm0NdxzFrWAfQ8ESBKTEtbEMngz0k6hU",
# "c5RlYlk_JeeAcY7nONW59Y1eRXhyTxxdDOvdkvKfGPl5ZonAB14", "kie1ZNdJz1FzqaCEkBjW7c5fL9-p91Wj9cq24BHWbdg7RuM4Emc", "8KFYOfkL2CcvuYJFqQNOPPzahE8NdrgNvSnaI7qnX35wkVMO92c"

# "RKgx3tgV9gbXcqe2CjGLvTbSEbKAwjyhlzb4MiMAKKc78Cpo_PM"

class DataExtractor:

    def __init__(self):
        self.api_key_list = ["mnYh70jlLSFNbN8oyhNyJhCxNIPTkeE0T8LiS4A7tj6M-XjCYH0", "87mdyvzjLRlXdIjHlMzgfXPZs2ZxB6XfUE27sNlhh4byBzWK_HM", "_3Q2-zdmQe1Yp92GWNN3nRYwbSyoD81DTMfC8wxgxLzCprWE28k", ]
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

            matches_raw_df = pd.concat([matches_raw_df, pd.json_normalize(matches_info)])

            for match_info in matches_info:
                if "games" in match_info:
                    matches_games_raw_df = pd.concat([matches_games_raw_df, pd.json_normalize(match_info["games"])])

                if "streams_list" in match_info:
                    streams_df = pd.json_normalize(match_info["streams_list"])
                    streams_df["match_id"] = match_info["id"]

                    matches_streams_raw_df = pd.concat([matches_streams_raw_df, streams_df])

                if "opponents" in match_info and isinstance(match_info["opponents"], list) and len(match_info["opponents"]) >= 2:
                    opponents_dict = [{"home_id": match_info["opponents"][0]["opponent"]["id"], "away_id": match_info["opponents"][1]["opponent"]["id"], "match_id": match_info["id"]}]

                    opponents_df = pd.DataFrame(opponents_dict)

                    matches_opponents_raw_df = pd.concat([matches_opponents_raw_df, opponents_df])

            self.api_call_counter += 1

        return matches_raw_df, matches_streams_raw_df, matches_games_raw_df, matches_opponents_raw_df

    def fetch_raw_teams_and_players_from_tournaments_id_list(self):
        teams_raw_df = pd.DataFrame()
        players_raw_df = pd.DataFrame()

        for page_number in range(1, 100):
            self.check_api_key()

            url = f"{self.api_url}/teams?sort=&page={page_number}&per_page=100"

            teams_info = requests.get(url, headers=self.header).json()

            if isinstance(teams_info, list):

                for team_info in teams_info:
                    teams_raw_df = pd.concat([teams_raw_df, pd.json_normalize(teams_info)])

                    if "players" in team_info:
                        players_df = pd.json_normalize(team_info["players"])
                        players_df["team_id"] = team_info["id"]

                        players_raw_df = pd.concat([players_raw_df, players_df])

                self.api_call_counter += 1

        return teams_raw_df, players_raw_df
