import datetime
import time

import pandas as pd
import requests


class DataExtractor:

    def __init__(self):
        self.api_key_list = ["Jj_EamWkuCYJiBROZ6YjL69JE8g33IhOBcvLYCV8WFFGOi_CILc", "8KFYOfkL2CcvuYJFqQNOPPzahE8NdrgNvSnaI7qnX35wkVMO92c",
                             "kie1ZNdJz1FzqaCEkBjW7c5fL9-p91Wj9cq24BHWbdg7RuM4Emc", "87mdyvzjLRlXdIjHlMzgfXPZs2ZxB6XfUE27sNlhh4byBzWK_HM",
                             "c5RlYlk_JeeAcY7nONW59Y1eRXhyTxxdDOvdkvKfGPl5ZonAB14", "mnYh70jlLSFNbN8oyhNyJhCxNIPTkeE0T8LiS4A7tj6M-XjCYH0",
                             "QKufsi6ZbCvMLqXyjuhAm0NdxzFrWAfQ8ESBKTEtbEMngz0k6hU", "RKgx3tgV9gbXcqe2CjGLvTbSEbKAwjyhlzb4MiMAKKc78Cpo_PM",
                             "4OvqoW8WwOUUNmCOJDiCS-rWSGowbcOss4Hn-tTqm6Me-FM4GXI", "LYKeSzkZG5h96CANKNl_ssFDCcb84Cv3tjMCWj4tCRI_1FpzXrQ",
                             "IbuMRytPybtW_2wQ0iLbZZcr4zYx0hfQsAt07MLU5WY3b_M8fsc", "_3Q2-zdmQe1Yp92GWNN3nRYwbSyoD81DTMfC8wxgxLzCprWE28k",
                             "OS_egAWqsE7_WSvJfHHMa-uZIA7mDZitPZaRUuoPOrfI65UirWQ"]
        self.api_key_index = 0
        self.api_call_counter = 0
        self.api_key = self.api_key_list[self.api_key_index]
        self.api_url = "https://api.pandascore.co"
        self.header = {"accept": "application/json", "authorization": f"Bearer {self.api_key}"}

    def set_api_key(self, new_api_key):
        self.api_key = new_api_key
        self.header = {"accept": "application/json", "authorization": f"Bearer {new_api_key}"}

    def check_api_key(self):
        if self.api_call_counter % 1000 == 0 and self.api_call_counter != 0:
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
