import pandas as pd
import requests

from pipeline.extract.data_extractor import DataExtractor


class DateRangeDataExtractor(DataExtractor):

    def __init__(self):
        super().__init__()

    def fetch_leagues_with_date_range(self, last_record_datetime, current_datetime, videogames_id_list):
        leagues_df = pd.DataFrame()

        for videogame_id in videogames_id_list:
            self.check_api_key()
            url = f"{self.api_url}/videogames/{videogame_id}/leagues?sort=-modified_at&page=1&per_page=100"

            response = requests.get(url, headers=self.header).json()

            first_dataframe = pd.json_normalize(response)

            first_dataframe["modified_at"] = pd.to_datetime(first_dataframe["modified_at"])

            date_filtered_dataframe = first_dataframe.loc[(first_dataframe.modified_at > last_record_datetime) & (first_dataframe.modified_at <= current_datetime)]

            date_filtered_dataframe["videogame_id"] = videogame_id

            leagues_df = pd.concat([leagues_df, date_filtered_dataframe])

            self.api_call_counter += 1

        return leagues_df

    def fetch_series_with_date_range(self, leagues_id_list, last_update_datetime):
        series_df = pd.DataFrame()

        for league_id in leagues_id_list:
            self.check_api_key()
            url = f"{self.api_url}/leagues/{league_id}/series?sort=-modified_at&page=1&per_page=100"

            response = requests.get(url, headers=self.header).json()

            first_dataframe = pd.json_normalize(response)

            if not first_dataframe.empty:
                if "begin_at" in first_dataframe.columns:
                    first_dataframe["begin_at"] = pd.to_datetime(first_dataframe["begin_at"])
                    first_dataframe["end_at"] = pd.to_datetime(first_dataframe["end_at"])

                    date_filtered_dataframe = first_dataframe[((first_dataframe['begin_at'].dt.year >= 2019) | (first_dataframe['end_at'].dt.year >= 2019)) & (first_dataframe.modified_at > last_update_datetime)]

                    date_filtered_dataframe = date_filtered_dataframe.copy().reset_index(drop=False)
                    date_filtered_dataframe.insert(len(date_filtered_dataframe.columns), "videogame_id", response[0]["videogame"]["id"])

                    series_df = pd.concat([series_df, date_filtered_dataframe])

                    self.api_call_counter += 1

        return series_df

    def fetch_tournaments_with_date_range(self, series_id_list, last_update_datetime):
        tournaments_df = pd.DataFrame()

        for serie_id in series_id_list:
            self.check_api_key()
            url = f"{self.api_url}/series/{serie_id}/tournaments?sort=-modified_at&page=1&per_page=100"

            response = requests.get(url, headers=self.header).json()

            first_dataframe = pd.json_normalize(response)

            if not first_dataframe.empty:
                if "begin_at" in first_dataframe.columns:
                    first_dataframe["begin_at"] = pd.to_datetime(first_dataframe["begin_at"])
                    first_dataframe["end_at"] = pd.to_datetime(first_dataframe["end_at"])
                    first_dataframe["modified_at"] = pd.to_datetime(first_dataframe["modified_at"])

                    date_filtered_dataframe = first_dataframe[((first_dataframe['begin_at'].dt.year >= 2019) | (first_dataframe['end_at'].dt.year >= 2019)) & (first_dataframe.modified_at > last_update_datetime)]

                    date_filtered_dataframe = date_filtered_dataframe.copy().reset_index(drop=False)
                    date_filtered_dataframe.insert(len(date_filtered_dataframe.columns), "videogame_id", response[0]["videogame"]["id"])

                    tournaments_df = pd.concat([tournaments_df, date_filtered_dataframe])

                    self.api_call_counter += 1

        return tournaments_df


    def fetch_raw_all_matches_infos_with_date_range(self, tournaments_id_list, last_update_datetime):
        matches_raw_df = pd.DataFrame()
        matches_streams_raw_df = pd.DataFrame()
        matches_games_raw_df = pd.DataFrame()
        matches_opponents_raw_df = pd.DataFrame()

        for tournament_id in tournaments_id_list:
            self.check_api_key()
            url = f"{self.api_url}/tournaments/{tournament_id}/matches"

            matches_info = requests.get(url, headers=self.header).json()

            first_dataframe = pd.json_normalize(matches_info)

            date_filtered_dataframe = first_dataframe[((first_dataframe['begin_at'].dt.year >= 2019) | (first_dataframe['end_at'].dt.year >= 2019)) & (first_dataframe.modified_at > last_update_datetime)]

            matches_raw_df = pd.concat([matches_raw_df, date_filtered_dataframe])

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


    def fetch_raw_teams_and_players_from_tournaments_id_list(self, tournaments_id_list):
        teams_raw_df = pd.DataFrame()
        players_raw_df = pd.DataFrame()

        for tournament_id in tournaments_id_list:
            for page_number in range(1, 20):
                self.check_api_key()

                url = f"{self.api_url}/tournaments/{tournament_id}/teams?sort=&page={page_number}&per_page=100"

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