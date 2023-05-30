import asyncio

import aiohttp
import numpy as np
import pandas as pd

from pipeline.extract.data_extractor import DataExtractor


class AsynchronousDateRangeDataExtractor(DataExtractor):

    def __init__(self):
        super().__init__()

    async def fetch_leagues_for_videogame(self, videogame_id, session, last_record_datetime, current_datetime):
        self.check_api_key()
        url = f"{self.api_url}/videogames/{videogame_id}/leagues?sort=-modified_at&page=1&per_page=100"

        async with session.get(url) as response:
            response = await response.json()

            first_dataframe = pd.json_normalize(response)

            first_dataframe["modified_at"] = pd.to_datetime(first_dataframe["modified_at"], format="%Y-%m-%dT%H:%M:%SZ", errors='coerce')

            date_filtered_dataframe = first_dataframe.loc[(first_dataframe.modified_at >= last_record_datetime) & (first_dataframe.modified_at <= current_datetime)]

            date_filtered_dataframe["videogame_id"] = videogame_id

            self.api_call_counter += 1

        return date_filtered_dataframe

    async def fetch_leagues_with_date_range(self, last_record_datetime, current_datetime, videogames_id_list):
        leagues_df = pd.DataFrame()

        async with aiohttp.ClientSession(headers=self.header) as session:
            tasks = [self.fetch_leagues_for_videogame(videogame_id, session, last_record_datetime, current_datetime) for videogame_id in videogames_id_list]
            results = await asyncio.gather(*tasks)

        for result in results:
            leagues_df = pd.concat([leagues_df, result])

        return leagues_df

    async def fetch_series_for_league(self, league_id, session, last_update_datetime, current_datetime):
        series_df = pd.DataFrame()

        self.check_api_key()

        url = f"{self.api_url}/leagues/{league_id}/series?sort=-modified_at&page=1&per_page=100"

        async with session.get(url) as response:
            response = await response.json()

            first_dataframe = pd.json_normalize(response)

            if not first_dataframe.empty:
                if "modified_at" in first_dataframe.columns:
                    first_dataframe["begin_at"] = pd.to_datetime(first_dataframe["begin_at"], format="%Y-%m-%dT%H:%M:%SZ", errors='coerce')
                    first_dataframe["end_at"] = pd.to_datetime(first_dataframe["end_at"], format="%Y-%m-%dT%H:%M:%SZ", errors='coerce')
                    first_dataframe["modified_at"] = pd.to_datetime(first_dataframe["modified_at"], format="%Y-%m-%dT%H:%M:%SZ", errors='coerce')

                    date_filtered_dataframe = first_dataframe[(first_dataframe.modified_at >= last_update_datetime) & (first_dataframe.modified_at <= current_datetime)]

                    date_filtered_dataframe = date_filtered_dataframe.copy().reset_index(drop=False)
                    date_filtered_dataframe.insert(len(date_filtered_dataframe.columns), "videogame_id", response[0]["videogame"]["id"])

                    series_df = pd.concat([series_df, date_filtered_dataframe])

                    self.api_call_counter += 1

        return series_df

    async def fetch_series_with_date_range(self, leagues_id_list, last_update_datetime, current_datetime):
        series_df = pd.DataFrame()

        async with aiohttp.ClientSession(headers=self.header) as session:
            tasks = [self.fetch_series_for_league(league_id, session, last_update_datetime, current_datetime) for league_id in leagues_id_list]
            results = await asyncio.gather(*tasks)

        for result in results:
            series_df = pd.concat([series_df, result])

        return series_df

    async def fetch_tournaments_for_serie(self, serie_id, session, last_update_datetime, current_datetime):
        self.check_api_key()
        url = f"{self.api_url}/series/{serie_id}/tournaments?sort=-modified_at&page=1&per_page=100"

        date_filtered_dataframe = pd.DataFrame()
        try:

            async with session.get(url) as response:

                    tournaments_info = await response.json()

                    first_dataframe = pd.json_normalize(tournaments_info)

                    if not first_dataframe.empty:
                        if "begin_at" in first_dataframe.columns:
                            first_dataframe["begin_at"] = pd.to_datetime(first_dataframe["begin_at"], format="%Y-%m-%dT%H:%M:%SZ", errors='coerce')
                            first_dataframe["end_at"] = pd.to_datetime(first_dataframe["end_at"], format="%Y-%m-%dT%H:%M:%SZ", errors='coerce')
                            first_dataframe["modified_at"] = pd.to_datetime(first_dataframe["modified_at"], format="%Y-%m-%dT%H:%M:%SZ", errors='coerce')

                            date_filtered_dataframe = first_dataframe[(first_dataframe.modified_at >= last_update_datetime) & (first_dataframe.modified_at <= current_datetime)]

                            date_filtered_dataframe = date_filtered_dataframe.copy().reset_index(drop=False)
                            date_filtered_dataframe.insert(len(date_filtered_dataframe.columns), "videogame_id", tournaments_info[0]["videogame"]["id"])

                            self.api_call_counter += 1

        except Exception as e:
            print(e)

        return date_filtered_dataframe

    async def fetch_tournaments_with_date_range(self, series_id_list, last_update_datetime, current_datetime):
        tournaments_df = pd.DataFrame()

        async with aiohttp.ClientSession(headers=self.header) as session:
            tasks = [self.fetch_tournaments_for_serie(serie_id, session, last_update_datetime, current_datetime) for serie_id in series_id_list]
            results = await asyncio.gather(*tasks)

        for result in results:
            tournaments_df = pd.concat([tournaments_df, result])

        return tournaments_df

    async def fetch_matches_for_tournament(self, tournament_id, session, last_update_datetime):
        self.check_api_key()
        url = f"{self.api_url}/tournaments/{tournament_id}/matches"

        matches_raw_df = pd.DataFrame()
        matches_streams_raw_df = pd.DataFrame()
        matches_games_raw_df = pd.DataFrame()
        matches_opponents_raw_df = pd.DataFrame()

        async with session.get(url) as response:
            matches_info = await response.json()

            first_dataframe = pd.json_normalize(matches_info)

            if "begin_at" in first_dataframe.columns:
                first_dataframe["begin_at"] = pd.to_datetime(first_dataframe["begin_at"], format="%Y-%m-%dT%H:%M:%SZ", errors='coerce')
                first_dataframe["end_at"] = pd.to_datetime(first_dataframe["end_at"], format="%Y-%m-%dT%H:%M:%SZ", errors='coerce')
                first_dataframe["modified_at"] = pd.to_datetime(first_dataframe["modified_at"], format="%Y-%m-%dT%H:%M:%SZ", errors='coerce')

                if "winner.modified_at" in first_dataframe.columns:
                    first_dataframe["winner.modified_at"] = pd.to_datetime(first_dataframe["winner.modified_at"], format="%Y-%m-%dT%H:%M:%SZ", errors='coerce')
                    date_filtered_dataframe = first_dataframe[(first_dataframe["modified_at"] >= last_update_datetime) | (first_dataframe["winner.modified_at"] >= last_update_datetime)]
                else:
                    date_filtered_dataframe = first_dataframe[(first_dataframe["modified_at"] >= last_update_datetime)]

                matches_raw_df = pd.concat([matches_raw_df, date_filtered_dataframe])

                if "games" in date_filtered_dataframe.columns:
                    games_df = pd.json_normalize(date_filtered_dataframe["games"].explode())

                    matches_games_raw_df = pd.concat([matches_games_raw_df, games_df])

                if "streams_list" in date_filtered_dataframe.columns:
                    streams_df = pd.json_normalize(date_filtered_dataframe["streams_list"].explode())
                    streams_df["match_id"] = date_filtered_dataframe["id"]

                    matches_streams_raw_df = pd.concat([matches_streams_raw_df, streams_df])

                for _, row in date_filtered_dataframe.iterrows():
                    if "opponents" in date_filtered_dataframe and isinstance(date_filtered_dataframe["opponents"], list) and len(date_filtered_dataframe["opponents"]) >= 2:
                        if pd.to_datetime(date_filtered_dataframe["opponents"][0]["modified_at"]) >= last_update_datetime or pd.to_datetime(date_filtered_dataframe["opponents"][1]["modified_at"]) >= last_update_datetime:
                            opponents_dict = [{"home_id": date_filtered_dataframe["opponents"][0]["opponent"]["id"], "away_id": date_filtered_dataframe["opponents"][1]["opponent"]["id"], "match_id": date_filtered_dataframe["id"]}]

                            opponents_df = pd.DataFrame(opponents_dict)

                            matches_opponents_raw_df = pd.concat([matches_opponents_raw_df, opponents_df])

            self.api_call_counter += 1

        if not matches_opponents_raw_df.empty and not matches_raw_df.empty:
            matches_raw_df = pd.merge(matches_raw_df, matches_opponents_raw_df, left_on="id", right_on="match_id", how="left")
        elif not matches_raw_df.empty and matches_opponents_raw_df.empty:
            matches_raw_df["home_id"] = np.nan
            matches_raw_df["away_id"] = np.nan

        return matches_raw_df, matches_streams_raw_df, matches_games_raw_df

    async def fetch_raw_all_matches_infos_with_date_range(self, tournaments_id_list, last_update_datetime):
        matches_raw_dfs = []
        matches_streams_raw_dfs = []
        matches_games_raw_dfs = []

        async with aiohttp.ClientSession(headers=self.header) as session:
            tasks = [self.fetch_matches_for_tournament(tournament_id, session, last_update_datetime) for tournament_id in tournaments_id_list]
            results = await asyncio.gather(*tasks)

        for result in results:
            matches_raw_df, matches_streams_raw_df, matches_games_raw_df = result

            if not matches_raw_df.empty:
                matches_raw_dfs.append(matches_raw_df)
            if not matches_streams_raw_df.empty:
                matches_streams_raw_dfs.append(matches_streams_raw_df)
            if not matches_games_raw_df.empty:
                matches_games_raw_dfs.append(matches_games_raw_df)

        matches_raw_df = pd.concat(matches_raw_dfs) if len(matches_raw_dfs) > 0 else pd.DataFrame()
        matches_streams_raw_df = pd.concat(matches_streams_raw_dfs) if len(matches_streams_raw_dfs) > 0 else pd.DataFrame()
        matches_games_raw_df = pd.concat(matches_games_raw_dfs) if len(matches_games_raw_dfs) > 0 else pd.DataFrame()

        return matches_raw_df, matches_streams_raw_df, matches_games_raw_df

    async def fetch_teams_and_players_for_tournament_page(self, tournament_id, page_number, session):
        self.check_api_key()
        url = f"{self.api_url}/tournaments/{tournament_id}/teams?sort=&page={page_number}&per_page=100"

        teams_raw_df = pd.DataFrame()
        players_raw_df = pd.DataFrame()

        async with session.get(url) as response:
            teams_info = await response.json()

            if isinstance(teams_info, list):
                for team_info in teams_info:
                    teams_raw_df = pd.concat([teams_raw_df, pd.json_normalize(teams_info)])

                    if "players" in team_info:
                        players_df = pd.json_normalize(team_info["players"])
                        players_df["team_id"] = team_info["id"]

                        players_raw_df = pd.concat([players_raw_df, players_df])

            self.api_call_counter += 1

        return teams_raw_df, players_raw_df

    async def fetch_raw_teams_and_players_from_tournaments_id_list(self, tournaments_id_list):
        teams_raw_dfs = []
        players_raw_dfs = []

        async with aiohttp.ClientSession(headers=self.header) as session:
            tasks = [self.fetch_teams_and_players_for_tournament_page(tournament_id, page_number, session) for tournament_id in tournaments_id_list for page_number in range(1, 20)]
            results = await asyncio.gather(*tasks)

        for result in results:
            if result[0].empty:
                continue
            teams_raw_dfs.append(result[0])

            if result[1].empty:
                continue
            players_raw_dfs.append(result[1])

        if not teams_raw_dfs or not players_raw_dfs:
            return pd.DataFrame(), pd.DataFrame()

        return pd.concat(teams_raw_dfs), pd.concat(players_raw_dfs)
