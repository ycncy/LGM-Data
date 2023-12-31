import asyncio
import datetime

import aiomysql

from machine_learning.pre_process.data_processing import *


class DataCollector:

    def __init__(self, database_host, database_user, database_password, database_name):
        self.database_host = database_host
        self.database_user = database_user
        self.database_password = database_password
        self.database_name = database_name
        self.connection = None
        self.pool = None

    async def connect_to_database(self):
        self.pool = await aiomysql.create_pool(host=self.database_host, user=self.database_user, password=self.database_password, db=self.database_name, )
        print("Connecté à la base de données.")

    async def close_connection(self):
        self.pool.close()
        await self.pool.wait_closed()
        print("Déconnecté de la base de données")

    async def get_single_match_detailed_results(self, match_id):
        select_query = f"SELECT winner_id FROM match_game WHERE match_id = {match_id} ORDER BY end_at ASC"

        async with self.pool.acquire() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(select_query)
                rows = await cursor.fetchall()

        games = {"match_id": match_id}
        for index, row in enumerate(rows):
            games[f"Game {str(index + 1)} winner_id"] = row[0]

        return games

    async def collect_all_matches_infos_to_train(self):
        select_matches_detailed_data_query = """
            SELECT m.id, m.name, m.tournament_id, m.number_of_games, m.winner_id, m.home_id, m.away_id, t.tier, t.has_bracket, t.name
            FROM matchs AS m
            INNER JOIN tournament AS t ON m.tournament_id = t.id
            WHERE m.status = 'finished' AND m.draw = 0 AND YEAR(m.begin_at) > 2021
        """

        async with self.pool.acquire() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(select_matches_detailed_data_query)
                select_query_result = await cursor.fetchall()

        dataframe_from_select_query = pd.DataFrame(select_query_result, columns=["match_id", "name", "tournament_id", "number_of_games", "winner_id", "home_id", "away_id", "tournament_tier", "tournament_has_bracket", "tournament_name"])
        match_ids = dataframe_from_select_query["match_id"].tolist()

        tasks = []
        for match_id in match_ids:
            task = asyncio.ensure_future(self.get_single_match_detailed_results(match_id))
            tasks.append(task)

        matches_detailed_results = await asyncio.gather(*tasks)

        final_dataframe = pd.merge(dataframe_from_select_query, pd.DataFrame(matches_detailed_results), left_on="match_id", right_on="match_id")

        return final_dataframe

    async def run_collect_data_method(self):
        await self.connect_to_database()

        dataframe = await self.collect_all_matches_infos_to_train()

        await self.close_connection()

        return dataframe

    async def collect_upcoming_matches_inf(self):
        current_date = datetime.datetime.now().strftime('%Y-%m-%d')

        select_matches_detailed_data_query = f"""
                    SELECT m.id, m.name, m.tournament_id, m.number_of_games, m.winner_id, m.home_id, m.away_id, t.tier, t.has_bracket, t.name
                    FROM matchs AS m
                    INNER JOIN tournament AS t ON m.tournament_id = t.id
                    WHERE YEAR(m.begin_at) >= {datetime.datetime.now().year} AND m.begin_at > '{current_date}' AND m.home_id IS NOT NULL AND m.away_id IS NOT NULL
                """

        async with self.pool.acquire() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(select_matches_detailed_data_query)
                select_query_result = await cursor.fetchall()

        dataframe_from_select_query = pd.DataFrame(select_query_result, columns=["match_id", "name", "tournament_id", "number_of_games", "winner_id", "home_id", "away_id", "tournament_tier", "tournament_has_bracket", "tournament_name"])
        match_ids = dataframe_from_select_query["match_id"].tolist()

        tasks = []
        for match_id in match_ids:
            task = asyncio.ensure_future(self.get_single_match_detailed_results(match_id))
            tasks.append(task)

        matches_detailed_results = await asyncio.gather(*tasks)

        final_dataframe = pd.merge(dataframe_from_select_query, pd.DataFrame(matches_detailed_results), left_on="match_id", right_on="match_id")

        return final_dataframe

    async def run_collect_upcoming_matches(self):
        await self.connect_to_database()

        dataframe = await self.collect_upcoming_matches_inf()

        await self.close_connection()

        return dataframe