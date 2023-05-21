"""

Features : name, team_id, opponent_id, number_of_games, tournament_tier, tournament_has_bracket, serie_name

Result : winner_id, games_result (résultat pour chaque game en gros noté combien de game gagné et combien de game perdues pour chaque équipe)

"""
import mysql.connector


class DataCollector:

    def __init__(self, database_host, database_user, database_password, database_name):
        self.database_host = database_host
        self.database_user = database_user
        self.database_password = database_password
        self.database_name = database_name
        self.connection = None

    def connect_to_database(self):
        self.connection = mysql.connector.connect(host=self.database_host, user=self.database_user, password=self.database_password, database=self.database_name)

        print("Connecté à la base de données.")

    def close_connection(self):
        self.connection.close()

        print("Déconnecté de la base de données")

    def get_opponents_infos(self, match_id, cursor):
        select_query = f"SELECT home_id, away_id FROM match_opponent WHERE match_id = {match_id}"

        cursor.fetchall()
        cursor.execute(select_query)

        opponents = {}

        for row in cursor:
            opponents = {"home_id": row[0], "away_id": row[1]}

        return opponents

    def get_tournament_infos(self, tournament_id, cursor):
        select_query = f"SELECT tier, has_bracket, name FROM tournament WHERE id = {tournament_id}"

        cursor.fetchall()
        cursor.execute(select_query)

        tournament_info = {}

        for row in cursor:
            tournament_info = {"tournament_tier": row[0], "tournament_has_bracket": row[1], "tournament_name": row[2]}

        return tournament_info

    def get_match_detailed_result(self, match_id, cursor):
        select_query = f"SELECT winner_id FROM match_game WHERE match_id = {match_id} ORDER BY end_at ASC"

        cursor.fetchall()
        cursor.execute(select_query)

        games = {}

        for index, row in enumerate(cursor):
            games[str(index + 1)] = row[0]

        return games

    def collect_all_matches_infos_to_train(self):
        cursor = self.connection.cursor()

        select_query = "SELECT id, name, tournament_id, number_of_games, winner_id FROM matchs WHERE status = 'finished' AND draw = 0"

        cursor.execute(select_query)

        select_result = cursor.fetchall()

        matches = []

        for row in select_result:
            try:
                opponents_info = self.get_opponents_infos(row[0], cursor)
                tournament_info = self.get_tournament_infos(row[2], cursor)
                games_detailed_results = self.get_match_detailed_result(row[0], cursor)

                match_info = {
                    "id": row[0],
                    "name": row[1],
                    "tournament_id": row[2],
                    "number_of_games": row[3],
                    "winner_id": row[4],
                    "home_id": opponents_info["home_id"],
                    "away_id": opponents_info["away_id"],
                    "tournament_tier": tournament_info["tournament_tier"],
                    "tournament_has_bracket": tournament_info["tournament_has_bracket"],
                    "tournament_name": tournament_info["tournament_name"],
                    "games_detailed_results": games_detailed_results
                }

                matches.append(match_info)

            except Exception as e:
                print(f"Error : {e}")


mysql_data_manager = DataCollector("lgm.cihggjssark1.eu-west-3.rds.amazonaws.com", "admin", "azertyuiop", "main")
mysql_data_manager.connect_to_database()

mysql_data_manager.collect_all_matches_infos_to_train()
