import numpy as np
import pandas as pd


def clean_matches_infos_dataframe(matches_infos_dataframe):
    matches_infos_dataframe[["match_id", "tournament_id", "number_of_games", "winner_id", "home_id", "away_id", 'Game 1 winner_id', 'Game 2 winner_id', 'Game 3 winner_id', 'Game 4 winner_id', 'Game 5 winner_id']] = matches_infos_dataframe[["match_id", "tournament_id", "number_of_games", "winner_id", "home_id", "away_id", 'Game 1 winner_id', 'Game 2 winner_id', 'Game 3 winner_id', 'Game 4 winner_id', 'Game 5 winner_id']].fillna(-1).astype("int64")

    matches_infos_dataframe[["name", "tournament_tier", "tournament_name"]] = matches_infos_dataframe[["name", "tournament_tier", "tournament_name"]].astype(str)

    matches_infos_dataframe["tournament_has_bracket"] = matches_infos_dataframe["tournament_has_bracket"].astype(bool).astype(int)

    return matches_infos_dataframe


def generate_data_representations(matches_infos_dataframe):
    matches_infos_dataframe = pd.read_pickle("../machine_learning/dataframes/data")

    dataframe_home_team = matches_infos_dataframe.rename({"away_id": "opponent_id", "home_id": "team_id"}, axis=1)
    dataframe_away_team = matches_infos_dataframe.rename({"home_id": "opponent_id", "away_id": "team_id"}, axis=1)

    concatenated_matches_infos_dataframe = pd.concat([dataframe_home_team, dataframe_away_team], ignore_index=True)

    team_won_games = concatenated_matches_infos_dataframe[concatenated_matches_infos_dataframe["team_id"] == concatenated_matches_infos_dataframe["winner_id"]]
    team_won_games = team_won_games[team_won_games["number_of_games"] > 1]

    percentage_of_win_per_team_after_won_game_1 = team_won_games[team_won_games["winner_id"] == team_won_games["Game 1 winner_id"]].groupby("team_id").size() / team_won_games.groupby("team_id").size() * 100

    percentage_of_win_per_team_after_won_game_1.name = "percentage_of_win_per_team_after_won_game_1"

    team_won_games = pd.merge(team_won_games, percentage_of_win_per_team_after_won_game_1.to_frame(), left_on="team_id", right_index=True, how="left")

    # nparray_home_team = np.array2string(dataframe_home_team.to_numpy(), separator=', ')
    # nparray_away_team = np.array2string(dataframe_away_team.to_numpy(), separator=', ')

    # return nparray_home_team, nparray_away_team
