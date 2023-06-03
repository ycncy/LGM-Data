import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder


def clean_matches_infos_dataframe(matches_infos_dataframe):
    matches_infos_dataframe[["match_id", "tournament_id", "number_of_games", "winner_id", "home_id", "away_id", 'Game 1 winner_id', 'Game 2 winner_id', 'Game 3 winner_id', 'Game 4 winner_id', 'Game 5 winner_id']] = matches_infos_dataframe[["match_id", "tournament_id", "number_of_games", "winner_id", "home_id", "away_id", 'Game 1 winner_id', 'Game 2 winner_id', 'Game 3 winner_id', 'Game 4 winner_id', 'Game 5 winner_id']].fillna(-1).astype("int64")

    matches_infos_dataframe[["name", "tournament_tier", "tournament_name"]] = matches_infos_dataframe[["name", "tournament_tier", "tournament_name"]].astype(str)

    matches_infos_dataframe["tournament_has_bracket"] = matches_infos_dataframe["tournament_has_bracket"].astype(bool).astype(int)

    return matches_infos_dataframe


def generate_data_representations(matches_infos_dataframe):
    dataframe_home_team = matches_infos_dataframe.rename({"away_id": "opponent_id", "home_id": "team_id"}, axis=1)
    dataframe_away_team = matches_infos_dataframe.rename({"home_id": "opponent_id", "away_id": "team_id"}, axis=1)

    concatenated_matches_infos_dataframe = pd.concat([dataframe_home_team, dataframe_away_team], ignore_index=True)

    label_encoder = LabelEncoder()

    concatenated_matches_infos_dataframe['tournament_name'] = label_encoder.fit_transform(concatenated_matches_infos_dataframe['tournament_name'])
    concatenated_matches_infos_dataframe['tournament_tier'] = label_encoder.fit_transform(concatenated_matches_infos_dataframe['tournament_tier'])
    concatenated_matches_infos_dataframe['name'] = label_encoder.fit_transform(concatenated_matches_infos_dataframe['name'])

    number_of_matches_won_by_team_id_after_won_game_1 = concatenated_matches_infos_dataframe[(concatenated_matches_infos_dataframe["team_id"] == concatenated_matches_infos_dataframe["winner_id"]) & (concatenated_matches_infos_dataframe["team_id"] == concatenated_matches_infos_dataframe["Game 1 winner_id"])].groupby("team_id")["team_id"].count()

    number_of_matches_played_by_team_id = concatenated_matches_infos_dataframe.groupby("team_id")["team_id"].count()

    percentage_of_win_after_won_game_1 = number_of_matches_won_by_team_id_after_won_game_1 / number_of_matches_played_by_team_id * 100

    concatenated_matches_infos_dataframe = concatenated_matches_infos_dataframe.merge(percentage_of_win_after_won_game_1.to_frame(), left_on="team_id", right_index=True, how="left")

    concatenated_matches_infos_dataframe.rename(columns={"team_id_y": "percentage_of_win_after_won_game_1", "team_id_x": "team_id"}, inplace=True)

    concatenated_matches_infos_dataframe['percentage_of_win_against_opponent'] = concatenated_matches_infos_dataframe.apply(
        lambda row: concatenated_matches_infos_dataframe[(concatenated_matches_infos_dataframe['team_id'] == row['team_id']) & (concatenated_matches_infos_dataframe['opponent_id'] == row['opponent_id'])]['winner_id'].value_counts(normalize=True).get(row['team_id'], np.nan) if row['team_id'] != row['opponent_id'] else np.nan, axis=1)

    number_of_matches_won_in_n_games_by_team_id = concatenated_matches_infos_dataframe[concatenated_matches_infos_dataframe["team_id"] == concatenated_matches_infos_dataframe["winner_id"]].groupby(["number_of_games", "team_id"])["team_id"].count()

    number_of_matches_played_by_team_id = concatenated_matches_infos_dataframe.groupby(["number_of_games", "team_id"])["team_id"].count()

    percentage_of_win_in_matches_of_n_games = number_of_matches_won_in_n_games_by_team_id / number_of_matches_played_by_team_id

    concatenated_matches_infos_dataframe = concatenated_matches_infos_dataframe.merge(percentage_of_win_in_matches_of_n_games.to_frame(), left_on=["number_of_games", "team_id"], right_index=True, how="left")

    concatenated_matches_infos_dataframe.rename(columns={"team_id_y": "percentage_of_win_after_n_games", "team_id_x": "team_id"}, inplace=True)

    columns_to_update = ["Game 1 winner_id", "Game 2 winner_id", "Game 3 winner_id", "Game 4 winner_id", "Game 5 winner_id", "winner_id"]

    for column in columns_to_update:
        concatenated_matches_infos_dataframe[column] = np.where(concatenated_matches_infos_dataframe[column] == concatenated_matches_infos_dataframe["team_id"], 1, np.where(concatenated_matches_infos_dataframe[column] == -1, -1, 0))

    return concatenated_matches_infos_dataframe


def split_and_encode_dataframe(dataframe):
    x_dataframe = dataframe[["match_id", "tournament_id", "number_of_games", "name", "team_id", "opponent_id", "tournament_has_bracket", "tournament_tier", "tournament_name"]]
    # y_dataframe = dataframe[["winner_id", 'Game 1 winner_id', 'Game 2 winner_id', 'Game 3 winner_id', 'Game 4 winner_id', 'Game 5 winner_id']]
    y_dataframe = dataframe["winner_id"]

    label_encoder = LabelEncoder()
    x_dataframe.loc[:, 'tournament_tier'] = label_encoder.fit_transform(x_dataframe['tournament_tier'])

    simple_imputer = SimpleImputer(strategy='mean')
    x_dataframe = simple_imputer.fit_transform(x_dataframe)

    x_train, x_test, y_train, y_test = train_test_split(x_dataframe, y_dataframe, test_size=0.2)

    return x_train, x_test, y_train, y_test