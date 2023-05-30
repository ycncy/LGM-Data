import pandas as pd
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

    return concatenated_matches_infos_dataframe

    # percentage_of_win_per_team_after_won_game_1 = concatenated_matches_infos_dataframe[concatenated_matches_infos_dataframe["winner_id"] == concatenated_matches_infos_dataframe["Game 1 winner_id"]].groupby("team_id").size() / concatenated_matches_infos_dataframe.groupby("team_id").size() * 100
    #
    # percentage_of_win_per_team_after_won_game_1.name = "percentage_of_win_per_team_after_won_game_1"
    #
    # concatenated_matches_infos_dataframe = pd.merge(concatenated_matches_infos_dataframe, percentage_of_win_per_team_after_won_game_1.to_frame(), left_on="team_id", right_index=True, how="left")
    #
    # team_games_against_opponent = concatenated_matches_infos_dataframe.groupby(["team_id", "opponent_id"]).size()
    #
    # team_games_against_opponent.name = "number_of_games_against_opponent"
    #
    # team_won_games = pd.merge(concatenated_matches_infos_dataframe, team_games_against_opponent, on=["team_id", "opponent_id"], how='left')
    #
    # df = team_won_games[team_won_games["team_id"] == team_won_games["winner_id"]]
    #
    # percentage_of_win_against_opponent = df.groupby(["team_id", "opponent_id"]).size() / team_games_against_opponent * 100
    # percentage_of_win_against_opponent.name = "percentage_of_win_against_opponent"
    #
    # team_won_games = pd.merge(team_won_games, percentage_of_win_against_opponent.to_frame(), on=["team_id", "opponent_id"], how='left')
    #
    # print(team_won_games)

    # nparray_home_team = np.array2string(dataframe_home_team.to_numpy(), separator=', ')
    # nparray_away_team = np.array2string(dataframe_away_team.to_numpy(), separator=', ')

    # return nparray_home_team, nparray_away_team


def split_dataframe(dataframe):
    x_dataframe = dataframe[["match_id", "tournament_id", "number_of_games", "name", "team_id", "opponent_id", "tournament_has_bracket", "tournament_tier", "tournament_name"]]
    y_dataframe = dataframe[["winner_id", 'Game 1 winner_id', 'Game 2 winner_id', 'Game 3 winner_id', 'Game 4 winner_id', 'Game 5 winner_id']]

    x_train, x_test, y_train, y_test = train_test_split(x_dataframe, y_dataframe, test_size=0.2)

    return x_train, x_test, y_train, y_test