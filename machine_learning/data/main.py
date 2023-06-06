import joblib

from machine_learning.pre_process.collect_data import DataCollector
from machine_learning.pre_process.data_processing import *

model = joblib.load("../../scripts/machine_learning/logistic_regression/logistic_regression_model.pkl")

data_collector = DataCollector("34.155.63.44", "admin", "azertyuiop", "main")

# print("Collecte des données...")
# matches_df = asyncio.run(data_collector.run_collect_data_method())
#
# print("Nettoyage des données...")
# matches_df = clean_matches_infos_dataframe(matches_df)
#
# old_matches_df = generate_data_representations(matches_df)
#
# old_matches_df.to_pickle("./df/old_matches_df")
#
# print("Collecte des données...")
# matches_df = asyncio.run(data_collector.run_collect_upcoming_matches())
#
# print("Nettoyage des données...")
# matches_df = clean_matches_infos_dataframe(matches_df)
#
# matches_df.to_pickle("./df/matches_df")

matches_df = pd.read_pickle("./df/matches_df")

matches_df = matches_df.replace('None', np.nan)

x_dataframe = matches_df[['match_id', 'tournament_id', 'number_of_games', 'tournament_tier', 'tournament_has_bracket', 'percentage_of_win_after_won_game_1', 'percentage_of_win_against_opponent', 'percentage_of_win_after_n_games']]

label_encoder = LabelEncoder()
x_dataframe.loc[:, 'tournament_tier'] = label_encoder.fit_transform(x_dataframe['tournament_tier'])

simple_imputer = SimpleImputer(strategy='mean')
x_dataframe = simple_imputer.fit_transform(x_dataframe)

predictions = model.predict_proba(x_dataframe)

print("Prédictions:", predictions[:, 1])

matches_df['winner_prediction'] = 1 / predictions[:, 1]

print(matches_df)

matches_df.to_pickle("matches_df")