import asyncio
from machine_learning.models.gradient_boosting.gradient_boosting import GradientBoostingModel
from machine_learning.pre_process.collect_data import DataCollector
from machine_learning.pre_process.data_processing import *

data_collector = DataCollector("34.155.63.44", "admin", "azertyuiop", "main")

print("Collecte des données...")
matches_df = asyncio.run(data_collector.run_collect_data_method())

print("Nettoyage des données...")
matches_df = clean_matches_infos_dataframe(matches_df)

matches_df = generate_data_representations(matches_df)

X_train, X_test, y_train, y_test = split_and_encode_dataframe(matches_df)

best_params = {'learning_rate': 0.01, 'max_depth': 3, 'n_estimators': 300}

model = GradientBoostingModel(best_params)

model.train(X_train, y_train)

print(model.mean_cross_val_score(X_train, y_train, 5))