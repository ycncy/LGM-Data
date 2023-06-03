import pandas as pd

from machine_learning.models.random_forest.random_forest import RandomForestModel
from machine_learning.pre_process.data_processing import *
from machine_learning.pre_process.collect_data import *

data_collector = DataCollector("34.155.63.44", "admin", "azertyuiop", "main")

print("Collecte des données...")
matches_df = asyncio.run(data_collector.run_collect_data_method())

print("Nettoyage des données...")
matches_df = clean_matches_infos_dataframe(matches_df)

matches_df = generate_data_representations(matches_df)

print(matches_df.head())

x_train, x_test, y_train, y_test = split_and_encode_dataframe(matches_df)

model = RandomForestModel()

param_grid = {
    'n_estimators': [50, 100, 200, 500],
    'max_depth': [None, 2, 4, 5, 10],
    'min_samples_split': [2, 5, 10],
    'min_samples_leaf': [1, 2, 4],
    'max_features': ['sqrt', 'log2']
}
print("Recherche des meilleurs hyperparamètres pour RandomForestClassifier...")
best_params = model.find_best_params(x_train, y_train, param_grid)

print("Meilleurs hyperparamètres pour RandomForestClassifier : ", best_params)

model.calibrate(x_train, y_train, best_params)

print("Entraînement du modèle RandomForestClassifier avec les meilleurs hyperparamètres...")
model.fit_with_best_params(x_train, y_train, best_params)

print("Score pour RandomForestClassifier : ", model.score(x_test, y_test))