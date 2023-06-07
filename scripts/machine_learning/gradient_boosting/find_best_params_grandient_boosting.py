import asyncio

from sklearn.ensemble import GradientBoostingClassifier
from sklearn.model_selection import StratifiedKFold
from machine_learning.pre_process.collect_data import DataCollector
from machine_learning.pre_process.data_processing import *
from machine_learning.utils.grid_search import GridSearch

data_collector = DataCollector("34.155.63.44", "admin", "azertyuiop", "main")

print("Collecte des données...")
matches_df = asyncio.run(data_collector.run_collect_data_method())

print("Nettoyage des données...")
matches_df = clean_matches_infos_dataframe(matches_df)

matches_df = generate_data_representations(matches_df)

print("Encodage et split des données...")
X_train, X_test, y_train, y_test = split_and_encode_dataframe(matches_df)

param_grid = {
    'n_estimators': [100, 200, 300],
    'learning_rate': [0.1, 0.01, 0.001],
    'max_depth': [3, 4, 5]
}

model = GradientBoostingClassifier()

stratified_kfold = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)

print("Recherche des meilleurs paramètres...")
grid_search = GridSearch(model, param_grid, cv=stratified_kfold)

grid_search.search_best_params(X_train, y_train)

print("Meilleurs hyperparamètres :", grid_search.get_best_params())