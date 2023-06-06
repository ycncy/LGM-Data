import asyncio

from machine_learning.models.logistic_regression.logistic_regression import LogisticRegressionModel
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
    'C': [0.01, 0.1, 1.0, 10.0],
    'penalty': ['l1', 'l2', 'none'],
    'solver': ['liblinear', 'saga', 'lbfgs', 'newton-cg', 'sag'],
    'max_iter': [1000, 2000, 3000, 6000, 8000, 10000, 15000]
}

print("Recherche des meilleurs paramètres...")
grid_search = GridSearch(LogisticRegressionModel(), param_grid)

grid_search.search_best_params(X_train, y_train)

best_params = grid_search.get_best_params()

print("Meilleurs paramètres pour la Régression Linéaire : ", best_params)