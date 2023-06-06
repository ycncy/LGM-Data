import asyncio

from sklearn.metrics import accuracy_score

from machine_learning.models.logistic_regression.logistic_regression import LogisticRegressionModel
from machine_learning.pre_process.collect_data import DataCollector
from machine_learning.pre_process.data_processing import *

data_collector = DataCollector("34.155.63.44", "admin", "azertyuiop", "main")

print("Collecte des données...")
matches_df = asyncio.run(data_collector.run_collect_data_method())

print("Nettoyage des données...")
matches_df = clean_matches_infos_dataframe(matches_df)

matches_df = generate_data_representations(matches_df)

X_train, X_test, y_train, y_test = split_and_encode_dataframe(matches_df)

best_param = {'C': 0.01, 'max_iter': 1000, 'penalty': 'l2', 'solver': 'newton-cg'}

model = LogisticRegressionModel(best_params=best_param)

model.train(X_train, y_train)

predictions_cc = model.predict(X_test)

accuracy_cc = accuracy_score(y_test, predictions_cc)

print("Précision du modèle CalibratedClassifierCV:", accuracy_cc)

model.save_model("logistic_regression_model.pkl")