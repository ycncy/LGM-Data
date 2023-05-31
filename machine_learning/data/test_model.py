import pandas as pd
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score

df = pd.read_pickle("../dataframes/final_dataframe")

features = df[['match_id', 'name', 'tournament_id', 'number_of_games', 'team_id', 'opponent_id', 'tournament_tier', 'tournament_has_bracket', 'tournament_name']]

target_columns = ['Game 1 winner_id', 'Game 2 winner_id', 'Game 3 winner_id', 'Game 4 winner_id', 'Game 5 winner_id', 'winner_id']

for target_column in target_columns:
    target = df[target_column]

    X_train, X_val, y_train, y_val = train_test_split(features, target, test_size=0.2)

    hyperparameters = {'C': [0.1, 1.0, 10.0], 'max_iter': [100, 500, 1000]}
    grid_search = GridSearchCV(estimator=LogisticRegression(), param_grid=hyperparameters, cv=5, scoring='accuracy', n_jobs=-1)

    grid_search.fit(X_train, y_train)

    best_params = grid_search.best_params_

    best_model = LogisticRegression(**best_params)
    best_model.fit(X_train, y_train)

    y_pred = best_model.predict(X_val)
    accuracy = accuracy_score(y_val, y_pred)
    precision = precision_score(y_val, y_pred, average="micro")
    recall = recall_score(y_val, y_pred, average="micro")
    f1 = f1_score(y_val, y_pred, average="micro")

    print("Target column:", target_column)
    print("Accuracy:", accuracy)
    print("Precision:", precision)
    print("Recall:", recall)
    print("F1-score:", f1)

    new_data = pd.DataFrame(features)
    new_predictions = best_model.predict_proba(new_data)[:, 1]

    print(new_predictions)
