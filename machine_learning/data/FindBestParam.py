import numpy as np
import pandas as pd
from sklearn.calibration import CalibratedClassifierCV
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.preprocessing import LabelEncoder

# Charger les données
df = pd.read_pickle("../dataframes/final_dataframe_modified")

features = df[['match_id', 'tournament_id', 'number_of_games', 'team_id', 'opponent_id', 'tournament_tier',
                'tournament_has_bracket', 'percentage_of_win_after_won_game_1', 'percentage_of_win_against_opponent',
                'percentage_of_win_after_n_games']]
targets = df[['Game 1 winner_id', 'Game 2 winner_id', 'Game 3 winner_id', 'Game 4 winner_id', 'Game 5 winner_id']]

df['tournament_has_bracket'] = df['tournament_has_bracket'].astype(int)

X = df[['match_id', 'tournament_id', 'number_of_games', 'tournament_tier', 'tournament_has_bracket',
        'percentage_of_win_after_won_game_1', 'percentage_of_win_against_opponent', 'percentage_of_win_after_n_games']]

y = df['winner_id']  # Utiliser la colonne "winner_id" comme variable cible

label_encoder = LabelEncoder()
X['tournament_tier'] = label_encoder.fit_transform(X['tournament_tier'])

imputer = SimpleImputer(strategy='mean')
X = imputer.fit_transform(X)

# Diviser les données en ensembles d'entraînement et de test
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

Bestparam_rc = {'max_depth': 10, 'max_features': 'log2', 'min_samples_leaf': 4, 'min_samples_split': 10, 'n_estimators': 50}

model = RandomForestClassifier(**Bestparam_rc)
model.fit(X_train, y_train)


# Définir les hyperparamètres à tester pour CalibratedClassifierCV
param_grid_cc = {
    'estimator__n_estimators': [50, 100, 200],
    'estimator__min_samples_leaf': [1, 5, 10]
}

# Créer un objet GridSearchCV pour le modèle CalibratedClassifierCV
grid_search_cc = GridSearchCV(
    CalibratedClassifierCV(model),
    param_grid_cc,
    cv=5
)

# Effectuer la recherche d'hyperparamètres pour CalibratedClassifierCV
grid_search_cc.fit(X_train, y_train)

# Afficher les meilleurs hyperparamètres et le score associé pour CalibratedClassifierCV
print("Meilleurs hyperparamètres pour CalibratedClassifierCV :")
print(grid_search_cc.best_params_)
print("Score :")
print(grid_search_cc.best_score_)
