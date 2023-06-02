import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.impute import SimpleImputer
from sklearn.model_selection import train_test_split
from sklearn.naive_bayes import GaussianNB
from sklearn.preprocessing import LabelEncoder
from sklearn.svm import SVC

# Charger les données
df = pd.read_pickle("../dataframes/final_dataframe")

features = df[
    ['match_id', 'tournament_id', 'number_of_games', 'team_id', 'opponent_id', 'tournament_tier', 'tournament_has_bracket', 'percentage_of_win_after_won_game_1', 'percentage_of_win_against_opponent', 'percentage_of_win_after_n_games']]
targets = df[['Game 1 winner_id', 'Game 2 winner_id', 'Game 3 winner_id', 'Game 4 winner_id', 'Game 5 winner_id']]

df['tournament_has_bracket'] = df['tournament_has_bracket'].astype(int)

X = df[['match_id', 'tournament_id', 'number_of_games', 'tournament_tier', 'tournament_has_bracket', 'percentage_of_win_after_won_game_1', 'percentage_of_win_against_opponent', 'percentage_of_win_after_n_games']]

y = np.where(df['winner_id'] == df['team_id'], 1, 0)

label_encoder = LabelEncoder()
X['tournament_tier'] = label_encoder.fit_transform(X['tournament_tier'])

imputer = SimpleImputer(strategy='mean')
X = imputer.fit_transform(X)

with open("predictions.txt", "w") as file:
    for target in ['winner_id']:
        y = np.where(df[target] == df['team_id'], 1, 0)

        target_feature = target

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)

        model = SVC(
            C=1.0,  # Paramètre de régularisation
            kernel='rbf',  # Noyau utilisé pour le modèle SVC ('rbf' pour un noyau gaussien)
            gamma='scale',  # Coefficient gamma pour le noyau ('scale' utilise 1 / (n_features * X.var()) par défaut)
            random_state=42,
            probability=True,
        )
        model.fit(X_train, y_train)

        proba_predictions = model.predict_proba(X_test)

        file.write(target + "\n\n")

        for i, proba in enumerate(proba_predictions):
            match_id = df.iloc[i]['match_id']
            team_id = df.iloc[i]['team_id']
            prediction = proba[1]
            file.write(f"Match ID: {match_id}, Team ID : {team_id}, Probabilité de l'équipe ID: {prediction}\n")

        file.write("\n")