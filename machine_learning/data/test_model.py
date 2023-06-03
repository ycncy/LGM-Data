import numpy as np
import pandas as pd
from sklearn.calibration import CalibratedClassifierCV
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split
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

with open("predictionsAll.txt", "w") as file:
    for target in ['winner_id']:
        target_feature = target

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)

        model = RandomForestClassifier(
            n_estimators=100,  # Nombre d'arbres dans la forêt
            min_samples_leaf=5,  # Nombre minimum d'échantillons requis dans une feuille
            random_state=42
        )
        calibrated_model = CalibratedClassifierCV(model, method='sigmoid', cv=5)

        # Entraînez le modèle calibré
        calibrated_model.fit(X_train, y_train)

        # Faites des prédictions sur les données de test
        y_pred = calibrated_model.predict(X_test)

        # Calculer le score d'exactitude
        accuracy = accuracy_score(y_test, y_pred)

        print("Accuracy Score:", accuracy)

        file.write(target + "\n\n")

        for i in range(len(df)):
            match_id = df.iloc[i]['match_id']
            team_id = df.iloc[i]['team_id']
            prediction = calibrated_model.predict_proba([X[i]])[0][1]
            file.write(f"Match ID: {match_id}, Team ID : {team_id}, Probabilité de l'équipe ID: {prediction}\n")

        file.write("\n")


