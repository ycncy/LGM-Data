import pandas as pd


def clean_series_dataframe(series_dataframe):
    #On récupère ici uniquement les colonnes que l'on veut garder dans la table finale
    keys_filter = series_dataframe.filter(items=["id", "league_id", "full_name", "slug", "begin_at", "end_at"])

    #Conversion des colonnes dans les types souhaités
    keys_filter[["id", "league_id"]] = keys_filter[["id", "league_id"]].apply(pd.to_numeric)
    keys_filter[["full_name", "slug"]] = keys_filter[["full_name", "slug"]].astype(str)
    keys_filter["begin_at"] = pd.to_datetime(keys_filter["begin_at"], format="%Y-%m-%dT%H:%M:%SZ")
    keys_filter["end_at"] = pd.to_datetime(keys_filter["end_at"], format="%Y-%m-%dT%H:%M:%SZ")

    #Suppression des lignes dupliquées
    cleaned_dataframe = keys_filter.drop_duplicates()

    return cleaned_dataframe


def clean_matches_dataframe(matches_dataframe):
    #On récupère ici uniquement les colonnes que l'on veut garder dans la table finale
    keys_filter = matches_dataframe.filter(items=["id", "name", "slug", "match_type", "number_of_games", "tournament_id", "status", "draw", "winner_id", "original_scheduled_at", "scheduled_at", "begin_at", "end_at"])

    # Conversion des colonnes dans les types souhaités
    keys_filter[["id", "tournament_id", "winner_id", "number_of_games"]] = keys_filter[["id", "tournament_id", "winner_id", "number_of_games"]].apply(pd.to_numeric)

    keys_filter[["slug", "status", "name", "match_type"]] = keys_filter[["slug", "status", "name", "match_type"]].astype(str)

    keys_filter["draw"] = keys_filter["draw"].astype(bool)

    keys_filter["begin_at"] = pd.to_datetime(keys_filter["begin_at"], format="%Y-%m-%dT%H:%M:%SZ")
    keys_filter["end_at"] = pd.to_datetime(keys_filter["end_at"], format="%Y-%m-%dT%H:%M:%SZ")
    keys_filter["original_scheduled_at"] = pd.to_datetime(keys_filter["original_scheduled_at"], format="%Y-%m-%dT%H:%M:%SZ")
    keys_filter["scheduled_at"] = pd.to_datetime(keys_filter["scheduled_at"], format="%Y-%m-%dT%H:%M:%SZ")

    #Suppression des lignes dupliquées
    cleaned_dataframe = keys_filter.drop_duplicates()

    return cleaned_dataframe


def clean_tournaments_participants_dataframe(tournament_dataframe):
    # On récupère ici uniquement les colonnes que l'on veut garder dans la table finale
    keys_filter = tournament_dataframe.filter(items=["id", "expected_roster"])

    #On récupère l'id du tournoi passé en paramètres et on créé un dictionnaire vide
    tournament_id = keys_filter["id"][0]

    #On créé une liste vide pour y ajouter les futurs valeurs du dataframe (seulement les valeurs !!!!!!!!!!)
    new_dataframe_values = []

    #On ajoute toutes les futurs valeurs (l'id du tournoi et l'id de l'équipe)
    for roster in keys_filter["expected_roster"][0]:
        new_row = [int(tournament_id), int(roster["team"]["id"])]
        new_dataframe_values.append(new_row)

    #On créé le dataframe avec les valeurs créées juste au dessus
    dataframe = pd.DataFrame(new_dataframe_values, columns=["tournament_id", "team_id"])

    #Suppression des lignes dupliquées
    cleaned_dataframe = dataframe.drop_duplicates()

    print(cleaned_dataframe.dtypes)

    return cleaned_dataframe