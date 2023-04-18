import pandas as pd


def clean_videogame_dataframe(videogame_info):
    dataframe, id_list = videogame_info

    # On récupère uniquement les colonnes de la base de données finale
    keys_filter = dataframe.filter(items=["id", "name", "slug", "current_version"])

    # Conversion des colonnes dans les types souhaités
    keys_filter["id"] = keys_filter["id"].astype(int)
    keys_filter[["slug", "name"]] = keys_filter[["slug", "name"]].astype(str)

    # Suppression des lignes dupliquées
    cleaned_dataframe = keys_filter.drop_duplicates()

    cleaned_dataframe.set_index("id", inplace=True)

    return cleaned_dataframe, id_list


def clean_league_dataframe(league_info):
    dataframe, id_list = league_info

    # On récupère ici uniquement les colonnes que l'on veut garder dans la table finale
    keys_filter = dataframe.filter(items=["id", "image_url", "name", "videogame_id"])

    # Conversion des colonnes dans les types souhaités
    keys_filter["id"] = keys_filter["id"].apply(pd.to_numeric)

    keys_filter[["name", "image_url"]] = keys_filter[["name", "image_url"]].astype(str)

    # Suppression des lignes dupliquées
    cleaned_dataframe = keys_filter.drop_duplicates()

    cleaned_dataframe.set_index("id", inplace=True)

    return cleaned_dataframe, id_list


def clean_serie_dataframe(serie_info):
    dataframe, id_list = serie_info

    # On récupère ici uniquement les colonnes que l'on veut garder dans la table finale
    keys_filter = dataframe.filter(items=["id", "league_id", "full_name", "slug", "begin_at", "end_at"])

    # Conversion des colonnes dans les types souhaités
    keys_filter[["id", "league_id"]] = keys_filter[["id", "league_id"]].apply(pd.to_numeric)
    keys_filter[["full_name", "slug"]] = keys_filter[["full_name", "slug"]].astype(str)
    keys_filter["begin_at"] = pd.to_datetime(keys_filter["begin_at"], format="%Y-%m-%dT%H:%M:%SZ")
    keys_filter["end_at"] = pd.to_datetime(keys_filter["end_at"], format="%Y-%m-%dT%H:%M:%SZ")

    # Suppression des lignes dupliquées
    cleaned_dataframe = keys_filter.drop_duplicates()

    cleaned_dataframe.set_index("id", inplace=True)

    return cleaned_dataframe, id_list


def clean_tournament_dataframe(tournament_info):
    dataframe, id_list = tournament_info

    # On récupère ici uniquement les colonnes que l'on veut garder dans la table finale
    keys_filter = dataframe.filter(items=["id", "slug", "begin_at", "end_at", "name", "serie_id", "winner_id", "tier", "has_bracket", "prizepool"])

    # Conversion des colonnes dans les types souhaités
    keys_filter[["id", "serie_id", "winner_id"]] = keys_filter[["id", "serie_id", "winner_id"]].apply(pd.to_numeric)
    keys_filter[["name", "slug", "tier"]] = keys_filter[["name", "slug", "tier"]].astype(str)
    keys_filter["has_bracket"] = keys_filter["has_bracket"].astype(bool)
    keys_filter["begin_at"] = pd.to_datetime(keys_filter["begin_at"], format="%Y-%m-%dT%H:%M:%SZ")
    keys_filter["end_at"] = pd.to_datetime(keys_filter["end_at"], format="%Y-%m-%dT%H:%M:%SZ")

    # Suppression des lignes dupliquées
    cleaned_dataframe = keys_filter.drop_duplicates()

    cleaned_dataframe.set_index("id", inplace=True)

    return cleaned_dataframe, id_list


def clean_match_dataframe(match_info):
    dataframe, id_list = match_info

    # On récupère ici uniquement les colonnes que l'on veut garder dans la table finale
    keys_filter = dataframe.filter(items=["id", "name", "slug", "match_type", "number_of_games", "tournament_id", "status", "draw", "winner_id", "original_scheduled_at", "scheduled_at", "begin_at", "end_at"])

    # Conversion des colonnes dans les types souhaités
    keys_filter[["id", "tournament_id", "winner_id", "number_of_games"]] = keys_filter[["id", "tournament_id", "winner_id", "number_of_games"]].apply(pd.to_numeric)

    keys_filter[["slug", "status", "name", "match_type"]] = keys_filter[["slug", "status", "name", "match_type"]].astype(str)

    keys_filter["draw"] = keys_filter["draw"].astype(bool)

    keys_filter["begin_at"] = pd.to_datetime(keys_filter["begin_at"], format="%Y-%m-%dT%H:%M:%SZ")
    keys_filter["end_at"] = pd.to_datetime(keys_filter["end_at"], format="%Y-%m-%dT%H:%M:%SZ")
    keys_filter["original_scheduled_at"] = pd.to_datetime(keys_filter["original_scheduled_at"], format="%Y-%m-%dT%H:%M:%SZ")
    keys_filter["scheduled_at"] = pd.to_datetime(keys_filter["scheduled_at"], format="%Y-%m-%dT%H:%M:%SZ")

    # Suppression des lignes dupliquées
    cleaned_dataframe = keys_filter.drop_duplicates()

    cleaned_dataframe.set_index("id", inplace=True)

    return cleaned_dataframe, id_list


def clean_games_dataframe(games_info):
    # On récupère ici uniquement les colonnes que l'on veut garder dans la table finale
    keys_filter = games_info.filter(items=["id", "begin_at", "end_at", "match_id", "finished", "winner.id", "forfeit", "length", "complete"])

    # Conversion des colonnes dans les types souhaités
    keys_filter[["id", "match_id", "winner.id", "length"]] = keys_filter[["id", "match_id", "winner.id", "length"]].apply(pd.to_numeric)

    keys_filter[["finished", "forfeit", "complete"]] = keys_filter[["finished", "forfeit", "complete"]].astype(bool)

    keys_filter["begin_at"] = pd.to_datetime(keys_filter["begin_at"], format="%Y-%m-%dT%H:%M:%SZ")
    keys_filter["end_at"] = pd.to_datetime(keys_filter["end_at"], format="%Y-%m-%dT%H:%M:%SZ")

    # Suppression des lignes dupliquées
    cleaned_dataframe = keys_filter.drop_duplicates()

    cleaned_dataframe.set_index("id", inplace=True)

    return cleaned_dataframe


def clean_tournaments_participants_dataframe(tournament_info):
    dataframe, id_list = tournament_info

    # On récupère ici uniquement les colonnes que l'on veut garder dans la table finale
    keys_filter = dataframe.filter(items=["id", "expected_roster"])

    # On récupère l'id du tournoi passé en paramètres et on créé un dictionnaire vide
    tournament_id = keys_filter["id"][0]

    # On créé une liste vide pour y ajouter les futurs valeurs du dataframe (seulement les valeurs !!!!!!!!!!)
    new_dataframe_values = []

    # On ajoute toutes les futurs valeurs (l'id du tournoi et l'id de l'équipe)
    for roster in keys_filter["expected_roster"][0]:
        new_row = [int(tournament_id), int(roster["team"]["id"])]
        new_dataframe_values.append(new_row)

    # On créé le dataframe avec les valeurs créées juste au dessus
    dataframe = pd.DataFrame(new_dataframe_values, columns=["tournament_id", "team_id"])

    # Suppression des lignes dupliquées
    cleaned_dataframe = dataframe.drop_duplicates()

    return cleaned_dataframe


def clean_players_dataframe(players_dataframe):
    # On retire les colonne inutile
    keys_filter_players_dataframe = players_dataframe.drop(["modified_at", "birthday"], axis=1)

    # Conversion des colonnes dans les types souhaités
    keys_filter_players_dataframe[["first_name", "last_name", "nationality", "slug", "role", "image_url", "name"]] = keys_filter_players_dataframe[["first_name", "last_name", "nationality", "slug", "role", "image_url", "name"]].astype(str)
    keys_filter_players_dataframe["age"] = keys_filter_players_dataframe["age"].fillna(999).astype(int)
    keys_filter_players_dataframe[["id", "team_id"]] = keys_filter_players_dataframe[["id", "team_id"]].astype(int)

    # Suppression des lignes dupliquées
    cleaned_dataframe = keys_filter_players_dataframe.drop_duplicates()

    return cleaned_dataframe


def clean_team_dataframe(team_dataframe):
    # On récupère uniquement les colonnes de la base de données finale
    keys_filter = team_dataframe.filter(["id", "acronym", "image_url", "slug", "name", "location"])

    # Conversion des colonnes dans les types souhaités
    keys_filter["id"] = keys_filter["id"].astype(int)
    keys_filter[["acronym", "image_url", "slug", "name", "location"]] = keys_filter[["acronym", "image_url", "slug", "name", "location"]].astype(str)

    # Suppression des lignes dupliquées
    cleaned_dataframe = keys_filter.drop_duplicates()

    cleaned_dataframe.set_index("id", inplace=True)

    return cleaned_dataframe


def clean_streams_dataframe(match_dataframe):
    # On récupère uniquement les colonnes de la base de données finale
    keys_filter = match_dataframe.drop("main", axis=1)

    keys_filter["match_id"] = keys_filter["match_id"].astype(int)
    keys_filter["official"] = keys_filter["official"].astype(bool)

    # Suppression des lignes dupliquées
    cleaned_dataframe = keys_filter.drop_duplicates()

    return cleaned_dataframe


def clean_opponents_dataframe(match_dataframe):
    # On récupère uniquement les colonnes de la base de données finale
    keys_filter = match_dataframe.drop("main", axis=1)

    keys_filter["match_id"] = keys_filter["match_id"].astype(int)
    keys_filter["official"] = keys_filter["official"].astype(bool)

    # Suppression des lignes dupliquées
    cleaned_dataframe = keys_filter.drop_duplicates()

    return cleaned_dataframe