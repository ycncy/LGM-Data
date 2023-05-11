import pandas as pd


def clean_videogames_dataframe(videogame_raw_df):
    # On récupère uniquement les colonnes de la base de données finale
    keys_filter = videogame_raw_df.filter(items=["id", "name", "slug", "current_version"])

    # Conversion des colonnes dans les types souhaités
    keys_filter["id"] = keys_filter["id"].astype(int)
    keys_filter[["slug", "name"]] = keys_filter[["slug", "name"]].astype(str)

    keys_filter.set_index("id", inplace=True)
    keys_filter.index = keys_filter.index.astype(int)

    # Suppression des lignes dupliquées
    cleaned_dataframe = keys_filter.drop_duplicates()

    return cleaned_dataframe


def clean_leagues_dataframe(leagues_raw_df):
    # On récupère ici uniquement les colonnes que l'on veut garder dans la table finale
    keys_filter = leagues_raw_df.filter(items=["id", "image_url", "name", "videogame_id"])

    # Conversion des colonnes dans les types souhaités
    keys_filter["id"] = keys_filter["id"].astype(int)

    keys_filter[["name", "image_url"]] = keys_filter[["name", "image_url"]].astype(str)

    keys_filter.set_index("id", inplace=True)
    keys_filter.index = keys_filter.index.astype(int)

    # Suppression des lignes dupliquées
    cleaned_dataframe = keys_filter.drop_duplicates()

    return cleaned_dataframe


def clean_series_dataframe(series_raw_df):
    # On récupère ici uniquement les colonnes que l'on veut garder dans la table finale
    keys_filter = series_raw_df.filter(items=["id", "league_id", "full_name", "slug", "begin_at", "end_at"])

    # Conversion des colonnes dans les types souhaités
    keys_filter[["id", "league_id"]] = keys_filter[["id", "league_id"]].astype(int)
    keys_filter[["full_name", "slug"]] = keys_filter[["full_name", "slug"]].astype(str)
    keys_filter["begin_at"] = pd.to_datetime(keys_filter["begin_at"], format="%Y-%m-%dT%H:%M:%SZ")
    keys_filter["end_at"] = pd.to_datetime(keys_filter["end_at"], format="%Y-%m-%dT%H:%M:%SZ")

    keys_filter.set_index("id", inplace=True)
    keys_filter.index = keys_filter.index.astype(int)

    # Suppression des lignes dupliquées
    cleaned_dataframe = keys_filter.drop_duplicates()

    return cleaned_dataframe


def clean_tournaments_dataframe(tournaments_raw_df):
    # On récupère ici uniquement les colonnes que l'on veut garder dans la table finale
    keys_filter = tournaments_raw_df.filter(items=["id", "slug", "begin_at", "end_at", "name", "serie_id", "winner_id", "tier", "has_bracket", "prizepool"])

    # Conversion des colonnes dans les types souhaités
    keys_filter[["id", "serie_id", "winner_id"]] = keys_filter[["id", "serie_id", "winner_id"]].astype(int)
    keys_filter[["name", "slug", "tier"]] = keys_filter[["name", "slug", "tier"]].astype(str)
    keys_filter["has_bracket"] = keys_filter["has_bracket"].astype(bool)
    keys_filter["begin_at"] = pd.to_datetime(keys_filter["begin_at"], format="%Y-%m-%dT%H:%M:%SZ")
    keys_filter["end_at"] = pd.to_datetime(keys_filter["end_at"], format="%Y-%m-%dT%H:%M:%SZ")

    keys_filter.set_index("id", inplace=True)
    keys_filter.index = keys_filter.index.astype(int)

    # Suppression des lignes dupliquées
    cleaned_dataframe = keys_filter.drop_duplicates()

    return cleaned_dataframe


def clean_matches_dataframe(matches_raw_df):
    # On récupère ici uniquement les colonnes que l'on veut garder dans la table finale
    keys_filter = matches_raw_df.filter(items=["id", "name", "slug", "match_type", "number_of_games", "tournament_id", "status", "draw", "winner_id", "original_scheduled_at", "scheduled_at", "begin_at", "end_at", "games_id_list"])

    keys_filter = keys_filter[pd.notnull(keys_filter.id)]

    keys_filter.fillna(-1, inplace=True)

    # Conversion des colonnes dans les types souhaités
    keys_filter[["id", "tournament_id", "winner_id", "number_of_games"]] = keys_filter[["id", "tournament_id", "winner_id", "number_of_games"]].astype(int)

    keys_filter[["slug", "status", "name", "match_type"]] = keys_filter[["slug", "status", "name", "match_type"]].astype(str)

    keys_filter["draw"] = keys_filter["draw"].astype(bool)

    # Suppression des lignes dupliquées
    cleaned_dataframe = keys_filter.drop_duplicates()

    cleaned_dataframe.set_index("id", inplace=True)
    cleaned_dataframe.index = cleaned_dataframe.index.astype(int)

    return cleaned_dataframe


def clean_games_dataframe(games_raw_df):
    # On récupère ici uniquement les colonnes que l'on veut garder dans la table finale
    keys_filter = games_raw_df.filter(items=["id", "begin_at", "end_at", "match_id", "finished", "winner.id", "forfeit", "length", "complete"])

    keys_filter.rename(columns={"winner.id": "winner_id"}, inplace=True)

    keys_filter.dropna(inplace=True, subset=["begin_at", "end_at"])

    keys_filter[["winner_id", "length"]] = keys_filter[["winner_id", "length"]].fillna(-1)
    

    keys_filter = keys_filter[pd.notnull(keys_filter.id)]

    # Conversion des colonnes dans les types souhaités
    keys_filter[["id", "match_id", "winner_id", "length"]] = keys_filter[["id", "match_id", "winner_id", "length"]].astype(int)
    keys_filter[["finished", "forfeit", "complete"]] = keys_filter[["finished", "forfeit", "complete"]].astype(bool)

    # Suppression des lignes dupliquées
    cleaned_dataframe = keys_filter.drop_duplicates()

    cleaned_dataframe.set_index("id", inplace=True)
    cleaned_dataframe.index = cleaned_dataframe.index.astype(int)

    return cleaned_dataframe


def clean_streams_dataframe(streams_raw_df):
    # On récupère uniquement les colonnes de la base de données finale
    keys_filter = streams_raw_df.drop("main", axis=1)

    keys_filter["match_id"] = keys_filter["match_id"].astype(int)
    keys_filter["official"] = keys_filter["official"].astype(bool)

    keys_filter.set_index("match_id", inplace=True)

    # Suppression des lignes dupliquées
    cleaned_dataframe = keys_filter.drop_duplicates()

    return cleaned_dataframe


def clean_opponents_dataframe(opponents_raw_df):
    opponents_raw_df[["match_id", "home_id", "away_id"]] = opponents_raw_df[["match_id", "home_id", "away_id"]].fillna(-1).astype(int)

    opponents_raw_df.set_index("match_id", inplace=True)

    # Suppression des lignes dupliquées
    cleaned_dataframe = opponents_raw_df.drop_duplicates()

    return cleaned_dataframe


def clean_players_dataframe(players_raw_dataframe):
    # On retire les colonne inutile
    keys_filter_players_dataframe = players_raw_dataframe.drop(["modified_at", "birthday"], axis=1)

    keys_filter_players_dataframe.dropna(subset=["id", "team_id"], inplace=True)

    # Conversion des colonnes dans les types souhaités
    keys_filter_players_dataframe[["first_name", "last_name", "nationality", "slug", "role", "image_url", "name"]] = keys_filter_players_dataframe[["first_name", "last_name", "nationality", "slug", "role", "image_url", "name"]].astype(str)
    keys_filter_players_dataframe["age"] = keys_filter_players_dataframe["age"].fillna(999).astype(int)
    keys_filter_players_dataframe[["id", "team_id"]] = keys_filter_players_dataframe[["id", "team_id"]].astype(int)

    # Suppression des lignes dupliquées
    cleaned_dataframe = keys_filter_players_dataframe.drop_duplicates()


    cleaned_dataframe.set_index("id", inplace=True)
    cleaned_dataframe.index = cleaned_dataframe.index.astype(int)

    return cleaned_dataframe


def clean_teams_dataframe(teams_raw_dataframe):
    # On récupère uniquement les colonnes de la base de données finale
    keys_filter = teams_raw_dataframe.filter(["id", "acronym", "image_url", "slug", "name", "location"])

    keys_filter.dropna(inplace=True, subset=["id"])

    # Conversion des colonnes dans les types souhaités
    keys_filter["id"] = keys_filter["id"].astype(int)
    keys_filter[["acronym", "image_url", "slug", "name", "location"]] = keys_filter[["acronym", "image_url", "slug", "name", "location"]].astype(str)

    # Suppression des lignes dupliquées
    cleaned_dataframe = keys_filter.drop_duplicates()

    cleaned_dataframe.set_index("id", inplace=True)
    cleaned_dataframe.index = cleaned_dataframe.index.astype(int)

    return cleaned_dataframe
