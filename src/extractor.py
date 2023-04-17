import requests
from data.api_config import API_URL, HEADER
import pandas as pd


def fetch_raw_videogame(videogame):
    url = f"{API_URL}/videogames/{videogame}"

    game_info = requests.get(url, headers=HEADER).json()

    first_dataframe = pd.json_normalize(game_info)

    if hasattr(pd.DataFrame(first_dataframe.leagues[0]), 'id'):
        id_list = pd.DataFrame(first_dataframe.leagues[0]).id.to_list()
    else:
        id_list = []

    return first_dataframe, id_list


def fetch_raw_league(league):
    url = f"{API_URL}/leagues/{league}"

    league_info = requests.get(url, headers=HEADER).json()

    first_dataframe = pd.json_normalize(league_info)
    first_dataframe["videogame_id"] = league_info["videogame"]["id"]

    if hasattr(pd.DataFrame(first_dataframe.series[0]), 'id'):
        id_list = pd.DataFrame(first_dataframe.series[0]).id.to_list()
    else:
        id_list = []

    return first_dataframe, id_list


def fetch_raw_serie(serie):
    url = f"{API_URL}/series/{serie}"

    serie_info = requests.get(url, headers=HEADER).json()

    first_dataframe = pd.json_normalize(serie_info)

    if hasattr(pd.DataFrame(first_dataframe.tournaments[0]), 'id'):
        id_list = pd.DataFrame(first_dataframe.tournaments[0]).id.to_list()
    else:
        id_list = []

    return first_dataframe, id_list


def fetch_raw_tournament(tournament):
    url = f"{API_URL}/tournaments/{tournament}"

    tournament_info = requests.get(url, headers=HEADER).json()

    first_dataframe = pd.json_normalize(tournament_info)

    if hasattr(pd.DataFrame(first_dataframe.matches[0]), 'id'):
        id_list = pd.DataFrame(first_dataframe.matches[0]).id.to_list()
    else:
        id_list = []

    return first_dataframe, id_list


def fetch_raw_match(match):
    url = f"{API_URL}/matches/{match}"

    match_info = requests.get(url, headers=HEADER).json()

    first_dataframe = pd.json_normalize(match_info)

    if hasattr(pd.DataFrame(first_dataframe.game[0]), 'id'):
        id_list = pd.DataFrame(first_dataframe.game[0]).id.to_list()
    else:
        id_list = []

    return first_dataframe, id_list


def fetch_raw_games(match):
    url = f"{API_URL}/matches/{match}"

    games_info = requests.get(url, headers=HEADER).json()["games"]

    return pd.json_normalize(games_info)


def fetch_raw_team(team):
    url = f"{API_URL}/teams/{team}"

    raw_team = requests.get(url, headers=HEADER).json()

    return pd.json_normalize(raw_team)


def fetch_raw_players_from_team(team):
    url = f"{API_URL}/teams/{team}"

    response = requests.get(url, headers=HEADER).json()
    players_infos = response["players"]
    team_id = response["id"]

    players_df = pd.json_normalize(players_infos)
    players_df["team_id"] = team_id

    return players_df


def fetch_raw_stream_from_match(match):
    url = f"{API_URL}/matches/{match}"

    raw_stream = requests.get(url, headers=HEADER).json()

    first_dataframe = pd.json_normalize(raw_stream["streams_list"])
    first_dataframe["match_id"] = raw_stream["id"]

    return first_dataframe


def fetch_raw_opponent_from_match(match):
    url = f"{API_URL}/matches/{match}"

    raw_opponent = requests.get(url, headers=HEADER).json()["opponents"]

    return pd.json_normalize(raw_opponent)