from dataframe_cleaner import *
import pandas as pd
from src import fetch_api as api


def process_videogame(videogame):
    first_dataframe = pd.json_normalize(videogame)

    if hasattr(pd.DataFrame(first_dataframe.leagues[0]), 'id'):
        id_list = pd.DataFrame(first_dataframe.leagues[0]).id.to_list()
    else:
        id_list = []

    cleaned_dataframe = clean_videogame_dataframe(first_dataframe)

    return cleaned_dataframe, id_list


def process_league(league):
    first_dataframe = pd.json_normalize(league[0])
    first_dataframe["videogame_id"] = league[1]

    if hasattr(pd.DataFrame(first_dataframe.series[0]), 'id'):
        id_list = pd.DataFrame(first_dataframe.series[0]).id.to_list()
    else:
        id_list = []

    cleaned_dataframe = clean_league_dataframe(first_dataframe)

    return cleaned_dataframe, id_list


def process_serie(serie):
    first_dataframe = pd.json_normalize(serie)

    if hasattr(pd.DataFrame(first_dataframe.tournaments[0]), 'id'):
        id_list = pd.DataFrame(first_dataframe.tournaments[0]).id.to_list()
    else:
        id_list = []

    cleaned_dataframe = clean_serie_dataframe(first_dataframe)

    return cleaned_dataframe, id_list


def process_tournament(tournament):
    first_dataframe = pd.json_normalize(tournament)

    if hasattr(pd.DataFrame(first_dataframe.matches[0]), 'id'):
        id_list = pd.DataFrame(first_dataframe.matches[0]).id.to_list()
    else:
        id_list = []

    cleaned_dataframe = clean_tournament_dataframe(first_dataframe)

    return cleaned_dataframe, id_list


def process_match(match):
    first_dataframe = pd.json_normalize(match)

    if hasattr(pd.DataFrame(first_dataframe.match[0]), 'id'):
        id_list = pd.DataFrame(first_dataframe.match[0]).id.to_list()
    else:
        id_list = []

    cleaned_dataframe = clean_serie_dataframe(first_dataframe)

    return cleaned_dataframe, id_list


def process_game(game):
    first_dataframe = pd.json_normalize(game)

    cleaned_dataframe = clean_game_dataframe(first_dataframe)

    return cleaned_dataframe


def process_tournament_participants(tournament):
    first_dataframe = pd.json_normalize(tournament)

    cleaned_dataframe = clean_tournaments_participants_dataframe(first_dataframe)

    return cleaned_dataframe


def process_players(team):
    players_first_dataframe = pd.json_normalize(team[1])
    team_id = team[0]

    cleaned_dataframe = clean_players_dataframe((team_id, players_first_dataframe))

    return cleaned_dataframe


def process_team(team):
    first_dataframe = pd.json_normalize(team)

    cleaned_dataframe = clean_teams_dataframe(first_dataframe)

    return cleaned_dataframe


def process_stream(match):
    first_dataframe = pd.json_normalize(match)

    cleaned_dataframe = clean_stream_dataframe(first_dataframe)

    return cleaned_dataframe