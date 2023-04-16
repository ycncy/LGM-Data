import requests
from data.api_config import API_URL, HEADER


def fetch_raw_videogame(videogame):
    url = f"{API_URL}/videogames/{videogame}"

    game_info = requests.get(url, headers=HEADER).json()

    return game_info


def fetch_raw_league(league):
    url = f"{API_URL}/leagues/{league}"

    game_info = requests.get(url, headers=HEADER).json()

    return game_info, game_info["videogame"]["id"]


def fetch_raw_serie(serie):
    url = f"{API_URL}/series/{serie}"

    series = requests.get(url, headers=HEADER).json()

    return series


def fetch_raw_tournament(tournament):
    url = f"{API_URL}/tournaments/{tournament}"

    raw_tournament = requests.get(url, headers=HEADER).json()

    return raw_tournament


def fetch_raw_match(match):
    url = f"{API_URL}/tournaments/{match}"

    match = requests.get(url, headers=HEADER).json()

    return match


def fetch_raw_game(game):
    url = f"{API_URL}/matches/{game}"

    games = requests.get(url, headers=HEADER).json()["games"]

    return games


def fetch_raw_players_from_team(team):
    url = f"{API_URL}/teams/{team}"

    team = requests.get(url, headers=HEADER).json()

    players = team["players"]
    team_id = team["id"]

    return team_id, players


def fetch_raw_team(team):
    url = f"{API_URL}/teams/{team}"

    raw_team = requests.get(url, headers=HEADER).json()

    return raw_team

#print(fetch_raw_game("5450"))
#league vcl 4947
#serie spain 5450
#match 582010
#team 124412