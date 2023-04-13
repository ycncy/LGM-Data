import requests
from data.api_config import API_URL, HEADER


def fetch_raw_videogame(videogame):
    url = f"{API_URL}/videogames/{videogame}"

    game_info = requests.get(url, headers=HEADER).json()

    return game_info


def fetch_raw_series(videogame):
    url = f"{API_URL}/videogames/{videogame}/series?sort=&page=1&per_page=50"

    series = requests.get(url, headers=HEADER).json()

    return series


def fetch_raw_matches(tournament):
    url = f"{API_URL}/tournaments/{tournament}"

    matches = requests.get(url, headers=HEADER).json()["matches"]

    return matches


def fetch_raw_games(match):
    url = f"{API_URL}/matches/{match}"

    games = requests.get(url, headers=HEADER).json()["games"]

    return games


def fetch_raw_tournament(tournament):
    url = f"{API_URL}/tournaments/{tournament}"

    raw_tournament = requests.get(url, headers=HEADER).json()

    return raw_tournament


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


print(fetch_raw_series("valorant"))