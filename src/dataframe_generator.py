import sys
from dataframe_cleaner import *

sys.path.append('../data/')


def process_series(series):
    dataframes = []
    for elements in series:
        first_dataframe = pd.json_normalize(elements)

        cleaned_dataframe = clean_series_dataframe(first_dataframe)

        dataframes.append(cleaned_dataframe)

    series_dataframe = pd.concat(dataframes)

    return series_dataframe


def process_matches(matches):
    dataframes = []
    for elements in matches:
        first_dataframe = pd.json_normalize(elements)

        cleaned_dataframe = clean_matches_dataframe(first_dataframe)

        dataframes.append(cleaned_dataframe)

    matches_dataframe = pd.concat(dataframes)

    return matches_dataframe


def process_tournaments_participants(tournaments):
    first_dataframe = pd.json_normalize(tournaments)

    cleaned_dataframe = clean_tournaments_participants_dataframe(first_dataframe)

    return cleaned_dataframe


from data import fetch_api as api

process_tournaments_participants(api.fetch_raw_tournament_participants("10434"))