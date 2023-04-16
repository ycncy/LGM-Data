import pandas as pd

import dataframe_generator as gen
import fetch_api as api
import pickle

if __name__ == '__main__':
    videogames = ["valorant"]

    videogame_df = pd.DataFrame()
    leagues_df = pd.DataFrame()
    series_df = pd.DataFrame()
    tournaments_df = pd.DataFrame()

    # for videogame in videogames:
    #     dataframe, leagues_id = gen.process_videogame(api.fetch_raw_videogame(videogame))
    #
    #     videogame_df = pd.concat([videogame_df, dataframe])
    #
    #     for league in leagues_id:
    #         league_df, series_id = gen.process_league(api.fetch_raw_league(str(league)))
    #
    #         leagues_df = pd.concat([leagues_df, league_df])
    #
    #         for serie in series_id:
    #             serie_df, tournaments_id = gen.process_serie(api.fetch_raw_serie(str(serie)))
    #
    #             series_df = pd.concat([series_df, serie_df])
    #
    #             for tournament in tournaments_id:
    #                 tournament_df, matches_id = gen.process_tournament(api.fetch_raw_tournament(str(tournament)))
    #                 tournaments_df = pd.concat([tournaments_df, tournament_df])

    #CHANGER LA COMPLEXITE (3 for qui se suivent au lieu des for imbriqués)

    # videogame_df.to_pickle('videogame.pickle')
    # leagues_df.to_pickle('leagues.pickle')
    # series_df.to_pickle('series.pickle')
    # tournaments_df.to_pickle('tournaments.pickle')

    print(pd.read_pickle("leagues.pickle"))