# Répertoire Scripts

## Structure

C'est un répertoire simple qui contient trois scripts :

- [Générer des dataframes](generate_pickle_from_dataframe_first_time.py) : Ce script permet de créer les dataframes
  initiaux sans intervalle de date grâce à la
  classe [`ClassicDataExtractor`](../pipeline/extract/extract_data_without_date_range.py) et les stocker sous le
  format `Pickle`.
- [Envoyer les dataframes](load_pickle_to_mysql_server.py) : Ce script permet de parcourir le répertoire contenant les
  dataframes en format `Pickle` et envoyer chaque dataframe dans la base de données, ce script utilise la
  méthode `add_dataframe_to_database` créer les tables et d'y insérer les données du dataframe.
- [Script principal](add_data_until_last_record.py) : Ce script permet de récupérer les nouvelles données de l'API à
  partir de la date de la dernière mise à jour, ce script est fait pour se lancer toutes les heures et ajouter les
  nouvelles ou mettre à jour les données de la base de données.