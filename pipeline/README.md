# Répertoire PIPELINE

## Fonctionnement de la pipeline

Le processus de la pipeline consiste à récupérer les données à partir de l'API PandaScore, les nettoyer et les
envoyer à une base de données MySQL.

Nous extrayons les données relatives aux matchs de jeux vidéo, tels que les
résultats, les équipes, les tournois et les matchs en cours et à venir.

Ensuite, nous appliquons des opérations de nettoyage pour
assurer la qualité des données, en supprimant les valeurs manquantes, en normalisant les formats et en gérant les
doublons.

Une fois les données nettoyées, nous les envoyons à notre base de données MySQL pour les stocker et les rendre
accessibles pour des analyses et des utilisations ultérieures (Affichage sur l'application WEB et Mobile, Machine
Learning).

## Structure du répertoire

Le répertoire se divise en trois répertoire représentant une structure "extract-transform-load" (ETL). Ainsi chaque
répertoire représente une étape de la pipeline.

- Le répertoire [`extract`](extract) : Contient une classe Mère `DataExtractor` qui permet de récupérer les données de
  l'API, cette classe contient un constructeur et une méthode permettant de compter les appels API effectuer durant le
  processus et changer de clé API une fois la limite de la clé courante atteinte. Cette classe est étendue par 3 autres
  classes permettant ainsi de récupérer les données de différentes façons :
    <br><br>
    (Toutes les méthodes de ces classes renvoient un dataframe Pandas pour manipuler plus facilement les données)
    <br><br>
    - [Sans plage de date](extract/extract_data_without_date_range.py) : C'est la manière la plus simple, on récupère
      toutes les informations données par les appels API sans se soucier des dates. **Cette classe a notamment servi à
      l'initialisation de la base de données.**
    - [Avec plage de date](extract/extract_data_with_date_range.py) : Cette classe permet de collecter les données de
      l'API mais cette fois-ci en donnant un intervalle de date, par exemple on récupère les tournois qui commence (
      attribut : `begin_at`) entre deux dates données.
    - [Appels API asynchrone](extract/asynchronus_extraction_with_date_range.py) : Cette classe permet aussi de faire
      des appels API en donnant un intervalle de date, cependant elle permet une exécution beaucoup plus rapide car elle
      utilise les requêtes asynchrones grâce à la librairie `aiohttp`
  
<br>

- Le répertoire [`transform`](transform) : Contient un seul [script python](transform/clean_and_transform_dataframe.py) composé de plusieurs permettant de "nettoyer" les dataframes générés par les méthodes d'extraction de données. Le nettoyage se décompose en plusieurs parties :
    
  - **Filtrage des colonnes** : On applique les méthodes `filter()` ou `drop()` sur le dataframe "brut" pour garder uniquement les colonnes nécéssaires.
  - **Suppression des lignes inexploitables**: On supprime toutes les lignes où certaines colonnes importantes (ou toutes les colonnes) contiennent des valeurs nulle, par exemple un `id` ou `name`.
  - **Convertion des types** : On force le type de certaines colonnes, par exemple on convertit les dates au type `datetime[ns64]` de Pandas car l'API nous renvoie des dates qui sont en réalité des chaînes de caractères.
  - **Suppression des doublons**: Grâce à la méthode `drop_duplicates()`.
  
<br><br>

- Le répertoire [`load`](load) : Contient la classe [`MySQLDataManager`](load/mysql_data_manager.py) qui contient toutes les méthodes nécéssaires à l'envoie des données nettoyées dans la base de données grâce à la librairie `mysql-connector`.