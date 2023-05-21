# Répertoire des opérations SQL

Ce répertoire contient une seule classe [`Referencer`](create_table_references.py) qui contient une méthode permettant d'initialiser les clés primaires et les clés
étrangères des nouvelles tables. 

Cette méthode prend en argument un objet contenant 3 attributs :

- `table_name` : Le nom de la table.
- `table_primary_keys` : Une liste des clés primaires.
- `table_foreign_keys` : Une liste d'objet décrivant les clés primaires avec trois attributs :
  - `column_name`: Le nom de colonne qui correspond à la clé étrangère.
  - `reference_table_name`: Le nom de la table à laquelle on fait référence.
  - `reference_column_name`: Le nom de la colonne à laquelle on fait référence.