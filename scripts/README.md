# Répertoire Scripts

## Structure

Le répertoire est divisé en plusieurs sous-répertoires contenant chacun les scripts pour chaque partie du projet, actuellement il y a deux sous-répertoires :

- [machine_learning](./machine_learning) : Ce répertoire contient des sous répertoires pour chaque modèle comme expliqué dans le [README de la partie sur le machine learning](../machine_learning/README.md).
- [update_databse](./update_database) : Contient un seul [script](update_database/add_data_until_last_record.py) qui permet de récupérer les nouvelles données de l'API à
  partir de la date de la dernière mise à jour, ce script est fait pour se lancer toutes les heures et ajouter les
  nouvelles ou mettre à jour les données de la base de données.
