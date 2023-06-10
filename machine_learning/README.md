# Répertoire Machine Learning

Ce répertoire contient les fichiers et les scripts nécessaires à l'implémentation et à l'évaluation des modèles de machine learning pour calculer les cotes des futurs matchs.

## Structure du répertoire

Le répertoire est organisé de la manière suivante :

- Le répertoire `evaluate` : Contient les fichiers permettant d'évaluer les performances des modèles de machine learning.

  - `model_evaluation.py` : Ce fichier contient les fonctions pour évaluer les performances des modèles en utilisant des métriques telles que l'exactitude, la précision, le rappel et la courbe ROC.

- Le répertoire `models` : Contient les différents modèles de machine learning utilisés pour calculer les cotes des futurs matchs.

  - Un sous-répertoire par modèle contenant les fichiers et les classes relatifs à chaque modèle implémenté.

- Le répertoire `pre_process` : Contient les fichiers pour le prétraitement des données avant l'entraînement des modèles.

  - `collect_data.py` : Ce fichier contient les fonctions pour collecter les données nécessaires à l'entraînement des modèles.

  - `data_processing.py` : Ce fichier contient les fonctions pour traiter et préparer les données avant l'entraînement des modèles.

- Le répertoire `utils` : Contient des utilitaires et des scripts de support pour le processus de machine learning.

  - `grid_search.py` : Ce fichier contient les fonctions pour effectuer une recherche par grille afin de trouver les meilleurs hyperparamètres pour les modèles de machine learning.

- Le fichier `README.md` : Ce fichier contient des informations sur l'utilisation et la structure du répertoire Machine Learning.

- Le répertoire `scripts/machine_learning` à la racine du projet : Contient les scripts principaux du projet pour le processus de machine learning.

  - Un sous-répertoire par model implémenté : Contient les fichiers spécifiques au modèle de boosting par .

    - `find_best_params_NOM_DU_MODEL.py` : Ce fichier contient le code pour effectuer une recherche par grille afin de trouver les meilleurs hyperparamètres pour le modèle.

    - `train_and_save_NOM_DU_MODEL.py` : Ce fichier contient le code pour entraîner le modèle de boosting par gradient avec les hyperparamètres optimaux et le sauvegarder pour une utilisation ultérieure.

Cette structure de répertoire permet d'organiser les différentes parties du processus de machine learning, du prétraitement des données à l'entraînement et à l'évaluation des modèles. Vous pouvez ajouter d'autres fichiers ou répertoires selon vos besoins spécifiques pour votre projet de calcul de cotes pour les futurs matchs.
