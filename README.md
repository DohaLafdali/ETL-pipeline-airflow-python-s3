# Projet ETL avec Apache Airflow

Ce projet met en œuvre un pipeline ETL (Extract, Transform, Load) en utilisant Apache Airflow pour l'orchestration des tâches, Pandas pour la transformation des données, et AWS S3 pour le stockage.

## Étapes réalisées

1. **Extraction des données** :
   - Extraction des données météorologiques à partir de l'API Open Meteo.
   - voila notre api : https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&hourly=temperature_2m
2. **Transformation des données** :
   - Transformation des données au format JSON en un DataFrame lisible en utilisant Pandas.
   - Expansion des colonnes 'hourly.time' et 'hourly.temperature_2m' pour une meilleure analyse.
3. **Chargement des données** :
   - Chargement des données transformées dans AWS S3 en utilisant AWS Wrangler.
<img width="521" alt="image" src="https://github.com/DohaLafdali/ETL-pipeline-airflow-python-s3/assets/72882387/5f5edd84-b8c2-4b0d-905c-fcc9df736f93"><br>
4. **Mise en conteneurs avec Docker** :
   - Configuration et exécution d'Apache Airflow dans un environnement Docker pour garantir la portabilité et la facilité de déploiement.

5. **Exécution du DAG** :
   - Orchestration du pipeline ETL en définissant un DAG dans Apache Airflow.
     <img width="448" alt="image" src="https://github.com/DohaLafdali/ETL-pipeline-airflow-python-s3/assets/72882387/09fe3ad4-e7bd-4372-8e3c-6cef6617ecd2"><br>
     <img width="694" alt="image" src="https://github.com/DohaLafdali/ETL-pipeline-airflow-python-s3/assets/72882387/5689ba1b-4ce0-4911-9e66-48e1f78a2ec5">
   - Exécution du DAG pour automatiser le processus ETL.
 <img width="952" alt="image" src="https://github.com/DohaLafdali/ETL-pipeline-airflow-python-s3/assets/72882387/d28773fc-b188-4977-a8ce-1b280ffca055"><br>
## Comment exécuter le projet

1. Assurez-vous d'avoir Docker installé.
2. Clônez ce référentiel.
3. Créez un fichier `.env` à la racine du projet avec les variables d'environnement nécessaires.
4. Exécutez `docker-compose up --build` pour démarrer le projet.

## Structure du projet

- **dags/** : Contient les fichiers de définition des DAGs pour Apache Airflow.
- **logs/** : Emplacement où les journaux d'Apache Airflow seront stockés.
- **plugins/** : Répertoire pour les plugins personnalisés d'Apache Airflow.
- **config/** : Emplacement pour les fichiers de configuration.

## Auteur

[Lafdali doha.]

