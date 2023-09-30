from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
import pandas as pd
from datetime import timedelta
import json 
import requests
import boto3
from dotenv import load_dotenv
import os

# Charger les variables d'environnement du fichier .env
load_dotenv()

# Définir les paramètres de la DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Définir la DAG
dag = DAG('etl_pipeline', 
          description='ETL Pipeline DAG',
          schedule_interval='@daily',
          start_date=datetime(2023, 9, 27),
          catchup=False,
          default_args=default_args)

# Fonction d'extraction des données
def extract():
    data = requests.get('https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&hourly=temperature_2m')
    text = data.text
    dictionary = json.loads(text)
    return dictionary

def transform(raw_data: dict):
    # Convertit le dictionnaire en un DataFrame en utilisant la fonction json_normalize de pandas
    df=pd.json_normalize(raw_data)
    # Explose les listes présentes dans les colonnes 'hourly.time' et 'hourly.temperature_2m'
    # Cela signifie que si une cellule contient une liste, elle sera "décomposée" en plusieurs lignes
    # Chaque élément de la liste devient une nouvelle ligne dans le DataFrame
    df=df.explode(['hourly.time','hourly.temperature_2m'])

    return df

# Fonction de chargement des données
def load(transformed_data: pd.DataFrame):
    import awswrangler as wr
    #Chargement des variables d'environnement du fichier .env
    load_dotenv()
    
    #Obtention de la date et de l'heure actuelles
    now=datetime.now()
    year=now.strftime("%Y")
    month = now.strftime("%m")
    day =now.strftime("%d")
    time = now.strftime("%H:%M:%S")

    #crée une session Boto3 en utilisant les clés d'accès AWS et la région spécifiées.
    session=boto3.Session(
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name="us-east-2"
    )

    #On utilise la fonction to_parquet de awswrangler (wr.s3.to_parquet) 
    #pour charger le DataFrame transformed_data dans un bucket S3
    wr.s3.to_parquet(
        df=transformed_data,
        #Le chemin S3 où les données seront chargées est construit en fonction de l'année, du mois, du jour et de l'heure actuels.
        path='s3://etlairflowpipeline/open_meteo' + year + '/'+month +'/' +day +'/'+time + '.parquet',
        boto3_session=session,
    )

    wr.s3.to_parquet(
        df=transformed_data,
        path=f's3://etlairflowpipeline/open_meteo/{year}/{month}/{day}/{time}',
        boto3_session=session
    )
    
# Tâche import_task
import_task = BashOperator(
    task_id='import_task',
    bash_command='pip install awswrangler',
    dag=dag
)
# Tâche d'extraction
extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=extract,
    dag=dag
)

# Tâche de transformation
transform_task = PythonOperator(
    task_id='transform_task',
    python_callable=transform,
    op_args=[extract_task.output],  # Passer la sortie de la tâche d'extraction comme argument
    provide_context=True,
    dag=dag
)

# Tâche de chargement
load_task = PythonOperator(
    task_id='load_task',
    python_callable=load,
    op_args=[transform_task.output],  # Passer la sortie de la tâche de transformation comme argument
    provide_context=True,
    dag=dag
)

# Ordonner les tâches dans la DAG
import_task >> extract_task >> transform_task >> load_task
