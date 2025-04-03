from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime,date
from airflow.providers.http.sensors.http import HttpSensor
import json


with DAG(dag_id='Weather',description='This dag is to extract , transform and load the date from the API of weather',
         schedule='@daily',start_date=datetime(2023,10,1),end_date=datetime(2023,10,31),default_args={"depends_on_past":True} ,max_active_runs=1) as dag:
    
    # Task 1: Extract data from the API

    extract = BashOperator(task_id='Extract_data',bash_command="echo 'Extracting data from the API'")

    extract

