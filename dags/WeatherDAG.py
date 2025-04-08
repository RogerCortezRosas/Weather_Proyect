from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.common.sql.sensors.sql import SqlSensor 
import sys
import os

from pathlib import Path

# Añade el directorio Proyecto/ al path de Python
sys.path.append(str(Path(__file__).parent.parent))

from scripts.Transform import Transform
from scripts.TransformPyspark import Transform as TransformPyspark
from scripts.Drop import Drop


with DAG(dag_id='Weather_ETL',description='This dag is to extract , transform and load the date from the API of weather',
         schedule='@daily',start_date=datetime(2024,4,7),end_date=datetime(2024,4,20),default_args={"depends_on_past":False} ,max_active_runs=1) as dag:
    
    # Task 1: Initial task

    extract = BashOperator(task_id='Extract_data',bash_command="sleep 5 && echo 'DAG running'",depends_on_past=False)

    #Task 2 :Check new data in the datalake
    check_new_data = SqlSensor(
        task_id = 'check_new_data',
        conn_id= 'mysql_conn',
        sql=" SELECT COUNT(*) FROM humidity WHERE id> (SELECT MAX(id) FROM humidity) -10",
        mode = 'poke',
        timeout = 600,
        poke_interval = 30 )
    #Task3: Get data from the datalake and transform it with pandas an then load it in the data warehouse
    Transform_pandas = Transform(task_id = 'Transform_data',tables=['temperature','humidity','wind_speed'],depends_on_past=True)

    #Task4: Get data from the data base and transform it with pyspark and then load it in the data warehouse
    #Transform_pyspark = TransformPyspark(task_id = 'Transform_data_pyspark',tables=['pressure','weather_description','wind_direction'],depend_on_past=True)

    #Task5 : Eliminate data from the datalake
    Eliminate_data = Drop(task_id = 'Eliminate_data',tables=['temperature','humidity','wind_speed','pressure','weather_description','wind_direction'],depends_on_past=True)


    extract >> check_new_data >> Transform_pandas >> Eliminate_data





