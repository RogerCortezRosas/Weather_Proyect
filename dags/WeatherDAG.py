from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.common.sql.sensors.sql import SqlSensor 

with DAG(dag_id='Weather_ETL',description='This dag is to extract , transform and load the date from the API of weather',
         schedule='@daily',start_date=datetime(2023,10,1),end_date=datetime(2023,10,31),default_args={"depends_on_past":True} ,max_active_runs=1) as dag:
    
    # Task 1: Extract data from the API

    extract = BashOperator(task_id='Extract_data',bash_command="echo 'Extracting data from the API'")

    check_new_data = SqlSensor(
        task_id = 'check_new_data',
        conn_id= 'mysql_conn',
        sql=" SELECT COUNT(*) FROM humidity WHERE id> (SELECT MAX(id) FROM humidity) -10",
        mode = 'poke',
        timeout = 600,
        poke_interval = 30 )
    
    Transform_spak = 


