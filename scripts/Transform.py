#import pymysql
from airflow.models.baseoperator import BaseOperator
from datetime import datetime , date
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import inspect


class Transform(BaseOperator):
    def __init__(self,tables,**kwargs):
        super().__init__(**kwargs)
        self.tables = tables

    def connection(self):
        """This methos creates a connection with the  db with pandas"""
        host = 'localhost'
        port='3307'
        user = 'airflow'
        password = 'airflow'
        db = 'airflow'
        connection_string = f'mysql+pymysql://{user}:{password}@{host}:{port}/{db}'
        engine = create_engine(connection_string)

        return engine




    def execute(self,context):

        """This method is used to transform the data from the db and the save in a datawarehouse"""

        engine = self.connection()

        inspector = inspect(engine)

        # get list of the tables (método actual)
        available_tables = inspector.get_table_names()

        #Create diccionary of the tables

        dataframes = {}
        for table in self.tables:
            if table in available_tables:  # Verify if the table exist in the database
                dataframes[table] = pd.read_sql_table(table, engine) 
            else:
                print(f"Warning: the table {table} does not exist in the database")

        # Change the type of data of the datetime column to datetime
        for key,df in dataframes.items():
            if 'datetime' in df.columns:
                df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')
                df.dropna(subset=['datetime'], inplace=True)  # Eliminate rows with Nat values
                df['datetime'] = df['datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')  # change the format to YYYY-MM-DD
                dataframes[key] = df  # Actualizar el DataFrame en el diccionario

        #Eliminate the duplicated rows
        for key,df in dataframes.items():
            df.drop_duplicates(subset=['datetime'], inplace=True)  # Eliminar duplicados basados en la columna 'datetime'
            dataframes[key] = df
                    
        #Change Nan values to cero
        for key,df in dataframes.items():
            df.fillna(0, inplace=True)  # Cambiar NaN a 0
            dataframes[key] = df
      
      #Save the dataframes in the data warehouse
        for key,df in dataframes.items():
            key = key+'_WH'
            try:
                df.to_sql(key,con=engine,if_exists='append',index=False)
                return print("Conexión y insercion exitosa a la base de datos RDS MySQL")

            
            except Exception as e:
                print(f"Error al conectar a la base de datos: {e}")
            finally:
                if engine:
                    engine.dispose()