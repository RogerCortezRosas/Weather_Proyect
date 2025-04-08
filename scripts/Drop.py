from airflow.models.baseoperator import BaseOperator
from datetime import datetime , date
import pandas as pd
from sqlalchemy import create_engine,text
from sqlalchemy import inspect



class Drop(BaseOperator):
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

        for table in self.tables:
            if table in available_tables:  # Verify if the table exist in the database
                with engine.connect() as connection:
                    connection.execute(text (f"DELETE FROM {table}"))  
            else:
                print(f"Warning: the table {table} does not exist in the database")