from pyspark.sql import SparkSession
from airflow.models.baseoperator import BaseOperator
from datetime import datetime , date

class hello(BaseOperator):
    def __init__(self,ids,**kwargs):
        super().__init__(**kwargs)
        self.ids = ids

    def execute(self,context):
        spark = SparkSession.builder.appName("Get new data").config("spark.some.config.option","some-value").getOrCreate()

        #Read the table from the database
        df = spark.read.format("jdbc").option("url","jdbc:mysql://localhost:3306/airflow")\
            .option("dbtable","humidity")\
            .option("uswr","airflow")\
            .option("password","airflow")\
            .option("driver","com.mysql.jdbc.Driver")\
            .load()
