from airflow.models.baseoperator import BaseOperator
from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import col,when
from pyspark.sql.types import FloatType,TimestampType,DateType
from sqlalchemy import inspect




class Transform(BaseOperator):
    def __init__(self,tables,**kwargs):
        super().__init__(**kwargs)
        self.tables = tables

    def connection(self,tabla):
        """This methos creates a connection with the  db with pyspark"""

        # Create a SparkSession
        try:
             spark = SparkSession.builder.appName("WeatherProyect").getOrCreate()
             df = spark.read.format("jdbc").option("url","jdbc:mysql://mysql:3306/airflow")\
            .option("dbtable",tabla)\
            .option("user","airflow")\
            .option("password","airflow")\
            .option("driver","com.mysql.jdbc.Driver")\
            .load()
        except Exception as e:
                print(f"Error al conectar a la base de datos: {e}")
        finally:
                spark.stop()  # Stop the Spark session after use
        
        return df
    
    def load(tabla,df):
        """This method is used to load the data in the data warehouse"""
        # Create a connection to the database using pandas
      
        try:
            # Create a SparkSession
            spark = SparkSession.builder.appName("WeatherProyect").getOrCreate()
            df.write.format("jdbc")\
            .option("url","jdbc:mysql://mysql:3306/airflow")\
            .option("driver","com.mysql.cj.jdbc.Driver")\
            .option("dbtable",tabla)\
            .option("user","airflow")\
            .option("password","airflow")\
            .mode("append")\
            .save()

        except Exception as e:
                print(f"Error al conectar a la base de datos: {e}")
        finally:
                spark.stop()  # Stop the Spark session after use



    def execute(self,context):

        """This method is used to transform the data from the db and the save in a datawarehouse"""

        url = "jdbc:mysql://mysql:3306/airflow"
        properties = {
                        "user": "airflow",
                        "password": "airflow",
                        "driver": "com.mysql.cj.jdbc.Driver"
                }
        query = "(SELECT table_name FROM information_schema.tables WHERE table_schema = 'airflow') AS tabla_listado"
        spark = SparkSession.builder.appName("WeatherProyect")\
                                    .config("spark.driver.memory", "1g")\
                                    .config("spark.executor.memory", "1g")\
                                    .config("spark.sql.execution.arrow.pyspark.enabled", "true")\
                                    .config("spark.jars", "/usr/share/java/mysql-connector-java.jar").getOrCreate()
                                    
        
        
        df_tablas = spark.read.jdbc(url=url, table=query, properties=properties)

        print(f"COLUMNAS:{df_tablas.columns}")

        available_tables = [fila["table_name"] for fila in df_tablas.collect()]

        print(f"Available tables: {available_tables}")

       
        #Create diccionary of the tables

        dataframes = {}
        for table in self.tables:
            if table in available_tables:  # Verify if the table exist in the database
                dataframes[table] = self.connection(table)  
            else:
                print(f"Warning: the table {table} does not exist in the database")

        # Change the type of data of the datetime column to datetime
        for key,df in dataframes.items():
            if 'datetime' in df.columns:
                dataframes[key] = df.withColumn('datetime',col('datetime').cast(DateType()))

        #Eliminate the duplicated rows
        for key,df in dataframes.items():
            dataframes[key] = df.dropDuplicates(["datetime"])
            
                    
        #Change Nan values to cero
        for key,df in dataframes.items():
            df = df.fillna(0)  
            dataframes[key] = df
      
      #Save the dataframes in the data warehouse
        try:
            for key,df in dataframes.items():
                key = key+'_WH'
                try:
                    self.load(key,df)
                    print("Conexión y insercion exitosa a la base de datos RDS MySQL")

                
                except Exception as e:
                    print(f"Error al conectar a la base de datos: {e}")
        finally:
                   spark.stop() # Stop the Spark session after use
       