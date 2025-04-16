# Weather_Proyect

![imagen](data/clima-tipos.jpg)

## Descripcion 
Este repositorio contiene un proyecto de extracción y análisis de datos meteorológicos históricos provenientes de una base de datos climática de EE. UU. El objetivo es explorar tendencias climáticas, detectar anomalías y visualizar patrones relevantes que puedan apoyar investigaciones o aplicaciones en campos como agricultura, energía, planificación urbana y cambio climático.

## Que contiene este proyecto?

- **🖥️Extraccion de Datos:**

Conectamos a una base de datos MySql que es nuestro data Lake con los datos que aun no estan procesados mediante dos tareas .
Un a de ellas se conecta con python y la otra con pyspark estas se ejecutan solo si han ingresado nuevos registros a la base de datos.
- **📚⛏️Transformacion:**

Una vez que se ejecuta la accion de conectar a la base de datos se ejecuta la accion de transformacion .Las tablas que contienen la informacion de presion ,direccion de viento,descripcion de clima seran procesados con pyspark , mientras que las tablas con la informacion de temperatura,humedad y velocidad de viento sera procesados con python .
- **📲Carga:**

Cuando el proceso de transfoemacion de limpieza y transformacion haya acabado , los datos se suben a un data warehouse donde se consultara
para el analisis respectivo.
- **🔬Analisis exploratorio:**

Se realiza un analisis exploratorio de los datos para tener la visualizacion de patrones y tendencias del clima en diferentes regiones de estados unidos.
- **📈Visualización de Datos:**

Con Matplotlib, Seaborn y Plotly, generamos gráficos interactivos y mapas de calor que ayudan a comprender la evolución del clima en diferentes partes del país.

 ## Tecnologias  ⚒️

 - **Airflow:**

 Se crea un pipeline automatizado que se ejecuta cuando nuevos registros son insertados en un data lake , posteriormente , se transforman y se cargan en un data warehouse ,por ultimo, si todo se cargo exitosamente se eliminan los resgistro del data lake.
<p align="center">
  <img src="data/airflowLogoM.png" alt="Temperatura" width="35%" height="300px" />
  <img src="data/airflow.png" alt="Precipitación" width="55%" height="300px"/>
</p>

- **Docker:**

Se crea dos contenedores (airflow,mysql) con docker compose donde se alojaran las librerias necesarias para el proyecto

<p align="center">
  <img src="data/dockerLogo.png" alt="Temperatura" width="35%" height="300px" />
  <img src="data/docker.png" alt="Precipitación" width="55%" height="300px"/>
</p>

- **MySql:**

Se crea una base de datos donde se alojara nuestro Data Lake y nuestro Data Warehouse
<p align="center">
  <img src="data/MySqlLogo.png" alt="Temperatura" width="35%" height="300px" />
  <img src="data/MySql.png" alt="Precipitación" width="55%" height="300px"/>
</p>

- **Spark:**

Se utiliza pySpark para la transformacion de los datos del Data Lake
<p align="center">
  <img src="data/SparkLogo.png" alt="Temperatura" width="35%" height="300px" />
  <img src="data/spark.png" alt="Precipitación" width="55%" height="300px"/>
</p>

- **Python:**

Se utiliza pandas para la transformacion de los datos del Data Lake
<p align="center">
  <img src="data/PythonLogo.png" alt="Temperatura" width="35%" height="300px" />
  <img src="data/python.png" alt="Precipitación" width="55%" height="300px"/>
</p>

- **Plotly:**

Se utiliza plotly para para el analisis exploratorio
<p align="center">
  <img src="data/PlotlyLogo.png" alt="Temperatura" width="35%" height="300px" />
  <img src="data/eda.png" alt="Precipitación" width="55%" height="300px"/>
</p>

- **Power BI:**

Se utiliza Power BI para para la visualizacion de los datos
<p align="center">
  <img src="data/PoweLogo.png" alt="Temperatura" width="35%" height="300px" />
  <img src="data/python.png" alt="Precipitación" width="55%" height="300px"/>
</p>