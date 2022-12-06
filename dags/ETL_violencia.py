# Utilidades de airflow
from airflow.models import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup

# Utilidades de python
from datetime  import datetime

# Funciones ETL
from utils.crear_tablas import crear_tablas_violencia
from utils.file_util import procesar_datos
from utils.insert_queries import *

default_args= {
    'owner': 'Estudiante',
    'email_on_failure': False,
    'email': ['estudiante@uniandes.edu.co'],
    'start_date': datetime(2022, 5, 5) # inicio de ejecución
}

with DAG(
    "ETL_violencia",
    description='ETL violencia',
    schedule_interval='@daily', # ejecución diaría del DAG
    default_args=default_args, 
    catchup=False) as dag:

    # task: 1 - preprocesamiento
    preprocesamiento = PythonOperator(
        task_id='preprocesamiento',
        python_callable=procesar_datos
    )

    # task: 2 crear las tablas en la base de datos postgres
    crear_tablas_db = PostgresOperator(
    task_id="crear_tablas_en_postgres",
    postgres_conn_id="postgres_localhost", # Nótese que es el mismo ID definido en la conexión Postgres de la interfaz de Airflow
    sql= crear_tablas_violencia()
    )

    # task: 3 poblar las tablas de dimensiones en la base de datos
    with TaskGroup('poblar_tablas') as poblar_tablas_dimensiones:

        # task: 3.1 poblar tabla mineria
        poblar_city = PostgresOperator(
            task_id="poblar_mineria",
            postgres_conn_id='postgres_localhost',
            sql=insert_query_mineria_violencia(csv_path = "mineria")
        )

        # task: 3.2 poblar tabla educacion
        poblar_customer = PostgresOperator(
            task_id="poblar_educacion",
            postgres_conn_id='postgres_localhost',
            sql=insert_query_educacion_violencia(csv_path ="educacion")
        )

        # task: 3.3 poblar tabla demografia
        poblar_date = PostgresOperator(
            task_id="poblar_demografia",
            postgres_conn_id='postgres_localhost',
            sql=insert_query_demografia_violencia(csv_path = "poblacion")
        )

        # task: 3.4 poblar tabla violencia
        poblar_employee = PostgresOperator(
            task_id="poblar_violencia",
            postgres_conn_id='postgres_localhost',
            sql=insert_query_violencia_violencia(csv_path = "violencia")
        )

    # task: 4 poblar la tabla de hechos
    poblar_fact_order = PostgresOperator(
            task_id="construir_tabla_de_hechos",
            postgres_conn_id='postgres_localhost',
            sql=insert_query_municipio_violencia(csv_path = "municipio")
    )

    # flujo de ejecución de las tareas  
    preprocesamiento >> crear_tablas_db >> poblar_tablas_dimensiones >> poblar_fact_order