from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from datetime import datetime
from sqlalchemy import create_engine
import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account
import warnings
warnings.filterwarnings('ignore')

credential = service_account.Credentials.from_service_account_file('./dags/Keys_GCP/keys.json')

Postgres_Connection = "postgresql://ajied:postgres@postgres:5432/airflow"

def postgres():
    
    engine = create_engine(Postgres_Connection)
    sql = "SELECT * FROM public.socio_economic_of_indonesia"
    df = pd.read_sql_query(sql, engine)

    return df 

def upload_to_gcs(buket_name:str, files:str):

    client = storage.Client(credentials=credential)
    bucket = client.get_bucket(buket_name)
    
    blob = bucket.blob(f"{files}")
    blob.upload_from_filename(files)    


with DAG(
    dag_id="user_role_final_project_zoomcamp",
    default_args={
        "owner": "Ajied",
    },
    start_date= datetime(2024, 4, 1),
    schedule= "@daily",
    catchup= False,
    description= "Proses schedule job from Postgresql to GCS"
) as dag :

    start = PythonOperator(
        task_id= "start",
        python_callable= lambda: print("Started Job")
    )

    run_query_sql = PythonOperator(
        task_id="connect_to_database_postgres_and_show_data",
        python_callable=postgres,
    )

    schedule_job_postgresql_to_gcs = PostgresToGCSOperator(
        task_id="postgresql_to_gcs",
        sql="SELECT * FROM public.socio_economic_of_indonesia",
        postgres_conn_id="Postgres_docker_connect",
        bucket="final-project-zoomcamp",
        filename="socio_economic_of_indonesia.parquet",
        export_format="parquet",
        gcp_conn_id="GCS_connection",  
    )

    end = PythonOperator(
        task_id= "end",
        python_callable= lambda: print("Finished Job")
    )


    start >> run_query_sql >> schedule_job_postgresql_to_gcs >> end