from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook
from datetime import datetime
import pandas as pd
import sqlalchemy

## Transform Step

def load_to_sql(file_path):
    # hook to connect using connectoin id in airflow
    conn = BaseHook.get_connection('postgres_default')  
    #Initialise engine using sqlalchemy
    #psycopg2 is used to establish connection with postgres
    engine = sqlalchemy.create_engine(f"postgresql+psycopg2://{conn.login}:{conn.password}@user-survival-prediction_ed73e8-postgres-1:{conn.port}/{conn.schema}")
    df = pd.read_csv(file_path)
    #converting csv to df and then df to sql (table)
    df.to_sql(name="titanic", con=engine, if_exists="replace", index=False)

# Define the DAG
with DAG(
    dag_id="extract_titanic_data",
    schedule_interval=None, 
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag: 

    #Extract Step

    list_files = GCSListObjectsOperator(
        task_id="list_files",
        bucket="my-bucket-63", 
    )

    download_file = GCSToLocalFilesystemOperator(
        task_id="download_file",
        bucket="my-bucket-63", 
        object_name="Titanic-Dataset.csv", 
        filename="/tmp/Titanic-Dataset.csv", 
    )

    ## Transform and Load
    
    load_data = PythonOperator(
        task_id="load_to_sql",
        python_callable=load_to_sql,
        op_kwargs={"file_path": "/tmp/Titanic-Dataset.csv"}
    )

    list_files >> download_file >> load_data


