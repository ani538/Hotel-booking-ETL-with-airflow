from airflow import DAG
from datetime import timedelta, datetime
from pathlib import Path
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import sqlite3
import os


# get dag directory path
dag_path = os.getcwd()


# define funcctiion to check csv file exits or not
def check_dataset_exits():
    file_path = f"{dag_path}/airflow/dags/dagInjestionPipeline/raw_data/booking.csv"
    if os.path.isfile(file_path):
        print("File exists!")
    else:
        raise Exception("File does not exist. Stopping the DAG.")

# define function to transform data
def transform_data():

# Reading data 
    booking = pd.read_csv(f"{dag_path}/airflow/dags/dagInjestionPipeline/raw_data/booking.csv", low_memory=False)
    client = pd.read_csv(f"{dag_path}/airflow/dags/dagInjestionPipeline/raw_data/client.csv", low_memory=False)
    hotel = pd.read_csv(f"{dag_path}/airflow/dags/dagInjestionPipeline/raw_data/hotel.csv", low_memory=False)

    # merge booking with client
    data = pd.merge(booking, client, on='client_id')
    data.rename(columns={'name': 'client_name', 'type': 'client_type'}, inplace=True)

    # merge booking, client & hotel
    data = pd.merge(data, hotel, on='hotel_id')
    data.rename(columns={'name': 'hotel_name'}, inplace=True)

    # make date format consistent
    data.booking_date = pd.to_datetime(data.booking_date, infer_datetime_format=True)

    # make all cost in GBP currency
    data.loc[data.currency == 'EUR', ['booking_cost']] = data.booking_cost * 0.8
    data.currency.replace("EUR", "GBP", inplace=True)

    # remove unnecessary columns
    # data = data.drop('address')

    # save processed data
    data.to_csv(f"{dag_path}/airflow/dags/dagInjestionPipeline/processed_data/processed_data.csv",index=False)


def load_data():
    #connect to database
    conn = sqlite3.connect(f"{dag_path}/airflow/airflow.db")
    c = conn.cursor()
    c.execute('''
                CREATE TABLE IF NOT EXISTS booking_record (
                    client_id INTEGER NOT NULL,
                    booking_date TEXT NOT NULL,
                    room_type TEXT(512) NOT NULL,
                    hotel_id INTEGER NOT NULL,
                    booking_cost NUMERIC,
                    currency TEXT,
                    age INTEGER,
                    client_name TEXT(512),
                    client_type TEXT(512),
                    hotel_name TEXT(512),
                    address TEXT(512)
                );
             ''')

    records = pd.read_csv(f"{dag_path}/airflow/dags/dagInjestionPipeline/processed_data/processed_data.csv")
    records.to_sql('booking_record', conn, index=False, if_exists='append')



# initializing the default arguments that we'll pass to our DAG
default_args = {
    'owner': 'aniket',
    'start_date': days_ago(1)
}

ingestion_dag = DAG(
    'booking_ingestion_dag',
    default_args=default_args,
    description='Aggregates booking records for data analysis',
    schedule_interval=timedelta(hours=1),
    catchup=False,
    
)

task_0 = PythonOperator(
    task_id='check_dataset_exits',
    python_callable=check_dataset_exits,
    dag=ingestion_dag,
)

task_1 = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=ingestion_dag,
)

task_2 = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=ingestion_dag,
)


task_0 >> task_1 >> task_2