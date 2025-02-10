from airflow import DAG
import pendulum
from datetime import timedelta
from airflow.operators.python import PythonOperator  # Updated import path for Airflow 2.x

from process_dag.ingest_ccse_covid_daily_reports import *

# Define the start date in Asia/Manila timezone
start_date_manila = pendulum.datetime(2021, 1, 1, tz="Asia/Manila")

# Define default arguments
default_args = {
    'owner': 'Jayson',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id="ccse_covid_daily_report",
    default_args=default_args,
    tags=["exam"],
    start_date=start_date_manila,
    schedule_interval=None  # Set schedule_interval to None for manual triggering
) as dag:
    
    # Define your tasks/operators within the DAG context
    ingest_ccse_covid_daily_report_task = PythonOperator(
        task_id="ingest_ccse_covid_daily_report",
        python_callable=ingest_ccse_covid_daily_report,
    )

ingest_ccse_covid_daily_report_task
