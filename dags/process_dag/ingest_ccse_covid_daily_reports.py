import pandas as pd
import os
from pathlib import Path
from database_connections.postgres_connection import *
from datetime import datetime, timedelta
import logging
from sqlalchemy.sql import text

# Function to rename columns based on a predefined mapping
def rename_columns(df):
    columns_to_rename = {
        'FIPS': 'fips',
        'Admin2': 'admin_two',
        'Confirmed': 'confirmed',
        'Deaths': 'deaths',
        'Recovered': 'recovered',
        'Active': 'active',
        'Combined_Key': 'combined_key',
        'Case_Fatality_Ratio': 'case_fatality_ratio',
        'Incidence_Rate': 'incident_rate',
        'Incident_Rate': 'incident_rate',
        'Province_State': 'province_state',
        'Province/State': 'province_state',
        'Country_Region': 'country_region',
        'Country/Region': 'country_region',
        'Last_Update': 'last_update',
        'Last Update': 'last_update',
        'Lat': 'latitude',
        'Latitude': 'latitude',
        'Long_': 'longitude',
        'Longitude': 'longitude'
    }
    # Rename columns using the defined mapping
    df = df.rename(columns=columns_to_rename)
    
    # Replace NaN values with None across all columns
    df = df.apply(lambda x: x.where(pd.notna(x), None))
    
    return df

# Function to create a partition for the specified report date
def create_partition(engine, report_date):
    partition_name = f"covid_daily_reports_{report_date.year}"
    partition_query = f"""
    CREATE TABLE IF NOT EXISTS {partition_name} PARTITION OF covid_daily_reports
    FOR VALUES FROM ('{report_date.year}-01-01') TO ('{report_date.year + 1}-01-01');
    """
    with engine.connect() as connection:
        connection.execute(text(partition_query))

# Function to create partitions for a list of report dates
def create_partitions_for_dates(engine, report_dates):
    for report_date in report_dates:
        create_partition(engine, report_date)

# Function to batch insert data into the specified table
def batch_insert(engine, df, table_name, batch_size=1000):
    connection = engine.raw_connection()
    cursor = connection.cursor()
    df_columns = list(df.columns)
    columns = ",".join(df_columns)
    values = "VALUES({})".format(",".join(["%s" for _ in df_columns]))
    insert_stmt = f"INSERT INTO {table_name} ({columns}) {values}"

    try:
        for start in range(0, len(df), batch_size):
            batch = df.iloc[start:start+batch_size]
            
            # Convert float columns to string with 2 decimal places
            for col in batch.select_dtypes(include=['float']).columns:
                batch.loc[:, col] = batch[col].map(lambda x: f"{x:.2f}" if not pd.isnull(x) else None)

            batch = batch.where(pd.notna(batch), None).values.tolist()
            cursor.executemany(insert_stmt, batch)
            connection.commit()
    except Exception as e:
        logging.error(f"Error during batch insert: {str(e)}")
        connection.rollback()
    finally:
        cursor.close()
        connection.close()


# Function to delete existing data for a specific report date range
def delete_existing_data(engine, start_date, end_date):
    try:
        with engine.connect() as connection:
            while start_date <= end_date:
                partition_name = f"covid_daily_reports_{start_date.year}"
                delete_query = f"""
                DELETE FROM {partition_name} 
                WHERE report_date = '{start_date}' 
                AND EXISTS (SELECT 1 FROM {partition_name} WHERE report_date = '{start_date}')
                """
                connection.execute(text(delete_query))
                start_date += timedelta(days=1)
    except Exception as e:
        logging.error(f"Error deleting data: {str(e)}")

# Main function to ingest COVID-19 daily reports
def ingest_ccse_covid_daily_report():
    csv_directory_reports = Path("/opt/airflow/data/csse_covid_19_daily_reports/")
    csv_directory = Path("/opt/airflow/data/")
    engine = get_postgres_engine()
    
    try:
        # Read run_date.txt to get the date range
        run_date_file = csv_directory / "run_date.txt"
        if not os.path.exists(run_date_file):
            logging.error("run_date.txt does not exist.")
            return
        
        with open(run_date_file, 'r') as f:
            run_dates = f.readlines()
        
        # Parse start_date and end_date from run_date.txt
        start_date_str = run_dates[0].strip().split(':')[1].strip()
        end_date_str = run_dates[1].strip().split(':')[1].strip()
        
        # Check if start_date or end_date are blank
        if start_date_str == "" or end_date_str == "":
            current_date = datetime.now().date()
            start_date = current_date
            end_date = current_date
            logging.info(f"Using current date: {current_date}")
        else:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
            logging.info(f"Using date range from run_date.txt: {start_date} to {end_date}")
        
        # Loop through each date within the range
        current_date = start_date
        while current_date <= end_date:
            formatted_date_str = current_date.strftime('%m-%d-%Y')
            csv_file_to_process = csv_directory_reports / f"{formatted_date_str}.csv"
            
            if not csv_file_to_process.exists():
                logging.warning(f"No CSV file found for date {current_date}")
                
                # Attempt to find a matching file with the current_date format in the directory
                matching_files = list(csv_directory_reports.glob(f"*{formatted_date_str}.csv"))
                if matching_files:
                    csv_file_to_process = matching_files[0]
                    logging.info(f"Found matching file: {csv_file_to_process}")
                else:
                    logging.warning(f"No matching file found for date {current_date}")
                    current_date += timedelta(days=1)
                    continue
            
            try:
                # Read CSV file into DataFrame
                df = pd.read_csv(csv_file_to_process)
                df = rename_columns(df)
                
                # Extract report date from filename
                report_date = datetime.strptime(formatted_date_str, '%m-%d-%Y').date()
                df['report_date'] = report_date
                
                # Add 'etl_insertion_date' column with current timestamp
                df['etl_insertion_date'] = datetime.now()
                
                # Delete existing data for the report date
                delete_existing_data(engine, report_date, report_date)
                
                # Create partitions for unique report dates in the DataFrame
                unique_report_dates = df['report_date'].unique()
                create_partitions_for_dates(engine, unique_report_dates)
                
                # Define the order of columns for the DataFrame
                ordered_columns = [
                    'fips', 'admin_two', 'province_state', 'country_region', 'last_update',
                    'latitude', 'longitude', 'confirmed', 'deaths', 'recovered',
                    'active', 'combined_key', 'incident_rate', 'case_fatality_ratio',
                    'report_date', 'etl_insertion_date'
                ]
                
                ordered_columns = [col for col in ordered_columns if col in df.columns]
                df['filename'] = csv_file_to_process.name
                ordered_columns.append('filename')
                df = df[ordered_columns]
                
                target_table = "covid_daily_reports"
            
                # Batch insert data into the target table
                batch_insert(engine, df, target_table)
                
                logging.info(f"Successfully processed file: {csv_file_to_process.name}")
                
            except Exception as e:
                logging.error(f"Error processing file: {csv_file_to_process.name}, Error: {str(e)}")
            
            current_date += timedelta(days=1)
            
    except Exception as e:
        logging.error(f"Error accessing directory or processing dates: {str(e)}")