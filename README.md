# Airflow PostgreSQL Python Project

## Exam Overview

This exam demonstrates the use of Apache Airflow with PostgreSQL and Python for task automation and data processing.

## Technologies Used

- Apache Airflow
- PostgreSQL
- Python

## Prerequisites

- Python 3.12+
- Apache Airflow 2.9+
- PostgreSQL 13+
- Docker
- DBeaver: Optional

## Step Installation in your systen

1. Clone the repository:
   ```bash
   git clone https://github.com/hereithel-180303/airflow_exam.git
   cd your_repository/airflow_exam

   this is the Docker image 
   docker pull hereithel/airflow_exam:latest

2. Go to your terminal:
3. cd your_repository/airflow_exam
4. type in your terminal:
   ```bash
    # Build the Docker image for Airflow
   1. docker build . --tag airflow_exam:latest 
   
   # Initialize Airflow database
   2. docker compose up airflow-init
   
   # Start Docker containers
   3. docker compose up -d
   
   # Check the status if healthy and running
   4. docker ps **Check the status if healthy and running**

5 ## Access Airflow UI
Access Airflow UI at http://localhost:8080
- username: airflow
- password: airflow

**Note:** You can also check the docker-compose.yaml for the Docker configuration

## Connect to the PostgreSQL server
1. Host: localhost
2. Database: postgres
3. Port: 5432
4. Username: airflow
5. Password: airflow

## Database Setup

6. Create a database named 'covid'

7. Create a table
   Run the following script in your SQL editor:
   ```sql
   CREATE TABLE public.covid_daily_reports (
       fips float8 NULL,
       admin_two text NULL,
       province_state text NULL,
       country_region text NULL,
       last_update text NULL,
       latitude float8 NULL,
       longitude float8 NULL,
       confirmed int8 NULL,
       deaths text NULL,
       recovered float8 NULL,
       active text NULL,
       combined_key text NULL,
       incident_rate float8 NULL,
       case_fatality_ratio float8 NULL,
       report_date text NULL,
       filename text NULL,
       etl_insertion_date timestamp NULL
   )
   PARTITION BY RANGE (report_date);

8. Create a Function
   ```sql
   CREATE OR REPLACE FUNCTION public.create_partition_if_not_exists(partition_date date)
    RETURNS void
    LANGUAGE plpgsql
    AS $function$
    DECLARE
        partition_name TEXT;
        start_date DATE;
        end_date DATE;
    BEGIN
        -- Generate partition name based on the date
        partition_name := 'covid_daily_reports_' || TO_CHAR(partition_date, 'YYYY_MM');
        -- Determine the start and end date for the partition (monthly partitions)
        start_date := DATE_TRUNC('month', partition_date);
        end_date := start_date + INTERVAL '1 month';

        -- Check if the partition already exists
        IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = partition_name) THEN
            EXECUTE format(
                'CREATE TABLE %I PARTITION OF covid_daily_reports FOR VALUES FROM (%L) TO (%L)',
                partition_name,
                start_date,
                end_date
            );
        END IF;
    END;
    $function$
    ;

## Note:
> **Important:** This airflow version that I used (2.9.2) has a built-in library. No need to add in the requirements.txt