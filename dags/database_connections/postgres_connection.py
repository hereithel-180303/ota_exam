import os
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

# Default connection parameters
DEFAULT_PG_USERNAME = 'airflow'
DEFAULT_PG_PASSWORD = 'airflow'
DEFAULT_PG_HOST = 'postgres'
DEFAULT_PG_PORT = '5432'
DEFAULT_PG_DATABASE = 'covid'

def get_postgres_engine():
    """Create and return a SQLAlchemy engine for PostgreSQL using environment variables."""
    username = os.getenv('PG_USERNAME', DEFAULT_PG_USERNAME)
    password = os.getenv('PG_PASSWORD', DEFAULT_PG_PASSWORD)
    host = os.getenv('PG_HOST', DEFAULT_PG_HOST)
    port = os.getenv('PG_PORT', DEFAULT_PG_PORT)
    database = os.getenv('PG_DATABASE', DEFAULT_PG_DATABASE)

    connection_str = f"postgresql://{username}:{password}@{host}:{port}/{database}"
    engine = create_engine(connection_str)
    return engine

def execute_query(query):
    """Execute a SQL query on the connected PostgreSQL database."""
    try:
        engine = get_postgres_engine()
        with engine.connect() as connection:
            result = connection.execute(query)
            return result
    except SQLAlchemyError:
        raise

def insert_data(df, table_name, action="append"):
    """Insert data from a pandas DataFrame into a PostgreSQL table."""
    engine = None
    try:
        engine = get_postgres_engine()
        with engine.connect() as connection:
            transaction = connection.begin()
            try:
                df.to_sql(table_name, engine, if_exists=action, index=False, method="multi")
                transaction.commit()
            except SQLAlchemyError:
                transaction.rollback()
                raise
    except SQLAlchemyError:
        raise
    finally:
        if engine:
            engine.dispose()