import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.types import BigInteger, String, Date
from google.cloud import bigquery
from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
import logging

def get_engine():
    return create_engine('postgresql+psycopg2://<username>:<password>@<host>:<port>/<database>')

# Function to fetch data from BigQuery for a specific date
def fetch_bigquery_data_for_date(target_date):
    key_path = "/opt/airflow/dags/ga4_to_analytics_db/ga4_to_analytics_db_data/ga4-analytics.json"
    client = bigquery.Client.from_service_account_json(key_path)
    query = f"""
    WITH user_events AS (
        SELECT 
            PARSE_DATE('%Y%m%d', event_date) AS date,
            (SELECT value.int_value FROM UNNEST(event_params) AS param WHERE param.key = 'id') AS user_id, 
            COALESCE(
                collected_traffic_source.manual_medium,
                traffic_source.medium,
                (SELECT value.string_value FROM UNNEST(event_params) AS param WHERE param.key = 'medium'),
                REGEXP_EXTRACT((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'), 'utm_medium=([^&]+)')
            ) AS medium,
            COALESCE(
                collected_traffic_source.manual_source,
                traffic_source.name,
                (SELECT value.string_value FROM UNNEST(event_params) AS param WHERE param.key = 'source'),
                REGEXP_EXTRACT((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'), 'utm_source=([^&]+)')
            ) AS source,
            COALESCE(
                collected_traffic_source.manual_campaign_name,
                (SELECT value.string_value FROM UNNEST(event_params) AS param WHERE param.key = 'campaign'),
                REGEXP_EXTRACT((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'), 'utm_campaign=([^&]+)')
            ) AS campaign,
            COALESCE(
                	collected_traffic_source.manual_term,
                (SELECT value.string_value FROM UNNEST(event_params) AS param WHERE param.key = 'term'),
                REGEXP_EXTRACT((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'), 'utm_term=([^&]+)')
            ) AS term,
            (SELECT value.string_value FROM UNNEST(event_params) AS param WHERE param.key = 'page_location') AS page_location
        FROM 
            `banger-casino-ga4-analytics.analytics_402904639.events_intraday_*`
        WHERE 
            COALESCE(
                collected_traffic_source.manual_source,
                traffic_source.name,
                (SELECT value.string_value FROM UNNEST(event_params) AS param WHERE param.key = 'source')
            ) = '7720'
            AND (SELECT value.int_value FROM UNNEST(event_params) AS param WHERE param.key = 'id') IS NOT NULL
            AND PARSE_DATE('%Y%m%d', event_date) = '{target_date}'
    )
    SELECT 
        date,
        user_id,
        medium,
        source,
        campaign,
        email,
        country,
        page_location
    FROM user_events
    GROUP BY 
        date, user_id, medium, source, campaign, page_location,term
    """
    query_job = client.query(query)
    result = query_job.result()
    df = result.to_dataframe()
    return df

# Insert data into PostgreSQL using SQLAlchemy
def insert_into_postgres(data):
    df = data[['user_id', 'date', 'medium', 'source', 'campaign', 'page_location','term']]
    engine = get_engine()
    dtype = {
        'user_id': BigInteger,
        'date': Date,
        'medium': String,
        'source': String,
        'campaign': String,
        'page_location': String
    }
    df.to_sql('ga4_smm_report', con=engine, schema='banger', if_exists='append', index=False, dtype=dtype, chunksize=10000)
    logging.info('Data successfully inserted into PostgreSQL.')

# Delete recent data from PostgreSQL table
def delete_recent_data():
    engine = get_engine()
    with engine.connect() as connection:
        query = text("""DELETE FROM "db".ga4_smm_report WHERE date >= CURRENT_DATE - INTERVAL '1 DAY'""")
        result = connection.execute(query)
        logging.info(f"{result.rowcount} rows deleted from PostgreSQL.")

# Update recent data in PostgreSQL table
def update_recent_data():
    target_date = (datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%d')
    delete_recent_data()
    new_data = fetch_bigquery_data_for_date(target_date)
    insert_into_postgres(new_data)
    logging.info("Recent data updated successfully.")

# Main function to fetch and insert data
def main():
    update_recent_data()

# Callback function for task failure
def on_failure_callback(context):
    logging.error("Task failed.")

# Define the DAG
with DAG(
    dag_id="ga4_smm_report_analytics_db",
    schedule_interval='0 3 * * *',
    default_args={
        "owner": "Adel",
        "start_date": datetime(2024, 5, 14),
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        'on_failure_callback': on_failure_callback,
        'email': ['analytic@glory.bet'],
        'email_on_failure': False,
        'email_on_retry': False
    },
    catchup=False
) as dag:

    first_function = PythonOperator(
        task_id="main",
        python_callable=main,
        dag=dag
    )

first_function
