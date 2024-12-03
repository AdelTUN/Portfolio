from google.cloud import bigquery
import pandas as pd
from sqlalchemy import create_engine, BigInteger, String, Date, Integer, text
from concurrent.futures import ThreadPoolExecutor
from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python_operator import PythonOperator

# Define the engine creation function
def get_engine():
    # Replace with your connection string
    return create_engine('postgresql+psycopg2://<username>:<password>@<host>:<port>/<database>')

# Initialize BigQuery client
key_path = "/path/to/your-service-account.json"
client = bigquery.Client.from_service_account_json(key_path)

# Define the BigQuery query function
def fetch_bigquery_data():
    query = """
    WITH main AS (
        SELECT
            user_id,
            client_id,
            date,
            url,
            session_id,
            usertype,
            sourcemedium,
            campaign,
            country,
            language,
            devicecategory,
            hostname,
            sessiondurationinseconds,
            bounces,
            timeonpage,
            event_name,
            name_of_game,
            CASE 
                WHEN medium = 'organic' THEN 'organic'
                WHEN source IN ('tg', 'telegram') THEN 'Telegram'
                WHEN medium IN ('email') THEN 'Email Marketing'
                WHEN medium IN ('IL') THEN 'Marketing'
                WHEN source IN ('instagram') THEN 'Instagram'
                WHEN medium IN ('(none)') THEN 'Direct'
                WHEN medium = 'referral' OR source = 'referral' THEN 'Referral'
                WHEN medium = 'ab' THEN 'Alanbase'
                ELSE 'unidentified'
            END AS source_group,
            partner_campaign,
            partner_source,
            IFNULL(REGEXP_EXTRACT(REGEXP_REPLACE(absolutePage, r'/[a-z]{2}/', '/'), r'^https?:\/\/[^\/]+(\/[^\/]+\/)'), "/") AS level_one,
            content,
            keyword
        FROM (
            SELECT 
                (SELECT value.int_value FROM UNNEST(event_params) AS param WHERE param.key = 'id' AND value.int_value IS NOT NULL) AS user_id,
                user_pseudo_id AS client_id,
                event_name,
                (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'name_of_game') AS name_of_game,
                (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'name_of_provider') AS name_of_provider,
                DATE(CONCAT(SUBSTR(event_date, 1, 4), '-', SUBSTR(event_date, 5, 2), '-', SUBSTR(event_date, 7, 2))) AS date,
                REGEXP_EXTRACT((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'), r'([^?]+)') AS URL,
                IFNULL(collected_traffic_source.manual_content, TRIM(REGEXP_EXTRACT((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'), r'utm_content=[^&]+'), "utm_content=")) AS content,
                IFNULL(collected_traffic_source.manual_term, TRIM(REGEXP_EXTRACT((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'), r'utm_term=[^&]+'), "utm_term=")) AS keyword,
                REGEXP_EXTRACT((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'), r'([^?]+)') AS absolutePage,
                CONCAT(user_pseudo_id, '.', (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id'), '.', (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number')) AS session_id,
                CONCAT(IFNULL(IFNULL(collected_traffic_source.manual_source, traffic_source.source), REGEXP_EXTRACT((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'), 'utm_source=([^&]+)')), ' / ', IFNULL(IFNULL(collected_traffic_source.manual_medium, traffic_source.medium), REGEXP_EXTRACT((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'), 'utm_medium=([^&]+)'))) AS sourceMedium,
                REPLACE(REPLACE(REPLACE(REPLACE(IFNULL(IFNULL(collected_traffic_source.manual_campaign_id, traffic_source.name), REGEXP_EXTRACT((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'), 'utm_campaign=([^&]+)')), '%7C', '|'), '%5F', '_'), '%7C', '|'), '%20', ' ') AS campaign,
                geo.country AS country,
                device.language AS language,
                device.category AS deviceCategory,
                device.web_info.hostname AS hostname,
                device.web_info.hostname AS domain,
                collected_traffic_source.manual_campaign_name AS partner_campaign,
                collected_traffic_source.manual_source AS partner_source,
                IFNULL(IFNULL(collected_traffic_source.manual_source, traffic_source.source), REGEXP_EXTRACT((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'), 'utm_source=([^&]+)')) AS source,
                IFNULL(IFNULL(collected_traffic_source.manual_medium, traffic_source.medium), REGEXP_EXTRACT((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'), 'utm_medium=([^&]+)')) AS medium,
                (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number') AS session_number,
                (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_title') AS page_title,
                CASE WHEN SUM(CASE
                    WHEN event_name IN ('first_visit', 'first_open') THEN 1
                    ELSE 0
                END) > 0 THEN "New Visitor" ELSE "Returning Visitor" END AS userType,
                CASE WHEN SUM(CAST((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') AS INT)) < 1 THEN  1 ELSE 0 END AS bounces,
                SUM(CASE
                    WHEN event_name = 'user_engagement' AND (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'engagement_time_msec') IS NOT NULL THEN (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'engagement_time_msec') / 1000
                    ELSE 0
                END) AS timeOnPage,
                SUM(CASE
                    WHEN event_name = 'user_engagement' AND (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'engagement_time_msec') IS NOT NULL THEN (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'engagement_time_msec') / 1000
                    ELSE 0
                END) AS sessionDurationInSeconds
            FROM
                ga4-analytics.analytics.events_*
            GROUP BY
                1,2,3,4,5,6,7,8,9,10,11,12,13,14,16,15,17,18,19,20,21,22,23,24
        )
        WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    )
    SELECT *
    FROM main
    """
    
    query_job = client.query(query)
    result = query_job.result()
    df = result.to_dataframe()
    print(df.head())
    print("Data successfully fetched from BigQuery.")
    return df

# Insert data into PostgreSQL using SQLAlchemy
def insert_into_postgres(data):
    df = data[['user_id', 'client_id', 'date', 'url', 'session_id', 'usertype', 'sourcemedium', 'campaign', 'country', 'language', 'devicecategory', 'hostname', 'sessiondurationinseconds', 'bounces', 'timeonpage', 'event_name', 'name_of_game', 'source_group', 'partner_campaign', 'partner_source', 'level_one', 'content', 'keyword']]

    engine = get_engine()
    dtype = {
        'user_id': BigInteger,
        'client_id': String,
        'date': Date,
        'url': String,
        'session_id': String,
        'usertype': String,
        'sourcemedium': String,
        'campaign': String,
        'country': String,
        'language': String,
        'devicecategory': String,
        'hostname': String,
        'sessiondurationinseconds': Integer,
        'bounces': Integer,
        'timeonpage': Integer,
        'event_name': String,
        'name_of_game': String,
        'source_group': String,
        'partner_campaign': String,
        'partner_source': String,
        'level_one': String,
        'content': String,
        'keyword': String
    }

    df.to_sql('data_mart', con=engine, schema='your_schema', if_exists='append', index=False, dtype=dtype, chunksize=10000)
    print('Data successfully written to PostgreSQL.')

# Delete recent data from PostgreSQL table
def delete_recent_data():
    engine = get_engine()
    with engine.connect() as connection:
        query = text("""DELETE FROM your_schema.data_mart WHERE date >= CURRENT_DATE - INTERVAL '1 DAY'""")
        result = connection.execute(query)
        print(f"{result.rowcount} rows deleted from PostgreSQL.")

# Update recent data in PostgreSQL table
def update_recent_data():
    new_data = fetch_bigquery_data()
    insert_into_postgres(new_data)
    print("Recent data updated successfully.")

# Main function to run the update
def main():
    update_recent_data()

def on_failure_callback(context):
    print("Fail works!")

with DAG(dag_id="ga4_to_analytics_db_data",
         schedule_interval='0 3 * * *',
         default_args={
             "owner": "Adel",
             "start_date": datetime(2024, 5, 14),
             "retries": 2,
             "retry_delay": timedelta(minutes=5),
             'on_failure_callback': on_failure_callback,
             'email': ['youremail@example.com'],
             'email_on_failure': False,
             'email_on_retry': False},
         catchup=False) as dag:

    first_function = PythonOperator(
        task_id="main",
        python_callable=main,
        dag=dag
    )
first_function
