from google.cloud import bigquery
import psycopg2
from sqlalchemy import create_engine, BigInteger, String, Date, Numeric, Integer
import pandas as pd

# Initialize BigQuery client
client = bigquery.Client()

# Function to fetch data from BigQuery
def fetch_bigquery_data():
    query = """
    WITH main AS (
        SELECT 
            user_id,
            client_id,
            DATE(date) AS date,
            URL,
            session_Id,
            userType,
            sourceMedium,
            campaign,
            country,
            language,
            deviceCategory,
            hostname,
            sessionDurationInSeconds,
            bounces,
            timeOnPage,
            event_name,
            name_of_game,
            CASE 
                WHEN medium = 'organic' THEN 'organic'
                WHEN source IN ('tg', 'telgram') THEN 'Telegram'
                WHEN medium IN ('email') THEN  'Email Marketing'
                WHEN medium IN ('IL') THEN 'Marketing'
                WHEN source IN ('instagram') THEN 'Instagram'
                WHEN medium IN ('(none)') THEN 'Direct'
                WHEN medium = 'referral' OR source = 'referral' THEN 'Referral'
                WHEN medium = 'ab' THEN 'Alanbase'
                ELSE 'unidentified'
            END AS source_group,
            partner_campaign,
            partner_source,
            IFNULL(REGEXP_EXTRACT(REGEXP_REPLACE(absolutePage, r'/[a-z]{2}/', '/'), r'^https?:\/\/[^\/]+(\/[^\/]+\/)'), "/") AS Level_one,
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
                              END) > 0 THEN 'New Visitor' ELSE 'Returning Visitor' END AS userType,
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
                `ga4-analytics.analytics.events*`
            GROUP BY
                1,2,3,4,5,6,7,8,9,10,11,12,13,14,16,15,17,18,19,20,21,22,23,24
            LIMIT 1000
        )
        WHERE date <= "2024-06-20"
    )
    SELECT *
    FROM main
    """
    try:
        query_job = client.query(query)
        results = query_job.result()  # Get query results
        return results
    except Exception as e:
        print(f"Error executing BigQuery query: {str(e)}")
        return None

# Function to write data to analytics database
def write_data_to_analytics_db(df, if_exists='append') -> None:
    try:
        engine = create_engine('postgresql+psycopg2://<username>:<password>@<host>:<port>/<database>')
        
        # Convert specific columns from Row objects to strings
        df['user_id'] = df['user_id'].apply(lambda x: x['field_name'] if 'field_name' in x else None)
        df['client_id'] = df['client_id'].apply(lambda x: x['field_name'] if 'field_name' in x else None)
        df['URL'] = df['URL'].apply(lambda x: x['field_name'] if 'field_name' in x else None)
        df['session_Id'] = df['session_Id'].apply(lambda x: x['field_name'] if 'field_name' in x else None)
        df['userType'] = df['userType'].apply(lambda x: x['field_name'] if 'field_name' in x else None)
        df['sourceMedium'] = df['sourceMedium'].apply(lambda x: x['field_name'] if 'field_name' in x else None)
        df['campaign'] = df['campaign'].apply(lambda x: x['field_name'] if 'field_name' in x else None)
        df['country'] = df['country'].apply(lambda x: x['field_name'] if 'field_name' in x else None)
        df['language'] = df['language'].apply(lambda x: x['field_name'] if 'field_name' in x else None)
        df['deviceCategory'] = df['deviceCategory'].apply(lambda x: x['field_name'] if 'field_name' in x else None)
        df['hostname'] = df['hostname'].apply(lambda x: x['field_name'] if 'field_name' in x else None)
        df['sessionDurationInSeconds'] = df['sessionDurationInSeconds'].apply(lambda x: x['field_name'] if 'field_name' in x else None)
        df['bounces'] = df['bounces'].apply(lambda x: x['field_name'] if 'field_name' in x else None)
        df['timeOnPage'] = df['timeOnPage'].apply(lambda x: x['field_name'] if 'field_name' in x else None)
        df['event_name'] = df['event_name'].apply(lambda x: x['field_name'] if 'field_name' in x else None)
        df['name_of_game'] = df['name_of_game'].apply(lambda x: x['field_name'] if 'field_name' in x else None)
        df['source_group'] = df['source_group'].apply(lambda x: x['field_name'] if 'field_name' in x else None)
        df['partner_campaign'] = df['partner_campaign'].apply(lambda x: x['field_name'] if 'field_name' in x else None)
        df['partner_source'] = df['partner_source'].apply(lambda x: x['field_name'] if 'field_name' in x else None)
        df['Level_one'] = df['Level_one'].apply(lambda x: x['field_name'] if 'field_name' in x else None)
        df['content'] = df['content'].apply(lambda x: x['field_name'] if 'field_name' in x else None)
        df['keyword'] = df['keyword'].apply(lambda x: x['field_name'] if 'field_name' in x else None)
        
        # Define the data types for PostgreSQL table columns
        dtype = {
            'user_id': BigInteger,
            'client_id': String,
            'date': Date,
            'URL': String,
            'session_Id': String,
            'userType': String,
            'sourceMedium': String,
            'campaign': String,
            'country': String,
            'language': String,
            'deviceCategory': String,
            'hostname': String,
            'sessionDurationInSeconds': Integer,
            'bounces': Integer,
            'timeOnPage': Numeric,
            'event_name': String,
            'name_of_game': String,
            'source_group': String,
            'partner_campaign': String,
            'partner_source': String,
            'Level_one': String,
            'content': String,
            'keyword': String
        }
        
        # Write DataFrame to PostgreSQL table
        df.to_sql('data_mart', con=engine, schema='glory_GA4', if_exists='append', index_label='id', chunksize=10000, dtype=dtype)
        
        print('Data has been successfully written to the analytics database.')
    
    except Exception as e:
        print(f'Error writing data to analytics database: {str(e)}')

# Main function to orchestrate the process
def main():
    try:
        # Fetch data from BigQuery
        bigquery_data = fetch_bigquery_data()
        
        if bigquery_data is not None:
            # Convert BigQuery Row objects to dictionary and then to DataFrame
            rows_as_dicts = [row_to_dict(row) for row in bigquery_data]
            df = pd.DataFrame(rows_as_dicts)
            
            # Convert specific columns from Row objects to strings
            df['user_id'] = df['user_id'].apply(lambda x: x['field_name'] if 'field_name' in x else None)
            df['client_id'] = df['client_id'].apply(lambda x: x['field_name'] if 'field_name' in x else None)
            df['URL'] = df['URL'].apply(lambda x: x['field_name'] if 'field_name' in x else None)
            df['session_Id'] = df['session_Id'].apply(lambda x: x['field_name'] if 'field_name' in x else None)
            df['userType'] = df['userType'].apply(lambda x: x['field_name'] if 'field_name' in x else None)
            df['sourceMedium'] = df['sourceMedium'].apply(lambda x: x['field_name'] if 'field_name' in x else None)
            df['campaign'] = df['campaign'].apply(lambda x: x['field_name'] if 'field_name' in x else None)
            df['country'] = df['country'].apply(lambda x: x['field_name'] if 'field_name' in x else None)
            df['language'] = df['language'].apply(lambda x: x['field_name'] if 'field_name' in x else None)
            df['deviceCategory'] = df['deviceCategory'].apply(lambda x: x['field_name'] if 'field_name' in x else None)
            df['hostname'] = df['hostname'].apply(lambda x: x['field_name'] if 'field_name' in x else None)
            df['sessionDurationInSeconds'] = df['sessionDurationInSeconds'].apply(lambda x: x['field_name'] if 'field_name' in x else None)
            df['bounces'] = df['bounces'].apply(lambda x: x['field_name'] if 'field_name' in x else None)
            df['timeOnPage'] = df['timeOnPage'].apply(lambda x: x['field_name'] if 'field_name' in x else None)
            df['event_name'] = df['event_name'].apply(lambda x: x['field_name'] if 'field_name' in x else None)
            df['name_of_game'] = df['name_of_game'].apply(lambda x: x['field_name'] if 'field_name' in x else None)
            df['source_group'] = df['source_group'].apply(lambda x: x['field_name'] if 'field_name' in x else None)
            df['partner_campaign'] = df['partner_campaign'].apply(lambda x: x['field_name'] if 'field_name' in x else None)
            df['partner_source'] = df['partner_source'].apply(lambda x: x['field_name'] if 'field_name' in x else None)
            df['Level_one'] = df['Level_one'].apply(lambda x: x['field_name'] if 'field_name' in x else None)
            df['content'] = df['content'].apply(lambda x: x['field_name'] if 'field_name' in x else None)
            df['keyword'] = df['keyword'].apply(lambda x: x['field_name'] if 'field_name' in x else None)
            
            # Write data to analytics database (append mode)
            write_data_to_analytics_db(df, if_exists='append')
            
        else:
            print("No data fetched from BigQuery.")

    except Exception as e:
        print(f'Error in main process: {str(e)}')

if __name__ == "__main__":
    main()
