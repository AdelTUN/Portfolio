import pandas as pd
from google.cloud import bigquery
from sqlalchemy import create_engine
from datetime import datetime, timedelta

def get_postgres_data_for_date(target_date):
    engine = create_engine('oracle+cx_oracle://oraclecopy:A#defd23fwfesadf@192.168.179.202:1521/GLORY202')
    query = f"""
    SELECT DISTINCT 
        ID_CLIENT::BIGINT,DATE_REGISTR
    FROM 
        FROM GLORY_PROD.CLIENTS c 
    WHERE 
        (DATE_REGISTR + INTERVAL '3 hours') BETWEEN '{target_date} 00:00:00' AND '{target_date} 23:59:59'
    """
    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn)
    return df

def get_bigquery_data_for_date(target_date):
    if isinstance(target_date, str):
        target_date = datetime.strptime(target_date, '%Y-%m-%d').date()
    client = bigquery.Client()
    query = f"""
    SELECT 
      DISTINCT CAST((SELECT value.int_value 
         FROM UNNEST(event_params) AS param 
         WHERE param.key = 'id') AS int) AS user_id,
      PARSE_DATE("%Y%m%d", event_date) AS date  
    FROM `glory-casino-ga4-analytics.analytics_294913339.events_intraday_*`
    WHERE  event_name = 'registration'
    AND (SELECT value.int_value 
         FROM UNNEST(event_params) AS param 
         WHERE param.key = 'id') IS NOT NULL
    AND event_date = '{target_date.strftime("%Y%m%d")}'
    """
    df = client.query(query).to_dataframe()
    df['date'] = pd.to_datetime(df['date'], format="%Y%m%d")
    return df

# Initialize a list to store daily comparisons
daily_comparisons = []

# Get the unique dates for the last 30 days
unique_dates = pd.date_range(end=pd.Timestamp.now(), periods=10).date

for date in unique_dates:
    # Convert date to string format for queries
    date_str = date.strftime('%Y-%m-%d')
    
    # Extract data for the current date
    postgres_data = get_postgres_data_for_date(date.strftime('%Y-%m-%d'))
    bigquery_data = get_bigquery_data_for_date(date.strftime('%Y-%m-%d'))
    
    # Convert createdAt to date format
    postgres_data['date'] = pd.to_datetime(postgres_data['createdAt']).dt.date
    bigquery_data['date'] = pd.to_datetime(bigquery_data['date'])
    
    # Get users for the current date
    pg_users = set(postgres_data[(postgres_data['date'] == date) & (postgres_data['id'].notnull())]['id'])
    ga_users = set(bigquery_data[(bigquery_data['date'] == date) & (bigquery_data['user_id'].notnull())]['user_id'])
    
    common_users = pg_users.intersection(ga_users)
    only_pg_users = pg_users - ga_users
    only_ga_users = ga_users - pg_users
    
    # Print counts for debugging
    print(f"Date: {date}")
    print(f"PG Users: {len(pg_users)}, GA Users: {len(ga_users)}")
    print(f"Common Users: {len(common_users)}")
    print(f"Only DB Users: {len(only_pg_users)}")
    print(f"Only GA Users: {len(only_ga_users)}")
    
    daily_comparisons.append({
        'date': date,
        'both': len(common_users),
        'only_db': len(only_pg_users),
        'only_ga': len(only_ga_users),
        'only_db_details': list(only_pg_users),
        'only_ga_details': list(only_ga_users)
    })

# Convert daily_comparisons to a DataFrame for easy manipulation
report_df = pd.DataFrame(daily_comparisons)

# The output DataFrame to be loaded into Power BI
output_df = report_df[['date', 'both', 'only_db', 'only_ga']]

print("Final Report DataFrame:")
print(output_df)
