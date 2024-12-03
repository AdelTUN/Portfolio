

def get_mismatches(client, postgres_connection_string):
    # Define the BigQuery query
    bigquery_query = """
    WITH user_events AS (
      SELECT
        (SELECT value.int_value FROM UNNEST(event_params) AS param WHERE param.key = 'id' AND value.int_value IS NOT NULL) AS id,
        COALESCE(
          e.collected_traffic_source.manual_source,
          e.traffic_source.name,
          (SELECT value.string_value FROM UNNEST(e.event_params) AS param WHERE param.key = 'source')
        ) AS source,
        e.collected_traffic_source.manual_campaign_name AS campaign_name
      FROM
        `glory-casino-ga4-analytics.analytics_294913339.events_*` e
      WHERE
        (SELECT value.int_value FROM UNNEST(event_params) AS param WHERE param.key = 'id' AND value.int_value IS NOT NULL) IS NOT NULL
    ),
    postgress_data AS (
      SELECT
        ID_User,
        utm_source AS source,
        utm_campaign AS campaign_name
      FROM
        your_postgres_table
    )
    SELECT
      ue.id,
      ue.source AS bq_source,
      ue.campaign_name AS bq_campaign,
      pd.source AS postgres_source,
      pd.campaign_name AS postgres_campaign
    FROM
      user_events ue
    LEFT JOIN
      postgress_data pd
    ON
      ue.id = pd.ID_User
    """

    # Execute the BigQuery query
    bigquery_client = bigquery.Client()
    bigquery_results = bigquery_client.query(bigquery_query).to_dataframe()

    # Connect to PostgreSQL and execute query
    engine = create_engine(postgres_connection_string)
    postgres_query = """
    SELECT ID_User, utm_source AS source, utm_campaign AS campaign_name
    FROM your_postgres_table
    """
    postgres_results = pd.read_sql(postgres_query, engine)

    # Merge DataFrames on user ID
    merged_df = pd.merge(bigquery_results, postgres_results, left_on='id', right_on='ID_User', how='inner')

    # Check for mismatches
    mismatch_df = merged_df[(merged_df['bq_source'] != merged_df['postgres_source']) | (merged_df['bq_campaign'] != merged_df['postgres_campaign'])]

    # Get user IDs with mismatches
    mismatch_user_ids = mismatch_df['id'].tolist()

    return mismatch_user_ids
