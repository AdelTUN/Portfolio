WITH UserPaths AS (
  SELECT
    user_pseudo_id,
    STRING_AGG(event_name, ' -> ' ORDER BY event_date) AS user_path,
    COUNTIF(event_name = 'start_game') AS start_game_count
  FROM
    `ga4-analytics.analytics.events_intraday_202405*`
  GROUP BY
    1
)

SELECT
  DISTINCT event,
  user_pseudo_id,
  ROW_NUMBER() OVER (PARTITION BY user_pseudo_id ORDER BY event) AS event_sequence,
  start_game_count
FROM (
  SELECT
    Distinct event AS event,
    user_pseudo_id,
    start_game_count
  FROM (
    SELECT
      user_pseudo_id,
      TRIM(event) AS event,
      start_game_count
    FROM
      UserPaths
    CROSS JOIN
      UNNEST(SPLIT(user_path, ' -> ')) AS event
    WHERE
      TRIM(event) != ''
  )
)
ORDER BY
  user_pseudo_id, event_sequence;
