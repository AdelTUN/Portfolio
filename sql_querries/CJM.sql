WITH UserPaths AS (
  SELECT
    user_pseudo_id,
    event_date,
    STRING_AGG(event_name, ' -> ' ORDER BY event_date) AS user_path,
    COUNTIF(event_name = 'start_game') AS start_game_count
  FROM
    `ga4-analytics.analytics.events_intraday_202404*`
  GROUP BY
    user_pseudo_id,event_date
),

UsersWithFirstVisit AS (
  SELECT
    user_pseudo_id
  FROM
    UserPaths
  WHERE
    user_path LIKE 'first_visit%'
),

ExplodedEvents AS (
  SELECT
    UserPaths.user_pseudo_id,
    'first_visit' AS event,
    1 AS start_game_count,
    1 AS event_count,
    1 AS event_sequence
  FROM
    UserPaths
  JOIN
    UsersWithFirstVisit
  ON
    UserPaths.user_pseudo_id = UsersWithFirstVisit.user_pseudo_id

  UNION ALL

  SELECT
    UserPaths.user_pseudo_id,
    TRIM(event) AS event,
    start_game_count,
    COUNT(*) AS event_count,
    ROW_NUMBER() OVER (PARTITION BY UserPaths.user_pseudo_id ORDER BY event_date) + 1 AS event_sequence
  FROM
    UserPaths
  JOIN
    UsersWithFirstVisit
  ON
    UserPaths.user_pseudo_id = UsersWithFirstVisit.user_pseudo_id
  CROSS JOIN
    UNNEST(SPLIT(user_path, ' -> ')) AS event
  WHERE
    TRIM(event) != '' AND TRIM(event) != 'first_visit'
  GROUP BY
    UserPaths.user_pseudo_id, event, start_game_count,event_date
)

SELECT
  user_pseudo_id,
  event,
  event_sequence,
  start_game_count,
  event_count
FROM
  ExplodedEvents
ORDER BY
  user_pseudo_id, event_sequence
  LIMIT 5000
