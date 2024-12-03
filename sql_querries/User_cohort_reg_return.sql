WITH prep AS (
  SELECT
    user_pseudo_id,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_id,
    MAX((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number')) AS session_number,
    MIN(PARSE_DATE('%Y%m%d', event_date)) AS session_date,
    SUM((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'engagement_time_msec')) AS engagement_time_msec,
    FIRST_VALUE(MIN(PARSE_DATE('%Y%m%d', event_date))) OVER (PARTITION BY user_pseudo_id ORDER BY MIN(event_date) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_session_date
  FROM
    `glory-casino-ga4-analytics.analytics_294913339.events_intraday_*`
  WHERE
    _table_suffix BETWEEN FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 4 WEEK)) AND FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 0 WEEK))
    AND event_name NOT IN ('first_visit', 'first_open') 
  GROUP BY
    user_pseudo_id,
    session_id
),

registration_events AS (
  SELECT
    user_pseudo_id,
    MIN(PARSE_DATE('%Y%m%d', event_date)) AS registration_date
  FROM
    `glory-casino-ga4-analytics.analytics_294913339.events_intraday_*`
  WHERE
    _table_suffix BETWEEN FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 4 WEEK)) AND FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 0 WEEK))
    AND event_name = 'registration' 
  GROUP BY
    user_pseudo_id
)

SELECT
  CONCAT(EXTRACT(ISOYEAR FROM p.first_session_date), '-', FORMAT('%02d', EXTRACT(ISOWEEK FROM p.first_session_date))) AS year_week,
  COUNT(DISTINCT CASE WHEN DATE_DIFF(p.session_date, p.first_session_date, ISOWEEK) = 0 AND p.session_number >= 1 AND p.engagement_time_msec > 0 THEN p.user_pseudo_id END) AS week_0,
  COUNT(DISTINCT CASE WHEN DATE_DIFF(p.session_date, p.first_session_date, ISOWEEK) = 1 AND p.session_number > 1 AND p.engagement_time_msec > 0 THEN p.user_pseudo_id END) AS week_1,
  COUNT(DISTINCT CASE WHEN DATE_DIFF(p.session_date, p.first_session_date, ISOWEEK) = 2 AND p.session_number > 1 AND p.engagement_time_msec > 0 THEN p.user_pseudo_id END) AS week_2,
  COUNT(DISTINCT CASE WHEN DATE_DIFF(p.session_date, p.first_session_date, ISOWEEK) = 3 AND p.session_number > 1 AND p.engagement_time_msec > 0 THEN p.user_pseudo_id END) AS week_3,
  COUNT(DISTINCT CASE WHEN DATE_DIFF(p.session_date, p.first_session_date, ISOWEEK) = 4 AND p.session_number > 1 AND p.engagement_time_msec > 0 THEN p.user_pseudo_id END) AS week_4
FROM
  prep p
  JOIN registration_events r ON p.user_pseudo_id = r.user_pseudo_id
GROUP BY
  year_week
ORDER BY
  year_week;
