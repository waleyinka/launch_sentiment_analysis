-- Top company for a specific hour.
SELECT
  company_name,
  pageviews,
  hour_timestamp
FROM pageviews_hourly
WHERE hour_timestamp = '2025-12-10 16:00:00+00'
ORDER BY pageviews DESC
LIMIT 1;


-- Top company for a specific day.
SELECT
  date_trunc('day', hour_timestamp) AS day,
  company_name,
  SUM(pageviews) AS daily_pageviews
FROM pageviews_hourly
WHERE hour_timestamp >= '2025-12-10 00:00:00+00'
  AND hour_timestamp <  '2025-12-11 00:00:00+00'
GROUP BY 1, 2
ORDER BY daily_pageviews DESC
LIMIT 1;
