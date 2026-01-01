```sql
SELECT
    company_name,
    pageviews
FROM
    "pageviews_hourly"
ORDER BY pageviews DESC
LIMIT 1;
```

```sql
SELECT
    company_name,
    pageviews
FROM
    pageviews_hourly
WHERE
    hour_timestamp = '<hour>'
ORDER BY
    pageviews DESC
LIMIT 1;
```