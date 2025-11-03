SELECT date(last_modified_date) AS date,
    COUNT(*) AS target_total_rowcount_for_date,
    COUNT(DISTINCT id) as target_total_users_for_date
FROM datalake_raw.users
WHERE date(last_modified_date) = CAST(format_datetime (current_timestamp - interval '1' day, 'y-M-d') as date)
GROUP BY date(last_modified_date)
ORDER BY date desc