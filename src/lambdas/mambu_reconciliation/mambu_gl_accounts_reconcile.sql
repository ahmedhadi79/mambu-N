SELECT  date(timestamp_extracted) as date, COUNT(*)            AS target_total_rowcount_for_date
       ,COUNT(DISTINCT gl_code)  AS target_total_gl_accounts_for_date
FROM datalake_raw.gl_accounts
WHERE  date(timestamp_extracted) = CAST(format_datetime (current_timestamp - interval '1' day, 'y-M-d') as date)
GROUP BY date(timestamp_extracted)
