SELECT 
date(creation_date) AS date,
COUNT(*) AS target_total_rowcount_for_date,
COUNT(DISTINCT entry_id) as target_total_gl_journal_entries_for_date
FROM
(
SELECT 
date_parse(substr(creation_date,1,10),'%Y-%m-%d') AS creation_date,
entry_id
FROM "datalake_raw"."gl_journal_entries"
)
WHERE date(creation_date) = CAST(format_datetime (current_timestamp - interval '1' day, 'y-M-d') as date)
GROUP BY date(creation_date)