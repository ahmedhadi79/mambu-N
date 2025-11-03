SELECT  creation_date AS date
       ,COUNT(*)            AS target_total_rowcount_for_date
       ,COUNT(DISTINCT id)  AS target_total_deposit_accounts_for_date
FROM
(
	SELECT  date(date_parse(substr(creation_date,1,10),'%Y-%m-%d')) AS creation_date
	       ,id
	FROM datalake_raw.deposit_accounts
	
)
WHERE creation_date = CAST(format_datetime (current_timestamp - interval '1' day, 'y-M-d') as date)
GROUP BY creation_date
ORDER BY date desc