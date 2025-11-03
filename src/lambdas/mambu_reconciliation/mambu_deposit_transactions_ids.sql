SELECT
creation_date,
id
FROM
(
SELECT 
date_parse(substr(creation_date,1,10),'%Y-%m-%d') AS creation_date,
id
FROM "datalake_raw"."deposit_transactions"
)
WHERE creation_date = CAST(format_datetime (current_timestamp - interval '1' day, 'y-M-d') as date)
GROUP BY  creation_date,id