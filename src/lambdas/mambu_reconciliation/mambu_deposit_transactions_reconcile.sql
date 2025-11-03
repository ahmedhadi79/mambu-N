WITH ids AS
(
	SELECT  date_parse(substr(creation_date,1,10),'%Y-%m-%d') AS date
	       ,MIN(CAST(id AS int))                              AS min_id_for_date
	       ,MAX(CAST(id AS int))                              AS max_id_for_date
	       ,COUNT(*)                                          AS total_rowcount_for_date
	FROM datalake_raw.deposit_transactions
	GROUP BY  date_parse(substr(creation_date,1,10),'%Y-%m-%d')
), amounts AS
(
	SELECT  date_parse(substr(creation_date,1,10),'%Y-%m-%d') AS date
	       ,currency_code
	       ,type
	       ,SUM(CAST(amount AS decimal(20,2)))                AS sum_amount_for_currency_and_date
	       ,COUNT(*)                                          AS currency_type_transactions_for_date
	FROM datalake_raw.deposit_transactions
	GROUP BY  date_parse(substr(creation_date,1,10),'%Y-%m-%d')
	         ,currency_code
	         ,type
)
SELECT  ids.date
       ,ids.min_id_for_date AS target_min_id_for_date
       ,ids.max_id_for_date AS target_max_id_for_date
       ,ids.total_rowcount_for_date AS target_total_rowcount_for_date
       ,amounts.currency_code
       ,amounts.type
       ,amounts.sum_amount_for_currency_and_date AS target_sum_amount_for_currency_and_date
       ,amounts.currency_type_transactions_for_date AS target_currency_type_transactions_total_for_date
FROM ids
INNER JOIN amounts
ON ids.date = amounts.date
WHERE ids.date = CAST(format_datetime (current_timestamp - interval '1' day, 'y-M-d') as date)
ORDER BY ids.date desc, currency_code desc